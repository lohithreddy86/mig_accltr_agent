"""
helpers.py
==========
Shared utilities for all NiceGUI pages.
Pure Python — no Streamlit, no NiceGUI imports.
Callers pass storage dicts where needed.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

# ── Make src importable ───────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

from sql_migration.core.config_loader import get_config
from sql_migration.core.artifact_store import ArtifactStore


# ══════════════════════════════════════════════════════════════════════════════
# Colour / icon constants
# ══════════════════════════════════════════════════════════════════════════════

STATUS_COLOURS = {
    "PENDING":          "#484F58",
    "DISPATCHED":       "#6C5CE7",
    "CHUNK_CONVERTING": "#7C6CF0",
    "CHUNK_DONE":       "#B8A5FF",
    "ALL_CHUNKS_DONE":  "#B8A5FF",
    "VALIDATING":       "#6C5CE7",
    "VALIDATED":        "#4ECDC4",
    "PARTIAL":          "#7ECFC7",
    "FAILED":           "#9B6B6B",
    "FROZEN":           "#9B6B6B",
    "SKIPPED":          "#7D8590",
}

STATUS_ICONS = {
    "PENDING":          "⏳",
    "DISPATCHED":       "🔄",
    "CHUNK_CONVERTING": "⚙️",
    "CHUNK_DONE":       "✓",
    "ALL_CHUNKS_DONE":  "✓✓",
    "VALIDATING":       "🔍",
    "VALIDATED":        "✅",
    "PARTIAL":          "⚠️",
    "FAILED":           "❌",
    "FROZEN":           "🔴",
    "SKIPPED":          "⏭️",
}

AGENT_STEPS_QUERY_MODE = {
    "analysis": [
        ("A0", "LLM+SANDBOX", "Parse README and select source dialect adapter via LLM tool call"),
        ("A1", "SANDBOX",     "Split SQL files on semicolons into individual queries"),
        ("A2", "LLM",         "Per-query LLM analysis: classify tables, extract column functions with Trino equivalents, score complexity, write detailed business purpose"),
        ("A3", "TRINO MCP",   "Resolve real tables against live Trino via DESCRIBE — skips CTEs, views, system dummy tables"),
        ("A4", "SANDBOX",     "Synthesis: manifest, construct_map (function→Trino mappings), semantic_map, column_analysis"),
    ],
    "planning": [
        ("P1", "AUTO",       "Force TRINO_SQL strategy for all queries — skip LLM strategy selection"),
        ("P3", "TRINO MCP",  "Live schema fetch per query (column names + types from Trino)"),
        ("P4", "SANDBOX",    "Assemble plan: inject construct_hints, schema_context, semantic_context per query"),
    ],
    "orchestrator": [
        ("O1", "STATE",     "Init checkpoint — one entry per query"),
        ("O2", "STATE",     "Dispatch queries for conversion in order"),
        ("O3", "STATE",     "On failure: mark query as CONVERSION_FAILED, continue to next query"),
    ],
    "conversion": [
        ("C1",     "SANDBOX",   "Extract source query by line boundaries"),
        ("CONVERT","LLM+MCP",   "LLM converts source to Trino SQL, tests via query_trino on live Trino, self-corrects on errors"),
        ("REVIEW", "LLM",       "Separate LLM semantic review: compares source vs converted side-by-side, catches silent hallucinations"),
        ("ACCEPT", "CODE",      "Write final .sql file — or CONVERSION_FAILED if review cycles exhausted"),
    ],
    "validation": [
        ("V3", "TRINO MCP",  "Execute converted SQL against live Trino — validates column names, types, and syntax against real lakehouse"),
        ("V5", "SCORING",    "Score result: PARTIAL (Trino-validated, source-side equivalence pending)"),
    ],
}

AGENT_STEPS_PROC_MODE = {
    "analysis": [
        ("A0", "LLM+SANDBOX", "Parse README and select source dialect adapter via LLM tool call"),
        ("A1", "SANDBOX",     "Boundary scan — extract stored procedure boundaries using adapter patterns"),
        ("A2", "LLM",         "Per-proc LLM analysis: classify tables, extract column functions, score complexity, write business purpose and key business rules"),
        ("A3", "TRINO MCP",   "Resolve real tables against live Trino via DESCRIBE — skips CTEs, cursors, variables, system dummy tables"),
        ("A4", "SANDBOX",     "Synthesis: manifest, dependency graph, conversion order, complexity report, semantic_map, column_analysis"),
    ],
    "planning": [
        ("P1",   "LLM",       "LLM strategy selection per proc: TRINO_SQL, PYSPARK_DF, or PYSPARK_PIPELINE"),
        ("P1.5", "SANDBOX",   "Module grouping — group related procs for joint conversion"),
        ("P2",   "SANDBOX",   "Chunk boundary computation using adapter transaction patterns"),
        ("P3",   "TRINO MCP", "Live schema fetch per chunk (column names + types from Trino)"),
        ("P4",   "SANDBOX",   "Assemble plan: inject construct_hints, schema_context, semantic_context, loop guards per chunk"),
    ],
    "orchestrator": [
        ("O1", "STATE",     "Init or resume from checkpoint — supports crash recovery"),
        ("O2", "STATE",     "Dependency-gated dispatch: convert callees before callers"),
        ("O3", "STATE",     "Failure routing: retry → replan → freeze. Error classification + circuit breaker"),
    ],
    "conversion": [
        ("C1",     "SANDBOX",   "Extract chunk by line boundaries"),
        ("C2-C4", "LLM+TOOLS", "Agentic LLM loop with tools: run_pyspark (sandbox), query_trino (MCP), validate_sql. Self-corrects up to budget limit"),
        ("REVIEW", "LLM",       "Separate LLM semantic review: compares source vs converted, catches business logic drift"),
        ("C5",     "SANDBOX",   "Assemble all chunks into final output file (multi-chunk procs only)"),
    ],
    "validation": [
        ("V1", "TRINO MCP", "Data availability: COUNT(*) per table to determine validation level"),
        ("V2", "MCP+SBX",   "Sample data extraction: pull rows from Trino, write to parquet for sandbox"),
        ("V3", "SANDBOX",   "Dry-run PySpark execution in sandbox against local Spark with sample data"),
        ("V4", "LLM+MCP",   "Semantic diff (full reconciliation only): compare output schema against Trino source"),
        ("V5", "SCORING",   "Reconciliation report: PASS / PARTIAL / FAIL"),
    ],
}

AGENT_STEPS = AGENT_STEPS_PROC_MODE

TOOL_COLOURS = {
    "SANDBOX":   "#B8A5FF",
    "LLM":       "#6C5CE7",
    "TRINO MCP": "#7C6CF0",
    "MCP+SBX":   "#B8A5FF",
    "LLM+MCP":   "#6C5CE7",
    "LLM+TOOLS": "#6C5CE7",
    "STATE":     "#4ECDC4",
    "AUTO":      "#7D8590",
    "CODE":      "#7ECFC7",
    "SCORING":   "#B8A5FF",
    "LLM+SANDBOX": "#6C5CE7",
}

STAGE_META = {
    "analysis":        {"icon": "🔬", "label": "Analysis (Setup + Extraction + LLM)", "color": "#6C5CE7"},
    "dependency_gate": {"icon": "🔗", "label": "Dependency Gate",      "color": "#7D8590"},
    "planning":        {"icon": "📐", "label": "Planning",             "color": "#B8A5FF"},
    "preflight":       {"icon": "🛡️",  "label": "Pre-flight Assessment", "color": "#7C6CF0"},
    "orchestration":   {"icon": "🎯", "label": "Conversion + Validation", "color": "#4ECDC4"},
}

STEP_LABELS = {
    "C1": "Extract chunk",       "C2": "Converting + testing",
    "C3": "Static validation",   "C3.5": "Test execution",
    "C4": "Self-correction",     "C5": "Assembling",
    "REVIEW": "Semantic review", "ACCEPT": "Accepted",
    "V1": "Data availability",   "V2": "Sample extraction",
    "V3": "Execute on Trino",    "V4": "Semantic diff",
    "V5": "Scoring result",
}


# ══════════════════════════════════════════════════════════════════════════════
# Store access (pure Python — no Streamlit cache)
# ══════════════════════════════════════════════════════════════════════════════

_store_cache: dict[str, ArtifactStore] = {}

def get_store(run_id: str) -> ArtifactStore:
    """Get or create ArtifactStore for a run. Cached in-memory."""
    if run_id not in _store_cache:
        _store_cache[run_id] = ArtifactStore(run_id=run_id)
    return _store_cache[run_id]


def get_checkpoint(store: ArtifactStore) -> dict | None:
    try:
        return store.read("orchestrator", "checkpoint.json")
    except Exception:
        return None

def get_pipeline_status(store: ArtifactStore) -> dict | None:
    """Read pipeline_status.json — written by Orchestrator at end of run."""
    try:
        return store.read("orchestrator", "pipeline_status.json")
    except Exception:
        return None

def get_plan(store: ArtifactStore) -> dict | None:
    try:
        return store.read("planning", "plan.json")
    except Exception:
        return None

def get_manifest(store: ArtifactStore) -> dict | None:
    try:
        return store.read("analysis", "manifest.json")
    except Exception:
        return None

def get_feedback_bundle(store: ArtifactStore, proc_name: str) -> dict | None:
    try:
        return store.read("feedback", f"developer_feedback_{proc_name}.json")
    except Exception:
        return None

def get_validation_result(store: ArtifactStore, proc_name: str) -> dict | None:
    try:
        return store.read("validation", f"validation_{proc_name}.json")
    except Exception:
        return None

def get_converted_code(store: ArtifactStore, proc_name: str) -> tuple[str, str]:
    for ext in (".py", ".sql"):
        path = store.path("conversion", f"proc_{proc_name}{ext}")
        if path.exists():
            return path.read_text(errors="replace"), ext
    return "", ""

def get_conversion_log(store: ArtifactStore, proc_name: str) -> dict | None:
    try:
        return store.read("conversion", f"conversion_log_{proc_name}.json")
    except Exception:
        return None

def get_stage_progress(store: ArtifactStore) -> dict | None:
    """Read stage_progress.json for the pipeline stepper UI."""
    try:
        return store.read("orchestrator", "stage_progress.json")
    except Exception:
        return None

def get_conversion_step(store: ArtifactStore, proc_name: str,
                         chunk_id: str = "") -> dict | None:
    try:
        return store.read("conversion",
                           f"step_progress_{proc_name}{'_' + chunk_id if chunk_id else ''}.json")
    except Exception:
        return None

def get_conversion_steps(store: ArtifactStore, proc_name: str,
                          chunk_ids: list[str] | None = None) -> list[dict]:
    try:
        data = store.read("conversion",
                           f"step_progress_{proc_name}.json")
        return data if isinstance(data, list) else [data]
    except Exception:
        return []

def get_validation_step(store: ArtifactStore, proc_name: str) -> dict | None:
    try:
        return store.read("validation",
                           f"step_progress_{proc_name}.json")
    except Exception:
        return None

def get_validation_steps(store: ArtifactStore, proc_name: str) -> list[dict]:
    try:
        data = store.read("validation",
                           f"step_progress_{proc_name}.json")
        return data if isinstance(data, list) else [data]
    except Exception:
        return []


# ══════════════════════════════════════════════════════════════════════════════
# HTML formatting helpers (return strings — no UI calls)
# ══════════════════════════════════════════════════════════════════════════════

def status_badge(status: str) -> str:
    color = STATUS_COLOURS.get(status, "#7D8590")
    icon = STATUS_ICONS.get(status, "•")
    return (f'<span style="background:{color}20;color:{color};padding:2px 8px;'
            f'border-radius:4px;font-size:11px;font-weight:600;border:1px solid {color}40">'
            f'{icon} {status}</span>')

def tool_badge(tool: str) -> str:
    color = TOOL_COLOURS.get(tool, "#7D8590")
    return (f'<span style="background:{color}15;color:{color};padding:1px 6px;'
            f'border-radius:3px;font-size:10px;font-weight:500">{tool}</span>')

def progress_bar_html(validated: int, partial: int, frozen: int,
                       in_progress: int, pending: int, skipped: int = 0,
                       failed: int = 0) -> str:
    total = validated + partial + frozen + in_progress + pending + skipped + failed
    if total == 0:
        return ""

    def pct(n):
        return f"{n / total * 100:.1f}%"

    segments = [
        (pct(validated),   "#4ECDC4"),
        (pct(partial),     "#7ECFC7"),
        (pct(in_progress), "#6C5CE7"),
        (pct(frozen),      "#9B6B6B"),
        (pct(failed),      "#9B6B6B"),
        (pct(skipped),     "#7D8590"),
        (pct(pending),     "#243346"),
    ]
    inner = "".join(
        f'<div style="width:{w};height:100%;background:{c}"></div>'
        for w, c in segments if not w.startswith("0.0")
    )
    return (f'<div style="display:flex;height:8px;border-radius:4px;'
            f'overflow:hidden;background:#243346">{inner}</div>')

def elapsed_str(started_at: str) -> str:
    """Human-readable elapsed time from ISO timestamp."""
    if not started_at:
        return ""
    try:
        from datetime import datetime, timezone
        start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = (now - start).total_seconds()
        if delta < 60:
            return f"{int(delta)}s"
        if delta < 3600:
            return f"{int(delta // 60)}m {int(delta % 60)}s"
        return f"{int(delta // 3600)}h {int((delta % 3600) // 60)}m"
    except Exception:
        return ""


# ══════════════════════════════════════════════════════════════════════════════
# Pipeline state persistence (framework-agnostic — caller passes storage dict)
# ══════════════════════════════════════════════════════════════════════════════

_STATE_FILE = Path(__file__).resolve().parents[1] / "jobs" / ".active_pipeline.json"

def save_pipeline_state(storage: dict) -> None:
    """Write active pipeline state to disk. storage = app.storage.user"""
    state = {
        "active_run_id": storage.get("active_run_id", ""),
        "jobs_dir":      storage.get("jobs_dir", ""),
        "pipeline_pid":  storage.get("pipeline_pid"),
        "pipeline_log":  storage.get("pipeline_log", ""),
    }
    try:
        _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _STATE_FILE.write_text(json.dumps(state))
    except Exception:
        pass

def recover_pipeline_state(storage: dict) -> None:
    """Restore state from disk if lost (browser refresh). storage = app.storage.user"""
    if storage.get("pipeline_log"):
        return

    if not _STATE_FILE.exists():
        return

    try:
        state = json.loads(_STATE_FILE.read_text())
        for key in ("active_run_id", "jobs_dir", "pipeline_pid", "pipeline_log"):
            if state.get(key) and not storage.get(key):
                storage[key] = state[key]
    except Exception:
        pass

def clear_pipeline_state(storage: dict) -> None:
    """Clear pipeline state from storage and disk."""
    for key in ("pipeline_log", "pipeline_pid", "active_run_id", "jobs_dir"):
        storage.pop(key, None)
    try:
        _STATE_FILE.unlink(missing_ok=True)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# Pipeline log reader (pure data — no UI rendering)
# ══════════════════════════════════════════════════════════════════════════════

def get_pipeline_log_data(storage: dict) -> dict | None:
    """
    Read pipeline log state. Returns dict with:
      - log_tail: str (last 80 lines)
      - is_running: bool
      - status: "running" | "failed" | "complete" | "ended"
      - run_id: str
      - pid: int | None
    Returns None if no active pipeline log.
    """
    log_path_str = storage.get("pipeline_log")
    pid = storage.get("pipeline_pid")
    run_id = storage.get("active_run_id", "")

    if not log_path_str:
        return None

    log_path = Path(log_path_str)
    if not log_path.exists():
        return None

    # Check if process is still running
    is_running = False
    if pid:
        try:
            os.kill(pid, 0)
            mtime = log_path.stat().st_mtime
            stale_seconds = time.time() - mtime
            is_running = stale_seconds < 120
        except (ProcessLookupError, PermissionError):
            is_running = False

    # Read log tail
    try:
        content = log_path.read_text(errors="replace")
        lines = content.splitlines()
        tail = "\n".join(lines[-80:]) if len(lines) > 80 else content
    except Exception:
        tail = "(reading log file...)"
        lines = []

    # Determine status
    if is_running:
        status = "running"
    else:
        has_error = any("Pipeline FAILED" in l or "Traceback" in l for l in lines[-20:])
        has_success = any("Pipeline complete" in l or "pipeline_complete" in l for l in lines[-20:])
        if has_error:
            status = "failed"
        elif has_success:
            status = "complete"
        else:
            status = "ended"

    return {
        "log_tail": tail,
        "is_running": is_running,
        "status": status,
        "run_id": run_id,
        "pid": pid,
    }