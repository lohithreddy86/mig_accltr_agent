"""
db_store.py
===========
DatabaseStore — drop-in replacement for ArtifactStore.

Same public interface as ArtifactStore so every agent and the UI
can swap to DB with a one-line change.  Binary blobs (parquet sample
data) still go to disk; everything structured goes to the DB.

Routing table (what goes where):
──────────────────────────────────────────────────────────────────
ARTIFACT                     → DB TABLE
──────────────────────────────────────────────────────────────────
analysis/dialect_profile    → dialect_profiles
analysis/construct_map      → dialect_profiles.adapter_json
analysis/readme_signals     → dialect_profiles.readme_signals_json
analysis/manifest            → proc_entries + proc_calls + proc_table_refs
analysis/table_registry      → table_registry + table_columns
analysis/dependency_graph    → proc_calls
analysis/complexity_report   → complexity_scores
analysis/conversion_order    → migration_runs.config_snapshot[conv_order]
planning/plan                → proc_plans + proc_chunks
planning/strategy_map        → proc_plans.strategy
planning/chunk_plan          → proc_chunks
planning/write_conflicts     → write_conflicts
orchestrator/checkpoint      → proc_states (one row per proc)
orchestrator/task_*          → proc_plans + proc_chunks (read-only)
orchestrator/replan_request  → proc_states.error JSON + proc_plans
conversion/result_*          → chunk_results
conversion/conversion_log_*  → conversion_logs
conversion/proc_*.py/.sql    → converted_code  (+ filesystem copy for sandbox)
validation/validation_*      → validation_results + column_diffs + todo_items
feedback/developer_feedback_*→ feedback_bundles
feedback/feedback_resolved_* → feedback_resolutions
samples/{proc}/{tbl}.parquet → FILESYSTEM ONLY (binary Spark input)
──────────────────────────────────────────────────────────────────

The path() method still returns a filesystem path — needed by:
  • Sandbox scripts (they run in Podman and read/write via mounted paths)
  • Parquet sample files (binary, must be on disk)
  • Converted code files (C5 writes them, sandbox reads them)

For everything else path() is a fallback never called at runtime.

Usage:
    from sql_migration.core.db_store import DatabaseStore
    store = DatabaseStore(run_id="migration_001")
    store.upsert_proc_state("sp_calc", "PENDING")
    store.get_proc_state("sp_calc")   # → dict
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine

from sql_migration.core.config_loader import get_config
from sql_migration.core.db_models import (
    Base,
    ChunkResult,
    ColumnDiff,
    ComplexityScore,
    ConversionLog,
    ConvertedCode,
    DialectProfile,
    FeedbackBundle,
    FeedbackResolution,
    MigrationRun,
    ProcCall,
    ProcChunk,
    ProcEntry,
    ProcError,
    ProcPlan,
    ProcState,
    ProcTableRef,
    TableColumn,
    TableRegistry,
    TodoItem,
    ValidationResult,
    WriteConflict,
    WriteSemanticsRecord,
    get_engine,
    get_session,
    init_db,
)
from sql_migration.core.logger import get_logger

log = get_logger("db_store")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _utcnow().isoformat()


# ---------------------------------------------------------------------------
# DatabaseStore
# ---------------------------------------------------------------------------

class DatabaseStore:
    """
    SQLAlchemy-backed artifact store.
    Drop-in replacement for ArtifactStore — same public interface.

    Instantiate once per pipeline run:
        store = DatabaseStore(run_id="migration_20250324_143000")
    """

    def __init__(self, run_id: str, engine: Engine | None = None) -> None:
        self.run_id = run_id
        self._engine = engine or init_db()

        cfg = get_config()
        self.base_dir = Path(cfg.paths.artifact_store) / run_id
        self.subdirs  = cfg.paths.subdirs
        self._ensure_dirs()
        self._ensure_run_row()

    # ------------------------------------------------------------------
    # Filesystem helpers (still needed for sandbox + parquet)
    # ------------------------------------------------------------------

    def _subdir(self, agent: str) -> Path:
        subdir_name = getattr(self.subdirs, agent, agent)
        return self.base_dir / subdir_name

    def _ensure_dirs(self) -> None:
        for field in type(self.subdirs).model_fields:
            self._subdir(field).mkdir(parents=True, exist_ok=True)
        (self.base_dir / self.subdirs.feedback).mkdir(parents=True, exist_ok=True)
        (self.base_dir / self.subdirs.samples).mkdir(parents=True, exist_ok=True)

    def path(self, agent: str, filename: str) -> Path:
        """
        Return filesystem path.
        Used by sandbox scripts and for binary files (parquet).
        Not used for structured data — that goes through typed DB methods.
        """
        return self._subdir(agent) / filename

    # ------------------------------------------------------------------
    # Run row bootstrap
    # ------------------------------------------------------------------

    def _ensure_run_row(self) -> None:
        """Create the migration_runs row if it doesn't exist yet."""
        with get_session(self._engine) as s:
            existing = s.execute(
                select(MigrationRun).where(MigrationRun.run_id == self.run_id)
            ).scalar_one_or_none()
            if not existing:
                s.add(MigrationRun(run_id=self.run_id, status="RUNNING"))
                log.debug("db_run_row_created", run_id=self.run_id)

    # ------------------------------------------------------------------
    # Generic write / read (legacy compatibility shim)
    # ------------------------------------------------------------------

    def write(self, agent: str, filename: str, data: Any, wrap: bool = True) -> Path:
        """
        Route structured writes to the appropriate DB table.
        Falls back to filesystem for unrecognised artifacts (forward-compat).
        """
        key = f"{agent}/{filename}"

        # ── Analysis (includes dialect setup artifacts from A0) ────────────
        if key == "analysis/dialect_profile.json":
            self.upsert_dialect_profile(data)
        elif key == "analysis/construct_map.json":
            self._patch_dialect_profile({"adapter_json": data})
        elif key == "analysis/readme_signals.json":
            self._patch_dialect_profile({"readme_signals_json": data})
        elif key == "analysis/manifest.json":
            self.upsert_manifest(data)
        elif key == "analysis/table_registry.json":
            self.upsert_table_registry(data)
        elif key == "analysis/dependency_graph.json":
            self.upsert_dependency_graph(data)
        elif key == "analysis/complexity_report.json":
            self.upsert_complexity_report(data)
        elif key == "analysis/conversion_order.json":
            self._patch_run({"config_snapshot": {"conversion_order": data}})

        # ── Planning ─────────────────────────────────────────────────────────
        elif key == "planning/strategy_map.json":
            self.upsert_strategy_map(data)
        elif key == "planning/chunk_plan.json":
            self.upsert_chunk_plan(data)
        elif key == "planning/plan.json":
            self.upsert_plan(data)
        elif key == "planning/write_conflicts.json":
            self.upsert_write_conflicts(data)

        # ── Orchestrator ─────────────────────────────────────────────────────
        elif key == "orchestrator/checkpoint.json":
            self.upsert_checkpoint(data)
        elif filename.startswith("task_") and agent == "orchestrator":
            pass   # dispatch tasks are ephemeral — not persisted to DB

        # ── Conversion ───────────────────────────────────────────────────────
        elif filename.startswith("result_") and agent == "conversion":
            self.upsert_chunk_result(data)
        elif filename.startswith("conversion_log_") and agent == "conversion":
            self.upsert_conversion_log(data)

        # ── Validation ───────────────────────────────────────────────────────
        elif filename.startswith("validation_") and agent == "validation":
            self.upsert_validation_result(data)
        elif filename.startswith("manual_review_") and agent == "validation":
            pass   # TODO items already stored inside validation_results

        # ── Feedback ─────────────────────────────────────────────────────────
        elif filename.startswith("developer_feedback_") and agent == "feedback":
            proc = filename.replace("developer_feedback_", "").replace(".json", "")
            self.upsert_feedback_bundle(proc, data)
        elif filename.startswith("feedback_resolved_") and agent == "feedback":
            proc = filename.replace("feedback_resolved_", "").replace(".json", "")
            self.upsert_feedback_resolution(proc, data)

        # ── Fallback: write to filesystem ─────────────────────────────────────
        else:
            return self._write_file(agent, filename, data, wrap)

        # Always mirror to filesystem for sandbox access compatibility
        return self._write_file(agent, filename, data, wrap)

    def write_code(self, agent: str, filename: str, code: str) -> Path:
        """
        Write converted code (.py or .sql).
        Stored in DB (converted_code table) AND filesystem (for sandbox access).
        """
        if agent == "conversion" and (filename.endswith(".py") or filename.endswith(".sql")):
            proc_name = filename.replace("proc_", "").rsplit(".", 1)[0]
            language  = "python" if filename.endswith(".py") else "sql"
            self.upsert_converted_code(proc_name, language, code)
        return self._write_file(agent, filename, code, wrap=False)

    def read(self, agent: str, filename: str) -> Any:
        """
        Route structured reads from DB.
        Falls back to filesystem for unrecognised or binary artifacts.
        """
        key = f"{agent}/{filename}"

        if key == "analysis/dialect_profile.json":
            return self.get_dialect_profile()
        if key == "analysis/construct_map.json":
            dp = self._get_dialect_profile_row()
            return (dp or {}).get("adapter_json") or {}
        if key == "analysis/readme_signals.json":
            dp = self._get_dialect_profile_row()
            return (dp or {}).get("readme_signals_json") or {}
        if key == "analysis/manifest.json":
            return self.get_manifest()
        if key == "analysis/table_registry.json":
            return self.get_table_registry()
        if key == "analysis/dependency_graph.json":
            return self.get_dependency_graph()
        if key == "analysis/complexity_report.json":
            return self.get_complexity_report()
        if key == "planning/plan.json":
            return self.get_plan()
        if key == "orchestrator/checkpoint.json":
            return self.get_checkpoint()

        return self._read_file(agent, filename)

    def read_code(self, agent: str, filename: str) -> str:
        """Read converted code — try DB first, fall back to filesystem."""
        if agent == "conversion":
            proc_name = filename.replace("proc_", "").rsplit(".", 1)[0]
            with get_session(self._engine) as s:
                row = s.execute(
                    select(ConvertedCode)
                    .where(ConvertedCode.run_id == self.run_id)
                    .where(ConvertedCode.proc_name == proc_name)
                ).scalar_one_or_none()
                if row:
                    return row.code
        return self._read_file(agent, filename)

    def exists(self, agent: str, filename: str) -> bool:
        """Check existence — DB first, filesystem fallback."""
        key = f"{agent}/{filename}"

        if key == "orchestrator/checkpoint.json":
            with get_session(self._engine) as s:
                count = s.execute(
                    select(ProcState)
                    .where(ProcState.run_id == self.run_id)
                    .limit(1)
                ).scalar_one_or_none()
                return count is not None

        if filename.startswith("developer_feedback_") and agent == "feedback":
            proc = filename.replace("developer_feedback_", "").replace(".json", "")
            with get_session(self._engine) as s:
                row = s.execute(
                    select(FeedbackBundle)
                    .where(FeedbackBundle.run_id == self.run_id)
                    .where(FeedbackBundle.proc_name == proc)
                ).scalar_one_or_none()
                return row is not None

        if filename.startswith("feedback_resolved_") and agent == "feedback":
            proc = filename.replace("feedback_resolved_", "").replace(".json", "")
            with get_session(self._engine) as s:
                row = s.execute(
                    select(FeedbackResolution)
                    .where(FeedbackResolution.run_id == self.run_id)
                    .where(FeedbackResolution.proc_name == proc)
                ).scalar_one_or_none()
                return row is not None

        if agent == "conversion" and (filename.endswith(".py") or filename.endswith(".sql")):
            proc_name = filename.replace("proc_", "").rsplit(".", 1)[0]
            with get_session(self._engine) as s:
                row = s.execute(
                    select(ConvertedCode)
                    .where(ConvertedCode.run_id == self.run_id)
                    .where(ConvertedCode.proc_name == proc_name)
                ).scalar_one_or_none()
                if row:
                    return True

        return (self._subdir(agent) / filename).exists()

    # ------------------------------------------------------------------
    # Human feedback interface (mirrors ArtifactStore)
    # ------------------------------------------------------------------

    def write_feedback_bundle(self, proc_name: str, bundle: dict) -> Path:
        self.upsert_feedback_bundle(proc_name, bundle)
        return self._write_file("feedback", f"developer_feedback_{proc_name}.json", bundle)

    def read_feedback_resolution(self, proc_name: str) -> dict | None:
        with get_session(self._engine) as s:
            row = s.execute(
                select(FeedbackResolution)
                .where(FeedbackResolution.run_id == self.run_id)
                .where(FeedbackResolution.proc_name == proc_name)
            ).scalar_one_or_none()
            if row:
                return {
                    "proc_name":     row.proc_name,
                    "action":        row.action,
                    "notes":         row.notes,
                    "corrected_code": row.corrected_code or "",
                    "resolved_at":   row.resolved_at.isoformat() if row.resolved_at else "",
                    "resolved_by":   row.resolved_by,
                }
        return None

    def write_feedback_resolution(self, proc_name: str, resolution: dict) -> Path:
        self.upsert_feedback_resolution(proc_name, resolution)
        return self._write_file("feedback", f"feedback_resolved_{proc_name}.json", resolution)

    def list_frozen_procs(self) -> list[str]:
        """Return proc names that have a bundle but no resolution."""
        with get_session(self._engine) as s:
            bundles = s.execute(
                select(FeedbackBundle.proc_name)
                .where(FeedbackBundle.run_id == self.run_id)
            ).scalars().all()
            resolved = set(s.execute(
                select(FeedbackResolution.proc_name)
                .where(FeedbackResolution.run_id == self.run_id)
            ).scalars().all())
        return sorted(p for p in bundles if p not in resolved)

    def list_resolved_procs(self) -> list[str]:
        with get_session(self._engine) as s:
            return sorted(s.execute(
                select(FeedbackResolution.proc_name)
                .where(FeedbackResolution.run_id == self.run_id)
            ).scalars().all())

    # ------------------------------------------------------------------
    # artifact_summary (for UI artifact browser)
    # ------------------------------------------------------------------

    def artifact_summary(self) -> list[dict]:
        """Return a flat list of all DB-stored artifacts for this run."""
        summary = []
        now = _now_iso()

        with get_session(self._engine) as s:
            # Dialect profile (from Analysis A0)
            dp = s.execute(select(DialectProfile)
                .where(DialectProfile.run_id == self.run_id)
            ).scalar_one_or_none()
            if dp:
                summary.append({"agent":"analysis","filename":"dialect_profile.json",
                    "written_at":now,"run_id":self.run_id,"size_bytes":0})

            # Analysis — one entry per proc entry
            proc_count = s.execute(
                select(ProcEntry).where(ProcEntry.run_id == self.run_id)
            ).scalars().all()
            if proc_count:
                summary.append({"agent":"analysis","filename":"manifest.json",
                    "written_at":now,"run_id":self.run_id,
                    "size_bytes":len(proc_count)})

            # Converted code
            for cc in s.execute(select(ConvertedCode)
                    .where(ConvertedCode.run_id == self.run_id)
            ).scalars().all():
                ext = ".py" if cc.language == "python" else ".sql"
                summary.append({"agent":"conversion",
                    "filename": f"proc_{cc.proc_name}{ext}",
                    "written_at": cc.written_at.isoformat() if cc.written_at else now,
                    "run_id": self.run_id, "size_bytes": len(cc.code)})

            # Validation results
            for vr in s.execute(select(ValidationResult)
                    .where(ValidationResult.run_id == self.run_id)
            ).scalars().all():
                summary.append({"agent":"validation",
                    "filename": f"validation_{vr.proc_name}.json",
                    "written_at": vr.validated_at.isoformat() if vr.validated_at else now,
                    "run_id": self.run_id, "size_bytes": 0})

        # Merge filesystem entries for samples/parquet
        samples_dir = self._subdir("samples")
        if samples_dir.exists():
            for f in sorted(samples_dir.rglob("*.parquet")):
                summary.append({"agent":"samples","filename":f.name,
                    "path": str(f), "written_at":"",
                    "run_id":self.run_id,"size_bytes":f.stat().st_size})
        return summary

    # ------------------------------------------------------------------
    # Run-level
    # ------------------------------------------------------------------

    def update_run_status(self, status: str) -> None:
        with get_session(self._engine) as s:
            s.execute(
                update(MigrationRun)
                .where(MigrationRun.run_id == self.run_id)
                .values(status=status,
                        completed_at=_utcnow() if status != "RUNNING" else None)
            )

    def refresh_run_counts(self, checkpoint: dict) -> None:
        procs = checkpoint.get("procs", {})
        with get_session(self._engine) as s:
            s.execute(
                update(MigrationRun)
                .where(MigrationRun.run_id == self.run_id)
                .values(
                    total_procs=len(procs),
                    validated_count=sum(1 for p in procs.values() if p.get("status")=="VALIDATED"),
                    partial_count  =sum(1 for p in procs.values() if p.get("status")=="PARTIAL"),
                    frozen_count   =sum(1 for p in procs.values() if p.get("status")=="FROZEN"),
                    skipped_count  =sum(1 for p in procs.values() if p.get("status")=="SKIPPED"),
                )
            )

    def _patch_run(self, fields: dict) -> None:
        with get_session(self._engine) as s:
            s.execute(
                update(MigrationRun)
                .where(MigrationRun.run_id == self.run_id)
                .values(**fields)
            )

    # ------------------------------------------------------------------
    # Detection writes
    # ------------------------------------------------------------------

    def upsert_dialect_profile(self, data: dict) -> None:
        with get_session(self._engine) as s:
            existing = s.execute(select(DialectProfile)
                .where(DialectProfile.run_id == self.run_id)
            ).scalar_one_or_none()
            if existing:
                existing.dialect_id      = data.get("dialect_id", "")
                existing.dialect_display = data.get("dialect_display", "")
                existing.confidence      = data.get("confidence", 0.0)
                existing.resolution      = data.get("resolution", "LLM_PRIMARY")
                existing.adapter_path    = data.get("adapter_path", "")
                existing.fallback_mode   = data.get("fallback_mode", False)
                existing.readme_quality_score = data.get("readme_quality_score", 0.0)
                existing.warnings        = data.get("warnings", [])
            else:
                s.add(DialectProfile(
                    run_id=self.run_id,
                    dialect_id      = data.get("dialect_id", ""),
                    dialect_display = data.get("dialect_display", ""),
                    confidence      = data.get("confidence", 0.0),
                    resolution      = data.get("resolution", "LLM_PRIMARY"),
                    adapter_path    = data.get("adapter_path", ""),
                    fallback_mode   = data.get("fallback_mode", False),
                    readme_quality_score = data.get("readme_quality_score", 0.0),
                    warnings        = data.get("warnings", []),
                ))

    def _patch_dialect_profile(self, fields: dict) -> None:
        with get_session(self._engine) as s:
            existing = s.execute(select(DialectProfile)
                .where(DialectProfile.run_id == self.run_id)
            ).scalar_one_or_none()
            if existing:
                for k, v in fields.items():
                    if hasattr(existing, k):
                        setattr(existing, k, v)
            else:
                # Only pass fields that are valid column names
                valid = {k: v for k, v in fields.items()
                         if k in DialectProfile.__table__.columns.keys()}
                s.add(DialectProfile(run_id=self.run_id, **valid))

    def _get_dialect_profile_row(self) -> dict | None:
        with get_session(self._engine) as s:
            row = s.execute(select(DialectProfile)
                .where(DialectProfile.run_id == self.run_id)
            ).scalar_one_or_none()
            if not row:
                return None
            return {
                "dialect_id": row.dialect_id, "dialect_display": row.dialect_display,
                "confidence": row.confidence, "resolution": row.resolution,
                "adapter_path": row.adapter_path, "fallback_mode": row.fallback_mode,
                "readme_quality_score": row.readme_quality_score,
                "warnings": row.warnings or [],
                "adapter_json": row.adapter_json,
                "readme_signals_json": row.readme_signals_json,
                "run_id": row.run_id,
            }

    def get_dialect_profile(self) -> dict:
        row = self._get_dialect_profile_row()
        if not row:
            raise FileNotFoundError(f"No dialect_profile for run {self.run_id}")
        return {k: v for k, v in row.items()
                if k not in ("adapter_json", "readme_signals_json")}

    # ------------------------------------------------------------------
    # Analysis writes / reads
    # ------------------------------------------------------------------

    def upsert_manifest(self, data: dict) -> None:
        """Write all ProcEntry rows from manifest data."""
        procs = data.get("procs", [])
        with get_session(self._engine) as s:
            # Delete old entries for this run (full replace)
            for old in s.execute(select(ProcEntry)
                    .where(ProcEntry.run_id == self.run_id)
            ).scalars().all():
                s.delete(old)
            s.flush()

            for p in procs:
                entry = ProcEntry(
                    run_id=self.run_id,
                    proc_name=p.get("name", ""),
                    source_file=p.get("source_file", ""),
                    start_line=p.get("start_line", 0),
                    end_line=p.get("end_line", 0),
                    line_count=p.get("line_count", 0),
                    has_cursor=p.get("has_cursor", False),
                    has_dynamic_sql=p.get("has_dynamic_sql", False),
                    has_exception_handler=p.get("has_exception_handler", False),
                    has_unsupported=p.get("has_unsupported", []),
                    unresolved_deps=p.get("unresolved_deps", []),
                    cursor_nesting_depth=p.get("cursor_nesting_depth"),
                    dynamic_sql_patterns_found=p.get("dynamic_sql_patterns_found", []),
                    unsupported_occurrences=p.get("unsupported_occurrences", {}),
                    dialect_functions_in_body=p.get("dialect_functions_in_body", []),
                    unmapped_count=p.get("unmapped_count", 0),
                )
                s.add(entry)
                s.flush()

                for tbl in p.get("tables_read", []):
                    s.add(ProcTableRef(run_id=self.run_id, proc_entry_id=entry.id,
                                       proc_name=entry.proc_name,
                                       table_name=tbl, ref_type="read"))
                for tbl in p.get("tables_written", []):
                    s.add(ProcTableRef(run_id=self.run_id, proc_entry_id=entry.id,
                                       proc_name=entry.proc_name,
                                       table_name=tbl, ref_type="write"))

    def upsert_dependency_graph(self, data: dict) -> None:
        """Write ProcCall edges from dependency graph."""
        with get_session(self._engine) as s:
            for old in s.execute(select(ProcCall)
                    .where(ProcCall.run_id == self.run_id)
            ).scalars().all():
                s.delete(old)
            s.flush()
            for caller, callee in data.get("edges", []):
                s.add(ProcCall(run_id=self.run_id,
                               caller_name=caller, callee_name=callee))
            for proc_name, deps in data.get("external_deps", {}).items():
                for dep in deps:
                    s.add(ProcCall(run_id=self.run_id,
                                   caller_name=proc_name, callee_name=dep,
                                   is_external=True))

    def upsert_table_registry(self, data: dict) -> None:
        with get_session(self._engine) as s:
            for old in s.execute(select(TableRegistry)
                    .where(TableRegistry.run_id == self.run_id)
            ).scalars().all():
                s.delete(old)
            s.flush()
            for source_name, entry in data.get("entries", {}).items():
                tr = TableRegistry(
                    run_id=self.run_id,
                    source_name=source_name,
                    trino_fqn=entry.get("trino_fqn", ""),
                    status=entry.get("status", "MISSING"),
                    is_temp=entry.get("is_temp", False),
                    row_count=entry.get("row_count"),
                )
                s.add(tr); s.flush()
                for col in entry.get("columns", []):
                    s.add(TableColumn(
                        table_id=tr.id,
                        name=col.get("name", ""), type=col.get("type", ""),
                        nullable=col.get("nullable", ""), comment=col.get("comment", ""),
                    ))

    def upsert_complexity_report(self, data: dict) -> None:
        with get_session(self._engine) as s:
            for old in s.execute(select(ComplexityScore)
                    .where(ComplexityScore.run_id == self.run_id)
            ).scalars().all():
                s.delete(old)
            s.flush()
            for score in data.get("scores", []):
                s.add(ComplexityScore(
                    run_id=self.run_id,
                    proc_name=score.get("name", ""),
                    score=score.get("score", "LOW"),
                    rationale=score.get("rationale", ""),
                    priority_flag=score.get("priority_flag", False),
                    shared_utility_candidate=score.get("shared_utility_candidate", False),
                ))

    def get_manifest(self) -> dict:
        with get_session(self._engine) as s:
            entries = s.execute(
                select(ProcEntry).where(ProcEntry.run_id == self.run_id)
            ).scalars().all()
            refs = s.execute(
                select(ProcTableRef).where(ProcTableRef.run_id == self.run_id)
            ).scalars().all()

            reads: dict[str, list] = {}
            writes: dict[str, list] = {}
            for r in refs:
                if r.ref_type == "read":
                    reads.setdefault(r.proc_name, []).append(r.table_name)
                else:
                    writes.setdefault(r.proc_name, []).append(r.table_name)

            procs = [
                {
                    "name": e.proc_name, "source_file": e.source_file,
                    "start_line": e.start_line, "end_line": e.end_line,
                    "line_count": e.line_count,
                    "tables_read": reads.get(e.proc_name, []),
                    "tables_written": writes.get(e.proc_name, []),
                    "has_cursor": e.has_cursor, "has_dynamic_sql": e.has_dynamic_sql,
                    "has_exception_handler": e.has_exception_handler,
                    "has_unsupported": e.has_unsupported or [],
                    "unresolved_deps": e.unresolved_deps or [],
                    "cursor_nesting_depth": e.cursor_nesting_depth,
                    "dialect_functions_in_body": e.dialect_functions_in_body or [],
                    "unmapped_count": e.unmapped_count,
                }
                for e in entries
            ]
        return {"procs": procs, "total_procs": len(procs),
                "total_lines": sum(p["line_count"] for p in procs)}

    def get_table_registry(self) -> dict:
        with get_session(self._engine) as s:
            rows = s.execute(
                select(TableRegistry).where(TableRegistry.run_id == self.run_id)
            ).scalars().all()
            cols_by_table = {}
            for r in rows:
                cols_by_table[r.id] = [
                    {"name": c.name, "type": c.type,
                     "nullable": c.nullable, "comment": c.comment}
                    for c in r.columns
                ]
            entries = {
                r.source_name: {
                    "source_name": r.source_name, "trino_fqn": r.trino_fqn,
                    "status": r.status, "is_temp": r.is_temp,
                    "row_count": r.row_count,
                    "columns": cols_by_table.get(r.id, []),
                }
                for r in rows
            }
        return {"entries": entries}

    def get_dependency_graph(self) -> dict:
        with get_session(self._engine) as s:
            calls = s.execute(
                select(ProcCall).where(ProcCall.run_id == self.run_id)
            ).scalars().all()
        edges = [[c.caller_name, c.callee_name]
                 for c in calls if not c.is_external]
        ext   = {}
        for c in calls:
            if c.is_external:
                ext.setdefault(c.caller_name, []).append(c.callee_name)
        return {"edges": edges, "external_deps": ext, "cycles": [], "has_cycles": False}

    def get_complexity_report(self) -> dict:
        with get_session(self._engine) as s:
            rows = s.execute(
                select(ComplexityScore).where(ComplexityScore.run_id == self.run_id)
            ).scalars().all()
        return {"scores": [
            {"name": r.proc_name, "score": r.score, "rationale": r.rationale,
             "priority_flag": r.priority_flag,
             "shared_utility_candidate": r.shared_utility_candidate}
            for r in rows
        ]}

    # ------------------------------------------------------------------
    # Planning writes / reads
    # ------------------------------------------------------------------

    def upsert_strategy_map(self, data: dict) -> None:
        """Upsert strategy into proc_plans (creates rows if needed)."""
        with get_session(self._engine) as s:
            for entry in data.get("strategies", []):
                existing = s.execute(select(ProcPlan)
                    .where(ProcPlan.run_id == self.run_id)
                    .where(ProcPlan.proc_name == entry["name"])
                ).scalar_one_or_none()
                if existing:
                    existing.strategy = entry.get("strategy", "TRINO_SQL")
                    existing.strategy_rationale = entry.get("rationale", "")
                    existing.shared_utility = entry.get("shared_utility", False)
                else:
                    s.add(ProcPlan(
                        run_id=self.run_id,
                        proc_name=entry["name"],
                        source_file="",
                        strategy=entry.get("strategy", "TRINO_SQL"),
                        strategy_rationale=entry.get("rationale", ""),
                        shared_utility=entry.get("shared_utility", False),
                    ))

    def upsert_chunk_plan(self, data: dict) -> None:
        """Upsert chunk boundaries into proc_chunks."""
        with get_session(self._engine) as s:
            for proc_name, pcp in data.get("procs", {}).items():
                plan = s.execute(select(ProcPlan)
                    .where(ProcPlan.run_id == self.run_id)
                    .where(ProcPlan.proc_name == proc_name)
                ).scalar_one_or_none()
                if not plan:
                    plan = ProcPlan(run_id=self.run_id, proc_name=proc_name,
                                    source_file="", strategy="TRINO_SQL")
                    s.add(plan); s.flush()

                # Delete old chunks for this plan
                for old in plan.chunks:
                    s.delete(old)
                s.flush()

                for idx, chunk in enumerate(pcp.get("chunks", [])):
                    s.add(ProcChunk(
                        plan_id=plan.id,
                        chunk_id=chunk.get("chunk_id", f"{proc_name}_c{idx}"),
                        chunk_index=idx,
                        start_line=chunk.get("start_line", 0),
                        end_line=chunk.get("end_line", 0),
                        line_count=chunk.get("line_count", 0),
                        tables=chunk.get("tables", []),
                        state_vars=chunk.get("state_vars", {}),
                        schema_context=chunk.get("schema_context", {}),
                        construct_hints=chunk.get("construct_hints", {}),
                    ))

    def upsert_plan(self, data: dict) -> None:
        """Full plan upsert — combines strategy + chunks + metadata."""
        with get_session(self._engine) as s:
            for proc_name, pdata in data.get("procs", {}).items():
                existing = s.execute(select(ProcPlan)
                    .where(ProcPlan.run_id == self.run_id)
                    .where(ProcPlan.proc_name == proc_name)
                ).scalar_one_or_none()

                if existing:
                    existing.strategy           = pdata.get("strategy", "TRINO_SQL")
                    existing.strategy_rationale = pdata.get("strategy_rationale", "")
                    existing.source_file        = pdata.get("source_file", "")
                    existing.shared_utility     = pdata.get("shared_utility", False)
                    existing.calls              = pdata.get("calls", [])
                    existing.tables_written     = pdata.get("tables_written", [])
                    existing.unresolved_deps    = pdata.get("unresolved_deps", [])
                    plan = existing
                else:
                    plan = ProcPlan(
                        run_id=self.run_id, proc_name=proc_name,
                        source_file    = pdata.get("source_file", ""),
                        strategy       = pdata.get("strategy", "TRINO_SQL"),
                        strategy_rationale = pdata.get("strategy_rationale", ""),
                        shared_utility = pdata.get("shared_utility", False),
                        calls          = pdata.get("calls", []),
                        tables_written = pdata.get("tables_written", []),
                        unresolved_deps= pdata.get("unresolved_deps", []),
                    )
                    s.add(plan); s.flush()

                # Full chunk replace
                for old_chunk in list(plan.chunks):
                    s.delete(old_chunk)
                s.flush()

                for idx, chunk in enumerate(pdata.get("chunks", [])):
                    s.add(ProcChunk(
                        plan_id=plan.id,
                        chunk_id=chunk.get("chunk_id", f"{proc_name}_c{idx}"),
                        chunk_index=idx,
                        start_line=chunk.get("start_line", 0),
                        end_line=chunk.get("end_line", 0),
                        line_count=chunk.get("line_count", 0),
                        tables=chunk.get("tables", []),
                        state_vars=chunk.get("state_vars", {}),
                        schema_context=chunk.get("schema_context", {}),
                        construct_hints=chunk.get("construct_hints", {}),
                    ))

    def upsert_write_conflicts(self, data: dict) -> None:
        with get_session(self._engine) as s:
            for tbl, writers in data.items():
                existing = s.execute(select(WriteConflict)
                    .where(WriteConflict.run_id == self.run_id)
                    .where(WriteConflict.table_name == tbl)
                ).scalar_one_or_none()
                if existing:
                    existing.writers      = writers
                    existing.writer_count = len(writers)
                else:
                    s.add(WriteConflict(run_id=self.run_id, table_name=tbl,
                                        writers=writers, writer_count=len(writers)))

    def get_plan(self) -> dict:
        with get_session(self._engine) as s:
            plans = s.execute(
                select(ProcPlan).where(ProcPlan.run_id == self.run_id)
            ).scalars().all()
            procs = {}
            for plan in plans:
                chunks = [
                    {"chunk_id": c.chunk_id, "start_line": c.start_line,
                     "end_line": c.end_line, "line_count": c.line_count,
                     "tables": c.tables or [], "state_vars": c.state_vars or {},
                     "schema_context": c.schema_context or {},
                     "construct_hints": c.construct_hints or {}}
                    for c in sorted(plan.chunks, key=lambda x: x.chunk_index)
                ]
                procs[plan.proc_name] = {
                    "proc_name": plan.proc_name, "source_file": plan.source_file,
                    "strategy": plan.strategy,
                    "strategy_rationale": plan.strategy_rationale,
                    "shared_utility": plan.shared_utility,
                    "calls": plan.calls or [],
                    "tables_written": plan.tables_written or [],
                    "unresolved_deps": plan.unresolved_deps or [],
                    "chunks": chunks,
                    "loop_guards": {
                        "max_chunk_retry": plan.loop_guard_max_chunk_retry,
                        "frozen_after":    plan.loop_guard_frozen_after,
                    },
                }
        return {"procs": procs, "run_id": self.run_id,
                "conversion_order": list(procs.keys())}

    # ------------------------------------------------------------------
    # Orchestrator: proc state (THE WRITE HOTSPOT)
    # ------------------------------------------------------------------

    def upsert_proc_state(self, proc_name: str, status: str,
                          chunks_done: list | None = None,
                          current_chunk: str | None = None,
                          retry_count: int = 0,
                          replan_count: int = 0,
                          validation_outcome: str | None = None,
                          started_at: datetime | None = None,
                          completed_at: datetime | None = None) -> None:
        """
        Upsert a single proc's state — the hot path.
        Called after every state transition (~138× for 23 procs).
        """
        with get_session(self._engine) as s:
            existing = s.execute(select(ProcState)
                .where(ProcState.run_id == self.run_id)
                .where(ProcState.proc_name == proc_name)
            ).scalar_one_or_none()

            if existing:
                existing.status             = status
                if chunks_done is not None:
                    existing.chunks_done    = chunks_done
                if current_chunk is not None:
                    existing.current_chunk  = current_chunk
                existing.retry_count        = retry_count
                existing.replan_count       = replan_count
                if validation_outcome is not None:
                    existing.validation_outcome = validation_outcome
                if completed_at:
                    existing.completed_at   = completed_at
                if started_at and not existing.started_at:
                    existing.started_at     = started_at
                existing.updated_at         = _utcnow()
            else:
                s.add(ProcState(
                    run_id=self.run_id, proc_name=proc_name,
                    status=status,
                    chunks_done=chunks_done or [],
                    current_chunk=current_chunk,
                    retry_count=retry_count,
                    replan_count=replan_count,
                    validation_outcome=validation_outcome,
                    started_at=started_at or _utcnow(),
                ))

    def add_proc_error(self, proc_name: str, error_type: str, message: str,
                       agent: str = "", chunk_id: str = "", attempt: int = 1,
                       signature: str = "", occurrence: int = 1,
                       error_class: str = "") -> None:
        """Append an error to a proc's error trail (append-only)."""
        with get_session(self._engine) as s:
            ps = s.execute(select(ProcState)
                .where(ProcState.run_id == self.run_id)
                .where(ProcState.proc_name == proc_name)
            ).scalar_one_or_none()
            if not ps:
                ps = ProcState(run_id=self.run_id, proc_name=proc_name, status="PENDING")
                s.add(ps); s.flush()

            s.add(ProcError(
                proc_state_id=ps.id,
                run_id=self.run_id, proc_name=proc_name,
                error_type=error_type, message=message, agent=agent,
                chunk_id=chunk_id, attempt=attempt,
                signature=signature, occurrence=occurrence, error_class=error_class,
            ))

    def get_proc_state(self, proc_name: str) -> dict | None:
        with get_session(self._engine) as s:
            ps = s.execute(select(ProcState)
                .where(ProcState.run_id == self.run_id)
                .where(ProcState.proc_name == proc_name)
            ).scalar_one_or_none()
            if not ps:
                return None
            return {
                "proc_name": ps.proc_name, "status": ps.status,
                "chunks_done": ps.chunks_done or [],
                "current_chunk": ps.current_chunk,
                "retry_count": ps.retry_count, "replan_count": ps.replan_count,
                "error_history": [
                    {"error_type": e.error_type, "message": e.message,
                     "agent": e.agent, "chunk_id": e.chunk_id,
                     "attempt": e.attempt,
                     "timestamp": e.timestamp.isoformat() if e.timestamp else "",
                     "signature": e.signature, "occurrence": e.occurrence,
                     "error_class": e.error_class}
                    for e in ps.errors
                ],
                "validation_outcome": ps.validation_outcome,
                "started_at": ps.started_at.isoformat() if ps.started_at else "",
                "completed_at": ps.completed_at.isoformat() if ps.completed_at else "",
            }

    def upsert_checkpoint(self, data: dict) -> None:
        """
        Write checkpoint by updating each proc_state row individually.
        Much faster than serialising the entire checkpoint to one JSON file
        because only the changed row is written.
        """
        procs = data.get("procs", {})
        for proc_name, pstate in procs.items():
            self.upsert_proc_state(
                proc_name=proc_name,
                status=pstate.get("status", "PENDING"),
                chunks_done=pstate.get("chunks_done", []),
                current_chunk=pstate.get("current_chunk"),
                retry_count=pstate.get("retry_count", 0),
                replan_count=pstate.get("replan_count", 0),
                validation_outcome=pstate.get("validation_outcome"),
            )
            for err in pstate.get("error_history", []):
                # Only add if not already present (idempotent)
                pass  # errors are added via add_proc_error() not checkpoint bulk load

        self.refresh_run_counts(data)

    def get_checkpoint(self) -> dict:
        """Reconstruct checkpoint dict from proc_states rows."""
        with get_session(self._engine) as s:
            states = s.execute(
                select(ProcState).where(ProcState.run_id == self.run_id)
            ).scalars().all()
            run = s.execute(
                select(MigrationRun).where(MigrationRun.run_id == self.run_id)
            ).scalar_one_or_none()

            procs: dict[str, dict] = {}
            for ps in states:
                errors = [
                    {"error_type": e.error_type, "message": e.message,
                     "agent": e.agent, "chunk_id": e.chunk_id,
                     "attempt": e.attempt,
                     "timestamp": e.timestamp.isoformat() if e.timestamp else "",
                     "signature": e.signature, "occurrence": e.occurrence,
                     "error_class": e.error_class}
                    for e in ps.errors
                ]
                procs[ps.proc_name] = {
                    "proc_name": ps.proc_name, "status": ps.status,
                    "chunks_done": ps.chunks_done or [],
                    "current_chunk": ps.current_chunk,
                    "retry_count": ps.retry_count, "replan_count": ps.replan_count,
                    "error_history": errors,
                    "validation_outcome": ps.validation_outcome,
                    "started_at": ps.started_at.isoformat() if ps.started_at else "",
                    "completed_at": ps.completed_at.isoformat() if ps.completed_at else "",
                }

            run_dict = {
                "started_at": run.started_at.isoformat() if run and run.started_at else "",
                "total_procs": run.total_procs if run else len(procs),
                "validated_count": run.validated_count if run else 0,
                "partial_count":   run.partial_count   if run else 0,
                "frozen_count":    run.frozen_count     if run else 0,
                "skipped_count":   run.skipped_count    if run else 0,
            } if run else {}

        return {
            "run_id": self.run_id, "procs": procs, "updated_at": _now_iso(),
            **run_dict,
        }

    def get_frozen_proc_names(self) -> list[str]:
        """Fast DB query — replaces file glob on 06_developer_feedback/."""
        with get_session(self._engine) as s:
            return s.execute(
                select(ProcState.proc_name)
                .where(ProcState.run_id == self.run_id)
                .where(ProcState.status == "FROZEN")
            ).scalars().all()

    # ------------------------------------------------------------------
    # Conversion writes
    # ------------------------------------------------------------------

    def upsert_chunk_result(self, data: dict) -> None:
        with get_session(self._engine) as s:
            existing = s.execute(select(ChunkResult)
                .where(ChunkResult.run_id == self.run_id)
                .where(ChunkResult.proc_name == data.get("proc_name",""))
                .where(ChunkResult.chunk_id  == data.get("chunk_id",""))
            ).scalar_one_or_none()

            if existing:
                existing.status                   = data.get("status", "")
                existing.converted_code           = data.get("converted_code", "")
                existing.errors                   = data.get("errors", [])
                existing.todo_count               = data.get("todo_count", 0)
                existing.todo_items               = data.get("todo_items", [])
                existing.updated_state_vars       = data.get("updated_state_vars", {})
                existing.self_correction_attempts = data.get("self_correction_attempts", 0)
                existing.duration_s               = data.get("duration_s", 0.0)
            else:
                s.add(ChunkResult(
                    run_id=self.run_id,
                    proc_name=data.get("proc_name",""),
                    chunk_id=data.get("chunk_id",""),
                    status=data.get("status",""),
                    converted_code=data.get("converted_code",""),
                    errors=data.get("errors",[]),
                    todo_count=data.get("todo_count",0),
                    todo_items=data.get("todo_items",[]),
                    updated_state_vars=data.get("updated_state_vars",{}),
                    self_correction_attempts=data.get("self_correction_attempts",0),
                    duration_s=data.get("duration_s",0.0),
                ))

    def upsert_conversion_log(self, data: dict) -> None:
        with get_session(self._engine) as s:
            existing = s.execute(select(ConversionLog)
                .where(ConversionLog.run_id == self.run_id)
                .where(ConversionLog.proc_name == data.get("proc_name",""))
            ).scalar_one_or_none()
            if existing:
                existing.strategy              = data.get("strategy","")
                existing.todos                 = data.get("todos",[])
                existing.warnings              = data.get("warnings",[])
                existing.total_self_corrections= data.get("total_self_corrections",0)
                existing.assembled             = data.get("assembled",False)
                existing.assembly_error        = data.get("assembly_error","")
            else:
                s.add(ConversionLog(
                    run_id=self.run_id,
                    proc_name=data.get("proc_name",""),
                    strategy=data.get("strategy",""),
                    todos=data.get("todos",[]),
                    warnings=data.get("warnings",[]),
                    total_self_corrections=data.get("total_self_corrections",0),
                    assembled=data.get("assembled",False),
                    assembly_error=data.get("assembly_error",""),
                ))

    def upsert_converted_code(self, proc_name: str, language: str, code: str) -> None:
        with get_session(self._engine) as s:
            existing = s.execute(select(ConvertedCode)
                .where(ConvertedCode.run_id == self.run_id)
                .where(ConvertedCode.proc_name == proc_name)
            ).scalar_one_or_none()
            if existing:
                existing.language    = language
                existing.code        = code
                existing.written_at  = _utcnow()
            else:
                s.add(ConvertedCode(run_id=self.run_id, proc_name=proc_name,
                                    language=language, code=code))

    # ------------------------------------------------------------------
    # Write semantics persistence (v6-fix Gap 11)
    # ------------------------------------------------------------------

    def upsert_write_semantics(self, profiles: list[dict]) -> None:
        """
        Persist write semantics records to the DB.

        Args:
            profiles: List of ProcWriteProfile dicts from WriteSemanticsAnalyzer.
                      Each has: proc_name, table_name, intent, confidence,
                      evidence, pyspark_directive, trino_directive, order_index,
                      is_first, is_last.
        """
        with get_session(self._engine) as s:
            for p in profiles:
                existing = s.execute(
                    select(WriteSemanticsRecord)
                    .where(WriteSemanticsRecord.run_id == self.run_id)
                    .where(WriteSemanticsRecord.proc_name == p.get("proc_name", ""))
                    .where(WriteSemanticsRecord.table_name == p.get("table_name", ""))
                ).scalar_one_or_none()

                if existing:
                    existing.intent = p.get("intent", "UNKNOWN")
                    existing.confidence = p.get("confidence", 0.0)
                    existing.evidence = p.get("evidence", [])
                    existing.pyspark_directive = p.get("pyspark_directive", "")
                    existing.trino_directive = p.get("trino_directive", "")
                    existing.order_index = p.get("order_index", -1)
                    existing.is_first = p.get("is_first", False)
                    existing.is_last = p.get("is_last", False)
                else:
                    s.add(WriteSemanticsRecord(
                        run_id=self.run_id,
                        proc_name=p.get("proc_name", ""),
                        table_name=p.get("table_name", ""),
                        intent=p.get("intent", "UNKNOWN"),
                        confidence=p.get("confidence", 0.0),
                        evidence=p.get("evidence", []),
                        pyspark_directive=p.get("pyspark_directive", ""),
                        trino_directive=p.get("trino_directive", ""),
                        order_index=p.get("order_index", -1),
                        is_first=p.get("is_first", False),
                        is_last=p.get("is_last", False),
                    ))

    # ------------------------------------------------------------------
    # Validation writes
    # ------------------------------------------------------------------

    def upsert_validation_result(self, data: dict) -> None:
        proc_name = data.get("proc_name", "")
        with get_session(self._engine) as s:
            existing = s.execute(select(ValidationResult)
                .where(ValidationResult.run_id == self.run_id)
                .where(ValidationResult.proc_name == proc_name)
            ).scalar_one_or_none()

            if existing:
                for old in existing.column_diffs:  s.delete(old)
                for old in existing.todo_items:    s.delete(old)
                s.flush()
                existing.outcome           = data.get("outcome","FAIL")
                existing.validation_level  = data.get("validation_level",1)
                existing.sandbox_row_count = data.get("sandbox_row_count")
                existing.trino_row_count   = data.get("trino_row_count")
                existing.row_deviation_pct = data.get("row_deviation_pct")
                existing.execution_result  = data.get("execution_result")
                existing.warnings          = data.get("warnings",[])
                existing.fail_reason       = data.get("fail_reason","")
                vr = existing
            else:
                vr = ValidationResult(
                    run_id=self.run_id, proc_name=proc_name,
                    outcome=data.get("outcome","FAIL"),
                    validation_level=data.get("validation_level",1),
                    sandbox_row_count=data.get("sandbox_row_count"),
                    trino_row_count=data.get("trino_row_count"),
                    row_deviation_pct=data.get("row_deviation_pct"),
                    execution_result=data.get("execution_result"),
                    warnings=data.get("warnings",[]),
                    fail_reason=data.get("fail_reason",""),
                )
                s.add(vr); s.flush()

            for diff in data.get("column_diffs", []):
                s.add(ColumnDiff(
                    validation_result_id=vr.id,
                    column=diff.get("column",""), status=diff.get("status","MATCH"),
                    severity=diff.get("severity","LOW"), detail=diff.get("detail",""),
                ))
            for todo in data.get("manual_review_items", []):
                s.add(TodoItem(
                    validation_result_id=vr.id,
                    run_id=self.run_id, proc_name=proc_name,
                    line=todo.get("line",0), comment=todo.get("comment",""),
                    original_fn=todo.get("original_fn",""),
                    construct_type=todo.get("construct_type","UNMAPPED_CONSTRUCT"),
                ))

    # ------------------------------------------------------------------
    # Feedback writes
    # ------------------------------------------------------------------

    def upsert_feedback_bundle(self, proc_name: str, data: dict) -> None:
        with get_session(self._engine) as s:
            existing = s.execute(select(FeedbackBundle)
                .where(FeedbackBundle.run_id == self.run_id)
                .where(FeedbackBundle.proc_name == proc_name)
            ).scalar_one_or_none()
            if existing:
                existing.replan_count       = data.get("replan_count", 0)
                existing.retry_count        = data.get("retry_count", 0)
                existing.manifest_entry     = data.get("manifest_entry", {})
                existing.complexity_entry   = data.get("complexity_entry", {})
                existing.chunk_codes        = data.get("chunk_codes", {})
                existing.validation_reports = data.get("validation_reports", {})
                existing.error_history      = data.get("error_history", [])
                existing.manual_review_items= data.get("manual_review_items", [])
                existing.available_actions  = data.get("available_actions", [])
            else:
                s.add(FeedbackBundle(
                    run_id=self.run_id, proc_name=proc_name,
                    replan_count=data.get("replan_count",0),
                    retry_count=data.get("retry_count",0),
                    manifest_entry=data.get("manifest_entry",{}),
                    complexity_entry=data.get("complexity_entry",{}),
                    chunk_codes=data.get("chunk_codes",{}),
                    validation_reports=data.get("validation_reports",{}),
                    error_history=data.get("error_history",[]),
                    manual_review_items=data.get("manual_review_items",[]),
                    available_actions=data.get("available_actions",[]),
                ))

    def upsert_feedback_resolution(self, proc_name: str, data: dict) -> None:
        with get_session(self._engine) as s:
            existing = s.execute(select(FeedbackResolution)
                .where(FeedbackResolution.run_id == self.run_id)
                .where(FeedbackResolution.proc_name == proc_name)
            ).scalar_one_or_none()
            if existing:
                existing.action         = data.get("action","")
                existing.notes          = data.get("notes","")
                existing.corrected_code = data.get("corrected_code","")
                existing.resolved_by    = data.get("resolved_by","human")
            else:
                # Link to bundle if present
                bundle = s.execute(select(FeedbackBundle)
                    .where(FeedbackBundle.run_id == self.run_id)
                    .where(FeedbackBundle.proc_name == proc_name)
                ).scalar_one_or_none()
                s.add(FeedbackResolution(
                    run_id=self.run_id, proc_name=proc_name,
                    bundle_id=bundle.id if bundle else None,
                    action=data.get("action",""),
                    notes=data.get("notes",""),
                    corrected_code=data.get("corrected_code",""),
                    resolved_by=data.get("resolved_by","human"),
                ))

    # ------------------------------------------------------------------
    # Filesystem fallback (for unrecognised artifacts)
    # ------------------------------------------------------------------

    def _write_file(self, agent: str, filename: str, data: Any,
                    wrap: bool = True) -> Path:
        import os as _os
        from datetime import datetime as _dt, timezone as _tz
        dest = self._subdir(agent) / filename
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp  = dest.with_suffix(dest.suffix + f".tmp_{uuid.uuid4().hex[:8]}")
        try:
            if isinstance(data, str) and not filename.endswith(".json"):
                tmp.write_text(data, encoding="utf-8")
            else:
                payload = {
                    "_meta": {"run_id": self.run_id, "agent": agent,
                              "written_at": _dt.now(_tz.utc).isoformat(),
                              "version": "1"},
                    "data": data,
                } if wrap else data
                tmp.write_text(json.dumps(payload, indent=2, default=str),
                               encoding="utf-8")
            _os.replace(tmp, dest)
        except Exception:
            tmp.unlink(missing_ok=True)
            raise
        return dest

    def _read_file(self, agent: str, filename: str) -> Any:
        path = self._subdir(agent) / filename
        if not path.exists():
            raise FileNotFoundError(
                f"Artifact not found: {agent}/{filename} (looked in {path})"
            )
        content = path.read_text(encoding="utf-8")
        if filename.endswith(".json"):
            parsed = json.loads(content)
            if isinstance(parsed, dict) and "_meta" in parsed and "data" in parsed:
                return parsed["data"]
            return parsed
        return content

    def list_files(self, agent: str, pattern: str = "*.json") -> list[Path]:
        return sorted(self._subdir(agent).glob(pattern))

    def list_matching(self, agent: str, prefix: str) -> list[Path]:
        return sorted(p for p in self._subdir(agent).iterdir()
                      if p.name.startswith(prefix))

    def read_meta(self, agent: str, filename: str) -> dict | None:
        path = self._subdir(agent) / filename
        if not path.exists():
            return None
        try:
            parsed = json.loads(path.read_text(encoding="utf-8"))
            return parsed.get("_meta") if isinstance(parsed, dict) else None
        except Exception:
            return None
