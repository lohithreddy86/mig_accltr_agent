"""
views/submit_job.py
====================
Job submission page: upload files, configure, start pipeline.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

from nicegui import ui, app, events

from ui.helpers import (
    AGENT_STEPS_QUERY_MODE, AGENT_STEPS_PROC_MODE,
    tool_badge, save_pipeline_state, clear_pipeline_state,
)

_PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_PROJECT / "src"))
from sql_migration.core.config_loader import get_config


def _upload_info(e):
    """Extract (name, bytes) from UploadEventArguments — handles NiceGUI version diffs."""
    name = getattr(e, 'name', None) or getattr(getattr(e, 'file', None), 'name', 'unknown')
    if hasattr(e, 'content'):
        data = e.content.read()
    else:
        data = getattr(getattr(e, 'file', None), '_data', b'')
    return name, data


# ══════════════════════════════════════════════════════════════════════════════
# FLOW SVG (shared nodes assembled per mode)
# ══════════════════════════════════════════════════════════════════════════════

def _flow_svg(mode):
    """Build SVG flow diagram. mode = 'sql' | 'proc' | 'ssis'"""
    nodes = [
        '<rect x="265" y="10" width="150" height="34" rx="17" fill="rgba(255,255,255,.04)" stroke="rgba(255,255,255,.1)"/><text x="340" y="31" text-anchor="middle" fill="rgba(255,255,255,.4)" font-size="12" font-weight="500">Source files</text>',
        '<line x1="340" y1="44" x2="340" y2="126" stroke="rgba(255,255,255,.1)" marker-end="url(#a)"/>',
        '<rect x="245" y="126" width="190" height="44" rx="8" fill="rgba(108,92,231,.1)" stroke="rgba(108,92,231,.35)"/><text x="340" y="144" text-anchor="middle" fill="#B8A5FF" font-size="12" font-weight="500">Analysis agent</text><text x="340" y="160" text-anchor="middle" fill="rgba(255,255,255,.2)" font-size="10">AI engine + Lakehouse</text>',
        '<line x1="340" y1="170" x2="340" y2="190" stroke="rgba(255,255,255,.1)" marker-end="url(#a)"/>',
        '<rect x="270" y="190" width="140" height="34" rx="8" fill="rgba(255,255,255,.03)" stroke="rgba(255,255,255,.08)"/><text x="340" y="211" text-anchor="middle" fill="rgba(255,255,255,.35)" font-size="12" font-weight="500">Dependency check</text>',
        '<line x1="340" y1="224" x2="340" y2="244" stroke="rgba(255,255,255,.1)" marker-end="url(#a)"/>',
        '<rect x="245" y="244" width="190" height="44" rx="8" fill="rgba(108,92,231,.1)" stroke="rgba(108,92,231,.35)"/><text x="340" y="262" text-anchor="middle" fill="#B8A5FF" font-size="12" font-weight="500">Strategy planner</text><text x="340" y="278" text-anchor="middle" fill="rgba(255,255,255,.2)" font-size="10">AI + Secure code env</text>',
        '<line x1="340" y1="288" x2="340" y2="308" stroke="rgba(255,255,255,.1)" marker-end="url(#a)"/>',
        '<rect x="270" y="308" width="140" height="34" rx="8" fill="rgba(255,255,255,.03)" stroke="rgba(255,255,255,.08)"/><text x="340" y="329" text-anchor="middle" fill="rgba(255,255,255,.35)" font-size="12" font-weight="500">Risk assessment</text>',
        '<line x1="340" y1="342" x2="287" y2="356" stroke="rgba(255,255,255,.1)" marker-end="url(#a)"/>',
        '<rect x="212" y="356" width="150" height="44" rx="8" fill="rgba(108,92,231,.1)" stroke="rgba(108,92,231,.35)"/><text x="287" y="374" text-anchor="middle" fill="#B8A5FF" font-size="12" font-weight="500">Conversion agent</text><text x="287" y="390" text-anchor="middle" fill="rgba(255,255,255,.2)" font-size="10">AI + Secure code env</text>',
        '<line x1="362" y1="378" x2="392" y2="378" stroke="rgba(255,255,255,.12)" marker-end="url(#a)"/>',
        '<rect x="392" y="356" width="172" height="44" rx="8" fill="rgba(108,92,231,.1)" stroke="rgba(108,92,231,.35)"/><text x="478" y="374" text-anchor="middle" fill="#B8A5FF" font-size="12" font-weight="500">Code validation</text><text x="478" y="390" text-anchor="middle" fill="rgba(255,255,255,.2)" font-size="10">Lakehouse + Secure code env</text>',
        '<path d="M478 400 L478 418 Q478 426 468 426 L222 426 Q212 426 212 416 L212 378" fill="none" stroke="rgba(255,255,255,.12)" stroke-dasharray="4 3" marker-end="url(#a)"/><text x="345" y="440" text-anchor="middle" fill="rgba(255,255,255,.18)" font-size="10">retry</text>',
    ]
    # Output node varies by mode
    if mode == "sql":
        out = '<rect x="200" y="484" width="150" height="42" rx="8" fill="rgba(78,205,196,.08)" stroke="rgba(78,205,196,.25)"/><text x="275" y="501" text-anchor="middle" fill="#7ECFC7" font-size="12" font-weight="500">Trino SQL files</text>'
    else:
        out = '<rect x="200" y="484" width="150" height="42" rx="8" fill="rgba(78,205,196,.08)" stroke="rgba(78,205,196,.25)"/><text x="275" y="501" text-anchor="middle" fill="#7ECFC7" font-size="12" font-weight="500">PySpark files</text>'
    nodes.append('<line x1="478" y1="400" x2="275" y2="484" stroke="rgba(78,205,196,.25)" marker-end="url(#a)"/>')
    nodes.append(out)

    hdr = '<svg width="100%" viewBox="0 0 680 540" xmlns="http://www.w3.org/2000/svg"><defs><marker id="a" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse"><path d="M2 2L8 5L2 8" fill="none" stroke="context-stroke" stroke-width="1.4" stroke-linecap="round"/></marker></defs><g style="font-family:DM Sans,system-ui">'
    return hdr + "".join(nodes) + '</g></svg>'


_DESC = {
    "proc": "Each procedure is analyzed, converted to PySpark with AI, and validated. Failed conversions retry, replan, or pause for human review.",
    "sql":  "Each query is independently converted to Trino SQL and tested against your live lakehouse.",
    "ssis": "SSIS packages are parsed for tasks and execution order. Each task becomes a PySpark script, wired together by an Airflow DAG.",
}


# ══════════════════════════════════════════════════════════════════════════════
# PANEL HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _prow(ico, lbl, val, mono=False, dim=False):
    vc = "pm" if mono else ("pvd" if dim else "pv")
    return (f'<div class="pr"><div class="pll"><div class="pi">{ico}</div>'
            f'<span class="plb">{lbl}</span></div><div class="prr">'
            f'<span class="{vc}">{val}</span><span class="pch">›</span></div></div>')


# ══════════════════════════════════════════════════════════════════════════════
# BUILD PAGE
# ══════════════════════════════════════════════════════════════════════════════

def build_page():
    storage = app.storage.user

    try:
        _cfg = get_config()
        default_model = _cfg.llm.primary_model
    except Exception:
        default_model = "gemini/gemini-2.5-flash"

    # Uploaded file buffers
    uploaded_sql: list[tuple[str, bytes]] = []
    uploaded_udf: list[tuple[str, bytes]] = []
    uploaded_samples: list[tuple[str, bytes]] = []

    # ── Section header ────────────────────────────────────────────────────
    ui.html('<div class="sh"><div class="cd"></div>Source configuration</div>').classes('w-full')

    # ── Two-column layout ─────────────────────────────────────────────────
    with ui.row(wrap=False).classes('w-full items-start gap-5'):

        # ═══ LEFT: Form card ═══
        with ui.column().classes('flex-1 min-w-0 gap-0'):
            with ui.element("div").classes("wc w-full"):
                source_type = ui.select(
                    label="Source type *",
                    options=["stored_procedures", "sql_queries", "ssis_package"],
                    value="stored_procedures",
                ).classes("w-full")

                with ui.row(wrap=False).classes('w-full gap-4'):
                    source_engine = ui.input(
                        label="Source database engine *", placeholder="Oracle 19c",
                    ).props('hint="e.g. Oracle 19c, DB2 11.5"').classes("flex-1")
                    dialect_family = ui.input(
                        label="Dialect family *", placeholder="PL/SQL",
                    ).props('hint="e.g. PL/SQL, T-SQL, BTEQ"').classes("flex-1")

                with ui.row(wrap=False).classes('w-full gap-4'):
                    input_catalog = ui.input(
                        label="Input catalog *", placeholder="lakehouse",
                    ).classes("flex-1")
                    input_schemas = ui.input(
                        label="Input schemas *", placeholder="core, staging, dw",
                    ).props('hint="Comma-separated"').classes("flex-1")

                # SQL upload
                ui.html('<div class="uz-l">Source files *</div>').classes('w-full')
                sql_status = ui.html('').classes('w-full')

                def on_sql(e: events.UploadEventArguments):
                    name, data = _upload_info(e)
                    uploaded_sql.append((name, data))
                    sql_status.content = (
                        f'<span style="color:#4ECDC4;font-size:11px">'
                        f'✅ {len(uploaded_sql)} file(s) ready</span>')

                ui.upload(on_upload=on_sql, multiple=True, auto_upload=True,
                          label="SQL / Proc / SSIS files"
                ).props('accept=".sql,.proc,.bteq,.txt,.dtsx,.ispac"').classes('w-full')

                # UDF upload
                ui.html('<div class="uz-l">UDF definitions (optional)</div>').classes('w-full')

                def on_udf(e: events.UploadEventArguments):
                    uploaded_udf.clear()
                    name, data = _upload_info(e)
                    uploaded_udf.append((name, data))
                    ui.notify(f"UDF: {name}", type="positive")

                ui.upload(on_upload=on_udf, auto_upload=True,
                          label="UDF (.sql / .py / .txt)"
                ).props('accept=".sql,.py,.txt" max-files=1').classes('w-full')

                # Sample upload
                ui.html('<div class="uz-l">Sample data (optional)</div>').classes('w-full')

                def on_sample(e: events.UploadEventArguments):
                    name, data = _upload_info(e)
                    uploaded_samples.append((name, data))
                    ui.notify(f"Sample: {name}", type="positive")

                ui.upload(on_upload=on_sample, multiple=True, auto_upload=True,
                          label="CSV sample data"
                ).props('accept=".csv"').classes('w-full')

        # ═══ RIGHT: Settings panels ═══
        with ui.column().classes('shrink-0 gap-0').style('width:300px'):
            # Hidden form fields (values read on submit)
            run_id_prefix = ui.input(value="migration").style("display:none")
            dry_run = ui.switch(value=False).style("display:none")
            procs_filter = ui.textarea(value="").style("display:none")
            primary_model_input = ui.input(value=default_model).style("display:none")

            ui.html(f'<div class="pc"><div class="pc-t">Pipeline settings</div>'
                    f'{_prow("⚙", "Output format", "AUTO")}'
                    f'{_prow("★", "Run ID prefix", "migration")}'
                    f'{_prow("▶", "Filter procs", "All", dim=True)}'
                    f'<div class="pr"><div class="pll"><div class="pi">●</div>'
                    f'<span class="plb">Dry run</span></div>'
                    f'<div class="tg" onclick="this.classList.toggle(\'on\')">'
                    f'<div class="tg-d"></div></div></div>'
                    f'</div>', sanitize=False).classes('w-full')

            ui.html(f'<div class="pc"><div class="pc-t">Model configuration</div>'
                    f'{_prow("◆", "Primary model", default_model, mono=True)}'
                    f'{_prow("▣", "Validation rows", "0")}'
                    f'</div>').classes('w-full')

    # ── Flow diagram ──────────────────────────────────────────────────────
    with ui.expansion("● How the pipeline works").classes("w-full"):
        with ui.tabs().classes('w-full') as ftabs:
            t_proc = ui.tab('Stored procedures')
            t_sql = ui.tab('SQL queries')
            t_ssis = ui.tab('SSIS packages')
        with ui.tab_panels(ftabs, value=t_proc).classes('w-full'):
            with ui.tab_panel(t_proc):
                ui.html(f'<div class="fw">{_flow_svg("proc")}<div class="fd">{_DESC["proc"]}</div></div>').classes('w-full')
            with ui.tab_panel(t_sql):
                ui.html(f'<div class="fw">{_flow_svg("sql")}<div class="fd">{_DESC["sql"]}</div></div>').classes('w-full')
            with ui.tab_panel(t_ssis):
                ui.html(f'<div class="fw">{_flow_svg("ssis")}<div class="fd">{_DESC["ssis"]}</div></div>').classes('w-full')

    # ── Info + Submit ─────────────────────────────────────────────────────
    ui.html('<div class="ib">ℹ️ Fill in the required fields (*) and upload at least one source file.</div>').classes('w-full')

    async def launch_pipeline():
        # Validate
        errors = []
        if not uploaded_sql:
            errors.append("Upload at least one source file")
        if not (source_engine.value or "").strip():
            errors.append("Source database engine is required")
        if not (dialect_family.value or "").strip():
            errors.append("Dialect family is required")
        if not (input_catalog.value or "").strip():
            errors.append("Input catalog is required")
        if not (input_schemas.value or "").strip():
            errors.append("Input schemas are required")
        if errors:
            for e in errors:
                ui.notify(e, type="negative")
            return

        ui.notify("Setting up run...", type="info")

        ts = time.strftime("%Y%m%d_%H%M%S")
        rid = f"{run_id_prefix.value}_{ts}"
        jobs_dir = _PROJECT / "jobs" / rid
        jobs_dir.mkdir(parents=True, exist_ok=True)
        (jobs_dir / "sql").mkdir(exist_ok=True)

        sql_paths = []
        for name, content in uploaded_sql:
            out = jobs_dir / "sql" / name
            out.write_bytes(content)
            sql_paths.append(str(out))

        udf_path = ""
        if uploaded_udf:
            (jobs_dir / "udfs").mkdir(exist_ok=True)
            out = jobs_dir / "udfs" / uploaded_udf[0][0]
            out.write_bytes(uploaded_udf[0][1])
            udf_path = str(out)

        sample_paths = []
        for name, content in uploaded_samples:
            (jobs_dir / "samples").mkdir(exist_ok=True)
            out = jobs_dir / "samples" / name
            out.write_bytes(content)
            sample_paths.append(str(out))

        schemas_list = [s.strip() for s in input_schemas.value.split(",") if s.strip()]
        filter_list = [p.strip() for p in (procs_filter.value or "").splitlines() if p.strip()]

        job_config = {
            "run_id": rid, "readme_path": "",
            "sql_file_paths": sql_paths, "sample_paths": sample_paths,
            "dry_run": dry_run.value, "primary_model": primary_model_input.value,
            "procs_filter": filter_list,
            "pipeline_config": {
                "source_type": source_type.value, "output_format": "AUTO",
                "source_engine": source_engine.value.strip(),
                "declared_dialect": dialect_family.value.strip(),
                "udf_file_path": udf_path,
                "input_catalog": input_catalog.value.strip(),
                "input_schemas": schemas_list,
            },
        }
        (jobs_dir / "job_config.json").write_text(json.dumps(job_config, indent=2))

        storage["active_run_id"] = rid
        storage["monitor_run_id"] = rid
        storage["jobs_dir"] = str(jobs_dir)

        config_path = str(jobs_dir / "job_config.json")
        log_path = jobs_dir / "pipeline_output.log"
        project_root = str(_PROJECT)

        cmd = [sys.executable, "-m", "sql_migration.main", "run",
               "--job-config", config_path]

        try:
            log_fh = open(log_path, "w")
            proc = subprocess.Popen(
                cmd, cwd=project_root, stdout=log_fh,
                stderr=subprocess.STDOUT,
                env={**os.environ, "PYTHONPATH": str(Path(project_root) / "src")},
            )
            storage["pipeline_pid"] = proc.pid
            storage["pipeline_log"] = str(log_path)
            save_pipeline_state(storage)
            ui.notify(f"Pipeline started — {rid}", type="positive")
            ui.navigate.to("/monitor")

        except Exception as ex:
            ui.notify(f"Failed to start: {ex}", type="negative")

    ui.button("⚡ Start migration pipeline", on_click=launch_pipeline).props(
        'color=deep-purple-6 no-caps unelevated'
    ).classes('w-full').style(
        'padding:12px;border-radius:9px;font-size:14px;font-weight:500;'
        'box-shadow:0 4px 16px rgba(108,92,231,.35)')