"""
views/pipeline_monitor.py
==========================
Live pipeline status: stage stepper, metrics, proc table, downloads.
Polls checkpoint every 3 seconds via ui.timer.
"""

from __future__ import annotations

import io
import json
import sys
import zipfile
from pathlib import Path

from nicegui import ui, app

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from ui.helpers import (
    AGENT_STEPS, STAGE_META, STATUS_COLOURS, STATUS_ICONS, STEP_LABELS,
    TOOL_COLOURS,
    elapsed_str, get_checkpoint, get_conversion_log, get_conversion_step,
    get_conversion_steps, get_converted_code, get_pipeline_status, get_plan,
    get_stage_progress, get_store, get_validation_result, get_validation_step,
    get_validation_steps, progress_bar_html, status_badge, tool_badge,
)


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _step_row_html(step_id, detail, s_status, started="", ended="", **kw):
    """Render a single conversion/validation step as HTML."""
    icons = {"DONE": ("✅", "#4ECDC4"), "RUNNING": ("⏳", "#B8A5FF"),
             "SKIPPED": ("⊘", "#7D8590"), "FAILED": ("❌", "#9B6B6B")}
    icon, clr = icons.get(s_status, ("○", "#484F58"))
    label = STEP_LABELS.get(step_id, step_id)

    dur = ""
    if started and ended:
        try:
            from datetime import datetime, timezone
            t0 = datetime.fromisoformat(started.replace("Z", "+00:00"))
            t1 = datetime.fromisoformat(ended.replace("Z", "+00:00"))
            secs = (t1 - t0).total_seconds()
            dur = f"{secs:.1f}s" if secs < 60 else f"{secs/60:.1f}m"
        except Exception:
            pass

    extra = ""
    if kw.get("result"):
        extra = f' — {kw["result"]}'
    if kw.get("tool_calls"):
        extra += f' ({kw["tool_calls"]} tool calls)'

    return (
        f'<div style="display:flex;align-items:center;gap:8px;'
        f'padding:4px 0;margin-left:16px;border-left:2px solid {clr}33;'
        f'padding-left:12px">'
        f'<span style="font-size:13px;min-width:18px">{icon}</span>'
        f'<span style="color:#1B2838;font-size:12px">{label}{extra}</span>'
        f'<span style="color:#7D8590;font-size:11px;margin-left:auto">{dur}</span>'
        f'</div>'
    )


def _metric_html(val, label, color):
    return (f'<div style="text-align:center">'
            f'<div class="metric-lbl">{label}</div>'
            f'<div class="metric-val" style="color:{color}">{val}</div></div>')


# ══════════════════════════════════════════════════════════════════════════════
# STAGE STEPPER
# ══════════════════════════════════════════════════════════════════════════════

def _build_stepper(store, procs, total, validated, partial):
    stage_prog = get_stage_progress(store)
    has_checkpoint = bool(procs)

    if not stage_prog:
        if not has_checkpoint:
            ui.html('<div class="ib">⏳ No checkpoint yet — has the pipeline started?</div>').classes('w-full')
        return

    stages = stage_prog.get("stages", {})
    pending_stages = []

    for stage_id, meta in STAGE_META.items():
        s = stages.get(stage_id, {"status": "PENDING"})
        s_status = s.get("status", "PENDING")
        summary = s.get("summary", "")

        if stage_id == "orchestration":
            conv_count = sum(1 for p in procs.values()
                            if p.get("status") in ("DISPATCHED", "CHUNK_CONVERTING", "CHUNK_DONE", "ALL_CHUNKS_DONE"))
            val_count = sum(1 for p in procs.values() if p.get("status") == "VALIDATING")
            done_count = sum(1 for p in procs.values()
                            if p.get("status") in ("VALIDATED", "PARTIAL", "FROZEN", "SKIPPED"))

            # Conversion virtual stage
            if s_status == "PENDING":
                pending_stages.append({"icon": "⚙️", "label": "Conversion"})
            elif s_status == "DONE" or (s_status == "RUNNING" and conv_count == 0 and has_checkpoint):
                with ui.expansion("✅ ⚙️ Conversion", value=False).classes('w-full'):
                    ui.html(f'<div style="color:#1B2838;font-size:12px">{total} queries converted</div>')
            else:
                with ui.expansion("⏳ ⚙️ Conversion", value=True).classes('w-full'):
                    ui.html(f'<div style="color:#1B2838;font-size:13px">'
                            f'Converting: {conv_count} in progress | Done: {done_count + val_count}/{total}</div>')

            # Validation virtual stage
            if s_status == "PENDING":
                pending_stages.append({"icon": "🔍", "label": "Validation"})
            elif s_status == "DONE":
                with ui.expansion("✅ 🔍 Validation", value=False).classes('w-full'):
                    ui.html(f'<div style="color:#1B2838;font-size:12px">'
                            f'{summary or f"Validated: {validated} | Partial: {partial}"}</div>')
            elif val_count > 0 or (conv_count == 0 and done_count > 0):
                with ui.expansion("⏳ 🔍 Validation", value=True).classes('w-full'):
                    ui.html(f'<div style="color:#1B2838;font-size:13px">'
                            f'Validating: {val_count} in progress | Done: {done_count}/{total}</div>')
            else:
                pending_stages.append({"icon": "🔍", "label": "Validation"})
            continue

        # Normal stages
        if s_status == "DONE":
            with ui.expansion(f"✅ {meta['icon']} {meta['label']}", value=False).classes('w-full'):
                ui.html(f'<div style="color:#1B2838;font-size:12px">{summary or "Completed"}</div>')
        elif s_status == "RUNNING":
            with ui.expansion(f"⏳ {meta['icon']} {meta['label']}", value=True).classes('w-full'):
                ui.html(f'<div style="color:#1B2838;font-size:13px">{summary or "Processing..."}</div>')
        else:
            pending_stages.append(meta)

    if pending_stages:
        names = " → ".join(f"{m['icon']} {m['label']}" for m in pending_stages)
        ui.html(f'<div style="color:#484F58;font-size:11px;padding:4px 0">Next: {names}</div>').classes('w-full')


# ══════════════════════════════════════════════════════════════════════════════
# DOWNLOADS (shown when pipeline is done)
# ══════════════════════════════════════════════════════════════════════════════

def _build_downloads(store, run_id, procs):
    ui.html('<div class="pg-hr"></div>').classes('w-full')
    ui.html('<div class="section-hdr">Download Results</div>').classes('w-full')

    # Converted code
    try:
        converted_files = store.list_matching("conversion", "proc_")
    except Exception:
        converted_files = []
    sql_files = [f for f in converted_files if f.suffix in (".sql", ".py")]

    if sql_files:
        with ui.expansion(f"📄 Converted Code ({len(sql_files)} files)", value=True).classes('w-full'):
            for f in sorted(sql_files):
                fp = str(f)
                with ui.row(wrap=False).classes('w-full items-center').style('padding:2px 0'):
                    ui.html(f'<code style="color:#B8A5FF;font-size:12px;flex:1">{f.name}</code>')
                    ui.button("⬇", on_click=lambda p=fp: ui.download(p)).props('flat dense size=sm')

            # Zip all
            def dl_all():
                buf = io.BytesIO()
                with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                    for f in sql_files:
                        zf.writestr(f.name, f.read_text(encoding="utf-8", errors="replace"))
                ui.download(buf.getvalue(), f"converted_{run_id}.zip")

            ui.button("📦 Download All (.zip)", on_click=dl_all).props(
                'color=deep-purple-6 no-caps unelevated').classes('w-full').style('margin-top:6px')

    # Failed conversions
    converted_names = {f.stem.replace("proc_", "") for f in sql_files} if sql_files else set()
    failed = [(n, s) for n, s in procs.items()
              if n not in converted_names and s.get("status") in ("FROZEN", "FAILED")]
    if failed:
        with ui.expansion(f"❌ Failed Conversions ({len(failed)})", value=True).classes('w-full'):
            for proc_name, state in failed:
                errs = state.get("error_history", [])
                reason = errs[-1].get("message", "") if errs and isinstance(errs[-1], dict) else f"Status: {state.get('status')}"
                ui.html(
                    f'<div style="background:#FBF5F5;border:1px solid #9B6B6B;'
                    f'border-radius:5px;padding:8px 12px;margin-bottom:4px">'
                    f'<code style="color:#9B8080;font-size:13px">{proc_name}</code>'
                    f'<div style="color:#7D8590;font-size:11px;margin-top:4px">{reason[:300]}</div></div>'
                ).classes('w-full')

    # Validation summary
    try:
        val_files = [f for f in store.list_matching("validation", "validation_")
                     if "progress" not in f.name]
    except Exception:
        val_files = []

    if val_files:
        with ui.expansion(f"✅ Validation Summary ({len(val_files)})", value=True).classes('w-full'):
            for f in sorted(val_files):
                try:
                    vdata = json.loads(f.read_text(encoding="utf-8"))
                    if isinstance(vdata, dict) and "_meta" in vdata and "data" in vdata:
                        vdata = vdata["data"]
                    proc = vdata.get("proc_name", f.stem.replace("validation_", ""))
                    outcome = vdata.get("outcome", "?")
                    oc = {"PASS": "#4ECDC4", "PARTIAL": "#7ECFC7", "FAIL": "#9B6B6B"}.get(outcome, "#7D8590")
                    ui.html(
                        f'<div style="background:#F1F5F9;border-radius:6px;padding:6px 12px;margin-bottom:4px">'
                        f'<code style="color:#6C5CE7;font-size:12px">{proc}</code>'
                        f'<span style="color:{oc};font-weight:700;margin-left:8px">{outcome}</span></div>'
                    ).classes('w-full')
                except Exception:
                    pass


# ══════════════════════════════════════════════════════════════════════════════
# CIRCUIT BREAKER
# ══════════════════════════════════════════════════════════════════════════════

def _build_circuit_breaker(store):
    try:
        sys_err_data = store.read("orchestrator", "systematic_errors.json")
        circuit_state = sys_err_data.get("circuit_state", "CLOSED")
        sys_errors = sys_err_data.get("systematic_errors", [])
    except Exception:
        return

    if circuit_state == "OPEN" and sys_errors:
        ui.html(
            '<div style="background:#FBF5F5;border:2px solid rgba(220,180,180,0.5);border-radius:10px;'
            'padding:16px 20px;margin:12px 0">'
            '<span style="color:#9B8080;font-size:16px;font-weight:800">⚡ Circuit Breaker OPEN</span>'
            '<br><span style="color:#7D8590;font-size:12px">'
            'Systematic cross-proc error detected. All PENDING dispatches paused.</span></div>'
        ).classes('w-full')

        for se in sys_errors:
            source = se.get("suspected_source", "unknown").upper()
            sig = se.get("signature", "?")[:80]
            affected = se.get("affected_procs", [])
            rec = se.get("recommendation", "")
            ui.html(
                f'<div style="background:#F8FAFC;border-left:3px solid #6C5CE7;'
                f'border-radius:0 6px 6px 0;padding:10px 14px;margin:6px 0">'
                f'<span style="color:#B8A5FF;font-weight:700;font-size:12px">[{source}]</span> '
                f'<code style="color:#1B2838;font-size:11px">{sig}</code>'
                f'<br><span style="color:#7D8590;font-size:11px">'
                f'Affected: {", ".join(affected[:5])}{"..." if len(affected) > 5 else ""}</span>'
                f'<br><span style="color:#7D8590;font-size:11px">→ {rec[:200]}</span></div>'
            ).classes('w-full')

        # Acknowledge form
        with ui.row(wrap=False).classes('w-full items-end gap-4').style('margin-top:8px'):
            ack_reason = ui.input(label="What did you fix?",
                                   placeholder="e.g. Fixed NVL mapping in adapters/oracle.json"
                                   ).classes('flex-1')

            def do_ack():
                if not (ack_reason.value or "").strip():
                    ui.notify("Describe what you fixed.", type="warning")
                    return
                try:
                    from sql_migration.core.global_error_aggregator import GlobalErrorAggregator
                    agg = GlobalErrorAggregator(store=store)
                    agg._circuit_state = "OPEN"
                    agg.write_ack_signal(reason=ack_reason.value, reset_procs=True)
                    ui.notify("✅ Acknowledged — pipeline will resume.", type="positive")
                except Exception as e:
                    ui.notify(f"Failed: {e}", type="negative")

            ui.button("⚡ Acknowledge & Resume", on_click=do_ack).props(
                'color=deep-purple-6 no-caps')

    elif circuit_state == "ACKNOWLEDGED":
        ui.html('<div class="ib">⚡ Circuit breaker was acknowledged — pipeline is resuming.</div>').classes('w-full')


# ══════════════════════════════════════════════════════════════════════════════
# PROC TABLE
# ══════════════════════════════════════════════════════════════════════════════

def _build_proc_table(store, procs, plan):
    ui.html('<div class="section-hdr">Procedure Status</div>').classes('w-full')

    order = plan.get("conversion_order", list(procs.keys())) if plan else list(procs.keys())
    if not order:
        ui.html('<div style="color:#7D8590;font-size:13px;padding:20px 0;text-align:center">'
                '⏳ Waiting for orchestration to begin</div>').classes('w-full')
        return

    for proc_name in order:
        state = procs.get(proc_name, {})
        if not state:
            continue

        status = state.get("status", "PENDING")
        color = STATUS_COLOURS.get(status, "#7D8590")
        icon = STATUS_ICONS.get(status, "•")
        _cd = state.get("chunks_done", 0)
        chunks_done = _cd if isinstance(_cd, int) else len(_cd)
        replan_count = state.get("replan_count", 0)
        _eh = state.get("error_count", state.get("error_history", 0))
        err_count = _eh if isinstance(_eh, int) else len(_eh)

        plan_entry = (plan.get("procs", {}) or {}).get(proc_name, {}) if plan else {}
        total_chunks = len(plan_entry.get("chunks", [])) or 1
        strategy = plan_entry.get("strategy", "—")

        # Proc header row
        chunk_clr = "#4ECDC4" if chunks_done == total_chunks else "#7D8590"
        rp_clr = "#B8A5FF" if replan_count > 0 else "#484F58"

        header_html = (
            f'<div style="background:#F8FAFC;border:1px solid {color}44;border-left:3px solid {color};'
            f'border-radius:0 8px 8px 0;padding:10px 14px;margin-bottom:4px;'
            f'display:flex;align-items:center;gap:12px;flex-wrap:wrap">'
            f'<span style="color:#1B2838;font-weight:700;font-size:14px">{proc_name}</span>'
            f'<code style="color:#7D8590;font-size:11px">{strategy}</code>'
            f'{status_badge(status)}'
            f'<span style="color:{chunk_clr};font-size:12px;margin-left:auto">Chunks: {chunks_done}/{total_chunks}</span>'
            f'<span style="color:{rp_clr};font-size:12px">Replans: {replan_count}</span>'
        )
        if status == "FROZEN":
            header_html += '<span style="color:#9B8080;font-size:12px;font-weight:700">⚠️ Needs Review</span>'
        elif err_count:
            header_html += f'<span style="color:#B8A5FF;font-size:12px">{err_count} error(s)</span>'
        header_html += '</div>'

        ui.html(header_html).classes('w-full')

        # Detail expander for non-PENDING procs
        if status != "PENDING":
            with ui.expansion(f"  ↳ {proc_name} detail", value=False).classes('w-full'):
                _build_proc_detail(store, proc_name, state, plan_entry)


def _build_proc_detail(store, proc_name, state, plan_entry):
    """Render conversion timeline, validation timeline, errors, and code preview."""

    # ── Conversion Timeline ───────────────────────────────────────────
    current_chunk = state.get("current_chunk", f"{proc_name}_c0")
    chunk_id = current_chunk or f"{proc_name}_c0"
    conv_steps = get_conversion_steps(store, proc_name, chunk_id)

    if not conv_steps:
        old_conv = get_conversion_step(store, proc_name, chunk_id)
        if old_conv and old_conv.get("step"):
            conv_steps = [{"step": old_conv["step"], "detail": old_conv.get("detail", ""),
                           "status": "RUNNING", "started_at": old_conv.get("updated_at", "")}]

    if conv_steps:
        rows = ""
        for s in conv_steps:
            rows += _step_row_html(s.get("step", "?"), s.get("detail", ""),
                                    s.get("status", "PENDING"), s.get("started_at", ""),
                                    s.get("ended_at", ""), result=s.get("result", ""),
                                    tool_calls=s.get("tool_calls", ""))
        ui.html(
            f'<div style="background:#F8FAFC;border:1px solid #E2E8F0;border-radius:8px;'
            f'padding:10px 14px;margin-bottom:8px">'
            f'<span style="color:#6C5CE7;font-weight:700;font-size:13px">⚙️ Conversion</span>'
            f'<div style="margin-top:6px">{rows}</div></div>'
        ).classes('w-full')

    # ── Validation Timeline ───────────────────────────────────────────
    val_steps = get_validation_steps(store, proc_name)
    if not val_steps:
        old_val = get_validation_step(store, proc_name)
        if old_val and old_val.get("step"):
            val_steps = [{"step": old_val["step"], "detail": old_val.get("detail", ""),
                          "status": "RUNNING", "started_at": old_val.get("updated_at", "")}]

    if val_steps:
        rows = ""
        for s in val_steps:
            rows += _step_row_html(s.get("step", "?"), s.get("detail", ""),
                                    s.get("status", "PENDING"), s.get("started_at", ""),
                                    s.get("ended_at", ""), result=s.get("result", ""))
        ui.html(
            f'<div style="background:#F8FAFC;border:1px solid #E2E8F0;border-radius:8px;'
            f'padding:10px 14px;margin-bottom:8px">'
            f'<span style="color:#B8A5FF;font-weight:700;font-size:13px">🔍 Validation</span>'
            f'<div style="margin-top:6px">{rows}</div></div>'
        ).classes('w-full')

    # ── Error History ─────────────────────────────────────────────────
    errors = state.get("error_history", [])
    if isinstance(errors, list) and errors:
        err_html = ""
        for e in errors[-5:]:
            ts = (e.get("timestamp", "") if isinstance(e, dict) else "")[:19].replace("T", " ")
            msg = e.get("message", str(e))[:150] if isinstance(e, dict) else str(e)[:150]
            etype = e.get("error_type", "") if isinstance(e, dict) else ""
            err_html += (
                f'<div style="background:#F1F5F9;border-radius:6px;padding:6px 10px;'
                f'margin:4px 0 4px 16px">'
                f'<span style="color:#9B6B6B;font-size:11px;font-family:monospace">[{etype}]</span>'
                f'<span style="color:#1B2838;font-size:11px;margin-left:6px">{msg}</span>'
                f'<br><span style="color:#484F58;font-size:10px">{ts}</span></div>'
            )
        ui.html(
            f'<div style="background:#FBF5F5;border:1px solid rgba(220,180,180,0.25);'
            f'border-radius:8px;padding:10px 14px;margin-top:8px">'
            f'<span style="color:#9B6B6B;font-weight:700;font-size:13px">⚠️ Errors ({len(errors)})</span>'
            f'{err_html}</div>'
        ).classes('w-full')

    # ── Code preview + validation result ──────────────────────────────
    with ui.row(wrap=False).classes('w-full gap-4').style('margin-top:8px'):
        with ui.column().classes('flex-1 min-w-0'):
            code, ext = get_converted_code(store, proc_name)
            if code:
                lang = "python" if ext == ".py" else "sql"
                ui.html(f'<div style="font-weight:700;font-size:12px;color:#1B2838;margin-bottom:4px">'
                        f'Converted Code (proc_{proc_name}{ext})</div>').classes('w-full')
                preview = code[:1500] + ("…" if len(code) > 1500 else "")
                ui.html(f'<pre style="background:#0d1117;color:#c9d1d9;border-radius:6px;'
                        f'padding:10px;font-size:11px;overflow-x:auto;max-height:300px;'
                        f'overflow-y:auto">{preview}</pre>').classes('w-full')

        with ui.column().classes('flex-1 min-w-0'):
            val = get_validation_result(store, proc_name)
            if val:
                outcome = val.get("outcome", "—")
                oc = {"PASS": "#4ECDC4", "PARTIAL": "#7ECFC7", "FAIL": "#9B6B6B"}.get(outcome, "#7D8590")
                warnings = ", ".join(val.get("warnings", [])) or val.get("fail_reason", "")
                ui.html(
                    f'<div style="background:#F1F5F9;border-radius:6px;padding:8px 12px">'
                    f'<span style="color:#7D8590;font-size:11px">Validation: </span>'
                    f'<span style="color:{oc};font-weight:700">{outcome}</span>'
                    f'<span style="color:#7D8590;font-size:11px;margin-left:8px">{warnings}</span></div>'
                ).classes('w-full')

            clog = get_conversion_log(store, proc_name)
            if clog and clog.get("todos"):
                todos_html = ""
                for todo in clog["todos"][:5]:
                    todos_html += (
                        f'<div style="background:#F5F3FF;border:1px solid rgba(220,180,180,0.3);'
                        f'border-radius:5px;padding:5px 10px;margin-bottom:3px;'
                        f'font-size:11px;font-family:monospace;color:#B8A5FF">'
                        f'Line {todo.get("line", "?")}: {todo.get("comment", "")[:100]}</div>'
                    )
                ui.html(
                    f'<div style="margin-top:8px"><span style="font-weight:700;font-size:12px;color:#1B2838">'
                    f'⚠️ {len(clog["todos"])} UNMAPPED Construct(s)</span>{todos_html}</div>'
                ).classes('w-full')


# ══════════════════════════════════════════════════════════════════════════════
# BUILD PAGE (main entry point)
# ══════════════════════════════════════════════════════════════════════════════

def build_page():
    storage = app.storage.user

    # ── Run ID + auto-refresh ─────────────────────────────────────────
    with ui.element("div").classes("wc w-full"):
        with ui.row(wrap=False).classes('w-full items-center gap-3'):
            default_run = storage.get("active_run_id", "")
            run_id_input = ui.input(label="Run ID", value=default_run,
                                     placeholder="migration_20250323_143000"
                                     ).classes('flex-1')
            with run_id_input.add_slot('prepend'):
                ui.icon('search').style('color:var(--tl)')

            load_btn = ui.button("Load run", on_click=lambda: refresh()).props(
                'no-caps unelevated'
            ).classes('btn-go').style(
                'width:auto;padding:0 18px;height:38px;font-size:13px')

            ui.html('<div style="width:1px;height:24px;background:#E2E8F0;flex-shrink:0"></div>')

            auto_refresh = ui.switch("Auto-refresh", value=True).style('margin:0')

    # ── Content area (cleared + rebuilt on each refresh) ──────────────
    content = ui.column().classes('w-full gap-0')

    def refresh():
        rid = (run_id_input.value or "").strip()
        content.clear()

        if not rid:
            with content:
                ui.html('<div class="ib">ℹ️ Enter a Run ID above to monitor a pipeline.</div>').classes('w-full')
            return

        store = get_store(rid)
        ck = get_pipeline_status(store)
        plan = get_plan(store)

        # Compute metrics
        procs = {}
        total = validated = partial = frozen = skipped = in_prog = pending = 0
        is_done = False

        if ck:
            procs = ck.get("procs", {})
            if isinstance(procs, list):
                procs = {p["name"]: p for p in procs}
            total = ck.get("total_procs", len(procs))
            validated = ck.get("validated", sum(1 for p in procs.values()
                                                 if (p.get("status") or p.get("validation")) == "VALIDATED"))
            partial = ck.get("partial", sum(1 for p in procs.values() if p.get("status") == "PARTIAL"))
            frozen = ck.get("frozen", sum(1 for p in procs.values() if p.get("status") == "FROZEN"))
            skipped = ck.get("skipped", sum(1 for p in procs.values() if p.get("status") == "SKIPPED"))
            in_prog = ck.get("in_progress", sum(1 for p in procs.values()
                                                  if p.get("status") not in
                                                  {"VALIDATED", "PARTIAL", "FROZEN", "SKIPPED", "PENDING"}))
            pending = ck.get("pending", sum(1 for p in procs.values() if p.get("status") == "PENDING"))
            is_done = ck.get("is_complete", all(
                p.get("status") in {"VALIDATED", "PARTIAL", "FROZEN", "SKIPPED"}
                for p in procs.values()) if procs else False)

        with content:
            # Stage stepper
            _build_stepper(store, procs, total, validated, partial)

            # Metrics row
            ui.html('<div class="pg-hr"></div>').classes('w-full')
            metrics_html = '<div style="display:flex;gap:12px;justify-content:space-around;margin:8px 0">'
            for val, lbl, clr in [
                (total, "Total", "#7D8590"), (validated, "Validated", "#4ECDC4"),
                (partial, "Partial", "#7ECFC7"), (in_prog, "In Progress", "#6C5CE7"),
                (frozen, "Frozen", "#9B6B6B"), (skipped, "Skipped", "#7D8590"),
            ]:
                metrics_html += _metric_html(val, lbl, clr)
            metrics_html += '</div>'
            ui.html(metrics_html).classes('w-full')

            # Progress bar
            ui.html(progress_bar_html(validated, partial, frozen, in_prog, pending,
                                       skipped=skipped)).classes('w-full')

            # Downloads (when done)
            if is_done:
                ui.html('<div class="log-status log-ok">✅ Pipeline complete</div>').classes('w-full')
                _build_downloads(store, rid, procs)

            # Circuit breaker
            _build_circuit_breaker(store)

            # Proc table
            ui.html('<div class="pg-hr"></div>').classes('w-full')
            _build_proc_table(store, procs, plan)

            # Footer
            ui.html('<div class="pg-hr"></div>').classes('w-full')
            updated = (ck.get("updated_at", "") if ck else "")[:19].replace("T", " ")
            status_lbl = "✅ Complete" if is_done else "🟢 Pipeline running"
            ui.html(f'<span style="color:#484F58;font-size:12px">'
                    f'Last checkpoint: {updated or "—"} • {status_lbl}</span>').classes('w-full')

            if not is_done:
                ui.button("🔄 Refresh Now", on_click=refresh).props('flat dense no-caps')

    # Initial render
    refresh()

    # Auto-refresh timer
    def poll():
        rid = (run_id_input.value or "").strip()
        if not rid or not auto_refresh.value:
            return
        # Check if pipeline is still running before refreshing
        store = get_store(rid)
        ck = get_pipeline_status(store)
        if ck:
            is_done = ck.get("is_complete", False)
            if not is_done:
                refresh()

    ui.timer(3.0, poll)