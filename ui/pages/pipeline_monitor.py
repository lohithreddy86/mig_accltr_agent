"""
pages/pipeline_monitor.py
=========================
Live pipeline status: proc table, agent steps timeline, per-proc drill-down.
Polls checkpoint.json every 3 seconds.
"""

import sys
import time
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from ui.helpers import (
    AGENT_STEPS, STAGE_META, STATUS_COLOURS, STATUS_ICONS, STEP_LABELS,
    TOOL_COLOURS,
    elapsed_str, get_checkpoint, get_conversion_log, get_conversion_step,
    get_conversion_steps, get_converted_code, get_pipeline_status, get_plan,
    get_stage_progress, get_store, get_validation_result, get_validation_step,
    get_validation_steps, progress_bar_html, status_badge, tool_badge,
)

st.markdown("## 📊 Pipeline Monitor")

# ── Run selector ──────────────────────────────────────────────────────────────
col_r, col_refresh = st.columns([4, 1])
with col_r:
    default_run = st.session_state.get("active_run_id", "")
    # Initialize the widget key if not set
    if "monitor_run_id" not in st.session_state:
        st.session_state["monitor_run_id"] = default_run
    run_id = st.text_input("Run ID",
                            placeholder="migration_20250323_143000",
                            key="monitor_run_id")
with col_refresh:
    st.markdown("<br>", unsafe_allow_html=True)
    auto_refresh = st.checkbox("Auto-refresh", value=True)

if not run_id:
    st.info("Enter a Run ID above to monitor a pipeline.")
    st.stop()

store = get_store(run_id)
# Prefer pipeline_status.json (richer: has_feedback, is_resolved, frozen_procs list)
# Falls back to checkpoint.json if status snapshot not yet written.
status = get_pipeline_status(store)
ck     = status  # Keep backward compat for drill-down code below
plan   = get_plan(store)

if not ck:
    # ── Pre-orchestration stage stepper ──────────────────────────────────
    # checkpoint.json doesn't exist yet, but stage_progress.json may have
    # been written by main.py during Analysis/Planning stages.
    # Show a live stepper so the user knows the pipeline IS running.
    stage_prog = get_stage_progress(store)
    if stage_prog:
        st.markdown("### ⏳ Pre-Orchestration Stages")
        st.markdown(
            '<span style="color:#6B7280;font-size:12px">'
            'The pipeline is preparing — proc-level tracking begins after planning completes.'
            '</span>', unsafe_allow_html=True)
        st.markdown("")

        stages = stage_prog.get("stages", {})
        for stage_id, meta in STAGE_META.items():
            s = stages.get(stage_id, {"status": "PENDING"})
            s_status = s.get("status", "PENDING")
            if s_status == "DONE":
                icon, color, bg = "✅", "#10B981", "#111827"
            elif s_status == "RUNNING":
                icon, color, bg = "⏳", meta["color"], meta["color"] + "11"
            else:
                icon, color, bg = "○", "#4B5563", "#111827"
            summary = s.get("summary", "")
            st.markdown(
                f'<div style="display:flex;align-items:center;gap:12px;padding:10px 14px;'
                f'margin-bottom:4px;background:{bg};'
                f'border-left:3px solid {color};border-radius:0 8px 8px 0">'
                f'<span style="font-size:16px">{icon}</span>'
                f'<span style="color:{color};font-weight:700;font-size:13px;min-width:200px">'
                f'{meta["icon"]} {meta["label"]}</span>'
                f'<span style="color:#9CA3AF;font-size:12px">{summary}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

        cur_num = stage_prog.get("stage_number", "?")
        tot_num = stage_prog.get("total_stages", "?")
        updated = stage_prog.get("updated_at", "")[:19].replace("T", " ")
        st.markdown(
            f'<div style="color:#6B7280;font-size:11px;margin-top:10px">'
            f'Stage {cur_num}/{tot_num} · '
            f'Last update: {updated} · '
            f'Proc table appears after planning completes</div>',
            unsafe_allow_html=True)
    else:
        st.warning(f"No checkpoint found for run `{run_id}`. Has the pipeline started?")

    if auto_refresh:
        time.sleep(3)
        st.rerun()
    st.stop()

# Use pre-computed counts from status snapshot when available,
# fall back to manual computation from checkpoint procs dict
procs     = ck.get("procs", {})
# pipeline_status.json has procs as a list of dicts; checkpoint has procs as a dict
# Normalize: if procs is a list, convert to dict keyed by name
if isinstance(procs, list):
    procs = {p["name"]: p for p in procs}
total     = ck.get("total_procs", len(procs))
validated = ck.get("validated", sum(1 for p in procs.values()
                                     if (p.get("status") or p.get("validation")) == "VALIDATED"))
partial   = ck.get("partial", sum(1 for p in procs.values()
                                   if p.get("status") == "PARTIAL"))
frozen    = ck.get("frozen", sum(1 for p in procs.values()
                                  if p.get("status") == "FROZEN"))
skipped   = ck.get("skipped", sum(1 for p in procs.values()
                                   if p.get("status") == "SKIPPED"))
in_prog   = ck.get("in_progress", sum(1 for p in procs.values()
                                       if p.get("status") not in
                {"VALIDATED","PARTIAL","FROZEN","SKIPPED","PENDING"}))
pending   = ck.get("pending", sum(1 for p in procs.values()
                                   if p.get("status") == "PENDING"))
is_done   = ck.get("is_complete", all(
                p.get("status") in {"VALIDATED","PARTIAL","FROZEN","SKIPPED"}
                for p in procs.values()))

# ── Header metrics ────────────────────────────────────────────────────────────
st.markdown("---")
m1, m2, m3, m4, m5, m6 = st.columns(6)
def metric(col, val, label, color):
    col.markdown(
        f'<div style="text-align:center">'
        f'<div class="metric-lbl">{label}</div>'
        f'<div class="metric-val" style="color:{color}">{val}</div>'
        f'</div>', unsafe_allow_html=True)

metric(m1, total,     "Total",       "#9CA3AF")
metric(m2, validated, "Validated",   "#10B981")
metric(m3, partial,   "Partial",     "#84CC16")
metric(m4, in_prog,   "In Progress", "#8B5CF6")
metric(m5, frozen,    "Frozen",      "#DC2626")
metric(m6, skipped,   "Skipped",     "#6B7280")

# Progress bar
st.markdown("<br>", unsafe_allow_html=True)
st.markdown(
    progress_bar_html(validated, partial, frozen, skipped, in_prog, pending, total),
    unsafe_allow_html=True,
)

if is_done:
    st.success("✅ Pipeline complete")

    # ── Download Results ─────────────────────────────────────────────────
    st.markdown("---")
    st.markdown('<p class="section-hdr">Download Results</p>', unsafe_allow_html=True)

    # ── Group 1: Converted Code ──────────────────────────────────────────
    try:
        converted_files = store.list_matching("conversion", "proc_")
    except (FileNotFoundError, OSError):
        converted_files = []
    sql_files = [f for f in converted_files if f.suffix in (".sql", ".py")]

    if sql_files:
        with st.expander(f"📄 Converted Code ({len(sql_files)} files)", expanded=True):
            for f in sorted(sql_files):
                col_name, col_dl = st.columns([4, 1])
                with col_name:
                    st.markdown(
                        f'<code style="color:#EC4899;font-size:12px">{f.name}</code>',
                        unsafe_allow_html=True,
                    )
                with col_dl:
                    st.download_button(
                        "⬇", f.read_bytes(),
                        file_name=f.name,
                        key=f"dl_{f.name}",
                    )

            # Zip all converted code
            import io, zipfile
            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
                for f in sql_files:
                    zf.writestr(f.name, f.read_text(encoding="utf-8", errors="replace"))
            st.download_button(
                "📦 Download All Converted Code (.zip)",
                zip_buf.getvalue(),
                file_name=f"converted_{run_id}.zip",
                mime="application/zip",
                key="dl_all_code",
                use_container_width=True,
            )

    # ── Failed Conversions ─────────────────────────────────────────────
    failed_procs = []
    converted_names = {f.stem.replace("proc_", "") for f in sql_files} if sql_files else set()
    for proc_name, state in procs.items():
        if proc_name not in converted_names:
            errors = state.get("error_history", [])
            if isinstance(errors, list) and errors:
                last_err = errors[-1] if isinstance(errors[-1], dict) else {"message": str(errors[-1])}
                failed_procs.append((proc_name, last_err.get("message", "Unknown error")))
            elif state.get("status") in ("FROZEN", "FAILED"):
                failed_procs.append((proc_name, f"Status: {state.get('status')}"))

    if failed_procs:
        with st.expander(f"❌ Failed Conversions ({len(failed_procs)} queries)", expanded=True):
            for proc_name, reason in failed_procs:
                st.markdown(
                    f'<div style="background:#1A0000;border:1px solid #DC2626;'
                    f'border-radius:5px;padding:8px 12px;margin-bottom:4px">'
                    f'<code style="color:#FCA5A5;font-size:13px">{proc_name}</code>'
                    f'<span style="color:#6B7280;font-size:12px;margin-left:10px">'
                    f'Conversion failed</span>'
                    f'<div style="color:#9CA3AF;font-size:11px;margin-top:4px">'
                    f'{reason[:300]}</div></div>',
                    unsafe_allow_html=True,
                )

    # ── Group 2: Validation Summary ──────────────────────────────────────
    try:
        val_files = store.list_matching("validation", "validation_")
    except (FileNotFoundError, OSError):
        val_files = []
    val_files = [f for f in val_files if "progress" not in f.name]

    if val_files:
        with st.expander(f"✅ Validation Summary ({len(val_files)} procs)", expanded=True):
            import json as _json
            for f in sorted(val_files):
                try:
                    vdata = _json.loads(f.read_text(encoding="utf-8"))
                    if isinstance(vdata, dict) and "_meta" in vdata and "data" in vdata:
                        vdata = vdata["data"]
                    proc = vdata.get("proc_name", f.stem.replace("validation_", ""))
                    outcome = vdata.get("outcome", "?")
                    level = vdata.get("validation_level", "?")
                    warnings = vdata.get("warnings", [])
                    todo_count = len(vdata.get("manual_review_items", []))

                    outcome_colors = {
                        "VALIDATED": "#10B981", "PARTIAL": "#84CC16",
                        "FAIL": "#DC2626", "SKIPPED": "#6B7280",
                    }
                    oc = outcome_colors.get(outcome, "#6B7280")
                    warn_str = f" — {len(warnings)} warning(s)" if warnings else ""
                    todo_str = f" — {todo_count} TODO(s)" if todo_count else ""

                    col_info, col_dl = st.columns([5, 1])
                    with col_info:
                        st.markdown(
                            f'<div style="display:flex;gap:10px;align-items:center;'
                            f'padding:4px 0;font-size:13px">'
                            f'<code style="color:#D1D5DB">{proc}</code>'
                            f'<span style="color:{oc};font-weight:700">{outcome}</span>'
                            f'<span style="color:#6B7280">Level {level}{warn_str}{todo_str}</span>'
                            f'</div>',
                            unsafe_allow_html=True,
                        )
                    with col_dl:
                        st.download_button(
                            "⬇", f.read_bytes(),
                            file_name=f.name,
                            key=f"dl_val_{f.name}",
                        )
                except Exception:
                    pass

            # Zip all validation results
            import io, zipfile
            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
                for f in val_files:
                    zf.writestr(f.name, f.read_text(encoding="utf-8", errors="replace"))
            st.download_button(
                "📦 Download All Validation Results (.zip)",
                zip_buf.getvalue(),
                file_name=f"validation_{run_id}.zip",
                mime="application/zip",
                key="dl_all_validation",
                use_container_width=True,
            )

    # ── Group 3: Manual Review Items ─────────────────────────────────────
    try:
        review_files = store.list_matching("validation", "manual_review_")
    except (FileNotFoundError, OSError):
        review_files = []
    if review_files:
        with st.expander(f"⚠️ Manual Review Required ({len(review_files)} procs)", expanded=True):
            import json as _json
            for f in sorted(review_files):
                try:
                    items = _json.loads(f.read_text(encoding="utf-8"))
                    if isinstance(items, dict) and "data" in items:
                        items = items["data"]
                    proc = f.stem.replace("manual_review_", "")
                    st.markdown(f"**{proc}** — {len(items)} item(s)")
                    for item in items[:10]:
                        if isinstance(item, dict):
                            line = item.get("line", "?")
                            comment = item.get("comment", item.get("description", ""))
                            st.markdown(
                                f'<div style="background:#2D1B00;border:1px solid #92400E;'
                                f'border-radius:5px;padding:5px 10px;margin-bottom:3px;'
                                f'font-size:11px;font-family:monospace;color:#FCD34D">'
                                f'Line {line}: {comment[:120]}</div>',
                                unsafe_allow_html=True,
                            )
                except Exception:
                    pass

    # ── Group 3b: Semantic Review Logs ────────────────────────────────────
    try:
        sr_files = store.list_matching("conversion", "semantic_review_")
    except (FileNotFoundError, OSError):
        sr_files = []
    sr_files = [f for f in sr_files if f.suffix == ".txt"]

    if sr_files:
        with st.expander(f"🔍 Semantic Review Logs ({len(sr_files)} queries)"):
            for f in sorted(sr_files):
                proc = f.stem.replace("semantic_review_", "")
                try:
                    content = f.read_text(encoding="utf-8", errors="replace")
                    passed = "PASSED" in content[:200]
                    icon = "✅" if passed else "⚠️"
                except Exception:
                    content = ""
                    icon = "?"
                col_info, col_dl = st.columns([4, 1])
                with col_info:
                    st.markdown(
                        f'{icon} <code style="color:#D1D5DB;font-size:12px">{proc}</code>',
                        unsafe_allow_html=True,
                    )
                with col_dl:
                    st.download_button(
                        "⬇", content.encode("utf-8"),
                        file_name=f.name,
                        key=f"dl_sr_{f.name}",
                    )

    # ── Group 4: Semantic Map ────────────────────────────────────────────
    try:
        sem_map = store.read("analysis", "semantic_map.json")
        # PipelineSemanticMap uses "proc_summaries" as the key
        entries = sem_map.get("proc_summaries",
                    sem_map.get("entries",
                    sem_map.get("procs", [])))
        if entries:
            with st.expander(f"📋 Semantic Map — What Each Query Does ({len(entries)} entries)", expanded=True):
                if isinstance(entries, list):
                    for entry in entries:
                        if isinstance(entry, dict):
                            proc = entry.get("proc_name", entry.get("name", "?"))
                            purpose = entry.get("business_purpose", entry.get("summary", ""))
                        else:
                            proc = str(entry)
                            purpose = ""
                        st.markdown(
                            f'<div style="padding:4px 0;font-size:13px">'
                            f'<code style="color:#06B6D4">{proc}</code> — '
                            f'<span style="color:#D1D5DB">{purpose}</span></div>',
                            unsafe_allow_html=True,
                        )
                elif isinstance(entries, dict):
                    for proc, info in entries.items():
                        purpose = info if isinstance(info, str) else info.get("business_purpose", info.get("summary", ""))
                        st.markdown(
                            f'<div style="padding:4px 0;font-size:13px">'
                            f'<code style="color:#06B6D4">{proc}</code> — '
                            f'<span style="color:#D1D5DB">{purpose}</span></div>',
                            unsafe_allow_html=True,
                        )
    except Exception:
        pass

    st.markdown("---")
elif in_prog > 0:
    st.markdown(
        f'<div style="color:#8B5CF6;font-size:13px;margin-top:4px">'
        f'⚙️ {in_prog} proc(s) actively converting  •  elapsed: {elapsed_str(ck.get("started_at",""))}'
        f'</div>', unsafe_allow_html=True)

# ── Circuit breaker status ────────────────────────────────────────────────────
try:
    sys_err_data = store.read("orchestrator", "systematic_errors.json")
    circuit_state = sys_err_data.get("circuit_state", "CLOSED")
    sys_errors = sys_err_data.get("systematic_errors", [])
except Exception:
    circuit_state = "CLOSED"
    sys_errors = []

if circuit_state == "OPEN" and sys_errors:
    st.markdown(
        '<div style="background:#1A0000;border:2px solid #DC2626;border-radius:10px;'
        'padding:16px 20px;margin:12px 0">'
        '<span style="color:#FCA5A5;font-size:16px;font-weight:800">⚡ Circuit Breaker OPEN</span>'
        '<br><span style="color:#9CA3AF;font-size:12px">'
        'Systematic cross-proc error detected. All PENDING dispatches are paused.</span>'
        '</div>',
        unsafe_allow_html=True,
    )

    for se in sys_errors:
        source = se.get("suspected_source", "unknown").upper()
        sig = se.get("signature", "?")[:80]
        affected = se.get("affected_procs", [])
        rec = se.get("recommendation", "")

        st.markdown(
            f'<div style="background:#0D1117;border-left:3px solid #F7B731;'
            f'border-radius:0 6px 6px 0;padding:10px 14px;margin:6px 0">'
            f'<span style="color:#F7B731;font-weight:700;font-size:12px">[{source}]</span> '
            f'<code style="color:#D1D5DB;font-size:11px">{sig}</code>'
            f'<br><span style="color:#6B7280;font-size:11px">'
            f'Affected: {", ".join(affected[:5])}'
            f'{"..." if len(affected) > 5 else ""}</span>'
            f'<br><span style="color:#8B949E;font-size:11px">→ {rec[:200]}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.markdown("")
    col_ack, col_reason = st.columns([1, 2])
    with col_reason:
        ack_reason = st.text_input(
            "What did you fix?",
            placeholder="e.g. Fixed NVL mapping in adapters/oracle.json",
            key="circuit_ack_reason",
        )
    with col_ack:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("⚡ Acknowledge & Resume", type="primary", key="circuit_ack_btn"):
            if not ack_reason.strip():
                st.warning("Please describe what you fixed.")
            else:
                try:
                    from sql_migration.core.global_error_aggregator import GlobalErrorAggregator
                    agg = GlobalErrorAggregator(store=store)
                    agg._circuit_state = "OPEN"
                    agg.write_ack_signal(reason=ack_reason, reset_procs=True)
                    st.success(
                        "✅ Acknowledgement written. The Orchestrator will resume "
                        "dispatching on its next cycle. Affected FROZEN procs will "
                        "be reset to PENDING."
                    )
                    st.balloons()
                except Exception as e:
                    st.error(f"Failed to write acknowledgement: {e}")

elif circuit_state == "ACKNOWLEDGED":
    st.info("⚡ Circuit breaker was acknowledged — pipeline is resuming.")

st.markdown("---")

# ── Agent steps reference ─────────────────────────────────────────────────────
with st.expander("🗺️ Agent Steps — What Each Agent Does", expanded=False):
    agent_cols = st.columns(3)
    agent_list = list(AGENT_STEPS.items())
    agent_colors = {
        "analysis":"#06B6D4","planning":"#8B5CF6",
        "orchestrator":"#10B981","conversion":"#EC4899","validation":"#3B82F6",
    }
    for idx, (agent_id, steps) in enumerate(agent_list):
        with agent_cols[idx % 3]:
            color = agent_colors[agent_id]
            st.markdown(
                f'<div style="color:{color};font-weight:700;font-size:12px;'
                f'margin-bottom:6px;border-bottom:1px solid {color}33;padding-bottom:4px">'
                f'{agent_id.upper()}</div>',
                unsafe_allow_html=True,
            )
            for step_id, tool, desc in steps:
                tc = TOOL_COLOURS.get(tool, "#6B7280")
                st.markdown(
                    f'<div style="display:flex;gap:6px;align-items:flex-start;margin-bottom:3px">'
                    f'<code style="color:#6B7280;font-size:10px;flex-shrink:0;width:22px">{step_id}</code>'
                    f'<span style="background:{tc}22;color:{tc};font-size:9px;padding:1px 5px;'
                    f'border-radius:3px;flex-shrink:0;font-weight:700">{tool}</span>'
                    f'<span style="color:#9CA3AF;font-size:11px">{desc}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

st.markdown("---")

# ── Proc status table ─────────────────────────────────────────────────────────
st.markdown('<p class="section-hdr">Procedure Status</p>', unsafe_allow_html=True)

# Build display order from plan
order = plan.get("conversion_order", list(procs.keys())) if plan else list(procs.keys())

for proc_name in order:
    state = procs.get(proc_name, {})
    if not state:
        continue

    status  = state.get("status", "PENDING")
    color   = STATUS_COLOURS.get(status, "#6B7280")
    icon    = STATUS_ICONS.get(status, "•")
    # pipeline_status.json writes chunks_done as int; checkpoint.json as list
    _cd = state.get("chunks_done", 0)
    chunks_done  = _cd if isinstance(_cd, int) else len(_cd)
    replan_count = state.get("replan_count", 0)
    retry_count  = state.get("retry_count", 0)
    # pipeline_status.json uses "error_count" (int); checkpoint.json uses "error_history" (list)
    _eh = state.get("error_count", state.get("error_history", 0))
    err_count    = _eh if isinstance(_eh, int) else len(_eh)

    plan_entry   = (plan.get("procs", {}) or {}).get(proc_name, {}) if plan else {}
    total_chunks = len(plan_entry.get("chunks", [])) or 1
    strategy     = plan_entry.get("strategy", "—")

    # Frozen procs show a call-to-action
    is_frozen = status == "FROZEN"
    bg_color  = "#1A0000" if is_frozen else "#111827"
    border    = f"1px solid {color}44"

    # Row container
    with st.container():
        st.markdown(
            f'<div style="background:{bg_color};border:{border};border-left:3px solid {color};'
            f'border-radius:0 8px 8px 0;padding:10px 14px;margin-bottom:6px">',
            unsafe_allow_html=True,
        )

        c1, c2, c3, c4, c5 = st.columns([3, 2, 2, 2, 2])
        with c1:
            st.markdown(
                f'<span style="color:#F3F4F6;font-weight:700;font-size:14px">{proc_name}</span>'
                f'&nbsp;&nbsp;<code style="color:#6B7280;font-size:11px">{strategy}</code>',
                unsafe_allow_html=True,
            )
        with c2:
            st.markdown(status_badge(status), unsafe_allow_html=True)
        with c3:
            chunk_color = "#10B981" if chunks_done == total_chunks else "#6B7280"
            st.markdown(
                f'<span style="color:{chunk_color};font-size:12px">'
                f'Chunks: {chunks_done}/{total_chunks}</span>',
                unsafe_allow_html=True,
            )
        with c4:
            rp_color = "#F59E0B" if replan_count > 0 else "#4B5563"
            st.markdown(
                f'<span style="color:{rp_color};font-size:12px">'
                f'Replans: {replan_count}</span>',
                unsafe_allow_html=True,
            )
        with c5:
            if is_frozen:
                st.markdown(
                    f'<span style="color:#FCA5A5;font-size:12px;font-weight:700">'
                    f'⚠️ Needs Review</span>',
                    unsafe_allow_html=True,
                )
            elif status == "CHUNK_CONVERTING":
                # Show live conversion sub-step (C1/C2/C3/C3.5/C4/C5)
                current_chunk = state.get("current_chunk", "")
                step_data = get_conversion_step(store, proc_name, current_chunk) if current_chunk else None
                if step_data:
                    step_id = step_data.get("step", "")
                    slabel = STEP_LABELS.get(step_id, step_data.get("detail", ""))
                    st.markdown(
                        f'<span style="color:#8B5CF6;font-size:12px">'
                        f'⚙️ {step_id} — {slabel}</span>',
                        unsafe_allow_html=True,
                    )
                elif err_count > 0:
                    st.markdown(
                        f'<span style="color:#F59E0B;font-size:12px">'
                        f'{err_count} error(s)</span>',
                        unsafe_allow_html=True,
                    )
            elif status == "VALIDATING":
                # Show live validation sub-step (V1/V2/V3/V4/V5)
                step_data = get_validation_step(store, proc_name)
                if step_data:
                    step_id = step_data.get("step", "")
                    slabel = STEP_LABELS.get(step_id, step_data.get("detail", ""))
                    st.markdown(
                        f'<span style="color:#F59E0B;font-size:12px">'
                        f'🔍 {step_id} — {slabel}</span>',
                        unsafe_allow_html=True,
                    )
                elif err_count > 0:
                    st.markdown(
                        f'<span style="color:#F59E0B;font-size:12px">'
                        f'{err_count} error(s)</span>',
                        unsafe_allow_html=True,
                    )
            elif err_count > 0:
                st.markdown(
                    f'<span style="color:#F59E0B;font-size:12px">'
                    f'{err_count} error(s)</span>',
                    unsafe_allow_html=True,
                )

        st.markdown("</div>", unsafe_allow_html=True)

    # Expandable detail for non-PENDING procs
    if status not in ("PENDING",):
        with st.expander(f"  ↳ {proc_name} detail", expanded=False):

            # ── Helper: render a step row ─────────────────────────────
            def _step_row(step_id, detail, s_status, started="", ended="", **kw):
                if s_status == "DONE":
                    icon, clr = "✅", "#10B981"
                elif s_status == "RUNNING":
                    icon, clr = "⏳", "#F59E0B"
                elif s_status == "SKIPPED":
                    icon, clr = "⊘", "#6B7280"
                elif s_status == "FAILED":
                    icon, clr = "❌", "#EF4444"
                else:
                    icon, clr = "○", "#4B5563"

                label = STEP_LABELS.get(step_id, step_id)
                # Duration
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

                detail_short = detail[:80] if detail and detail != label else ""

                return (
                    f'<div style="display:flex;align-items:center;gap:8px;'
                    f'padding:4px 0;margin-left:16px;border-left:2px solid {clr}33;'
                    f'padding-left:12px">'
                    f'<span style="font-size:13px;min-width:18px">{icon}</span>'
                    f'<span style="color:{clr};font-weight:600;font-size:12px;'
                    f'min-width:70px">{step_id}</span>'
                    f'<span style="color:#D1D5DB;font-size:12px">{label}{extra}</span>'
                    f'<span style="color:#6B7280;font-size:11px;margin-left:auto">'
                    f'{dur}</span>'
                    f'</div>'
                )

            # ── Conversion Timeline ───────────────────────────────────
            current_chunk = state.get("current_chunk", f"{proc_name}_c0")
            chunk_id = current_chunk or f"{proc_name}_c0"
            conv_steps = get_conversion_steps(store, proc_name, chunk_id)

            if conv_steps:
                st.markdown(
                    f'<div style="background:#0D1117;border:1px solid #1F2937;'
                    f'border-radius:8px;padding:10px 14px;margin-bottom:8px">'
                    f'<span style="color:#8B5CF6;font-weight:700;font-size:13px">'
                    f'⚙️ Conversion</span></div>',
                    unsafe_allow_html=True,
                )
                rows = ""
                for s in conv_steps:
                    rows += _step_row(
                        s.get("step", "?"),
                        s.get("detail", ""),
                        s.get("status", "PENDING"),
                        s.get("started_at", ""),
                        s.get("ended_at", ""),
                        result=s.get("result", ""),
                        tool_calls=s.get("tool_calls", ""),
                    )
                st.markdown(
                    f'<div style="background:#0D1117;border:1px solid #1F2937;'
                    f'border-radius:0 0 8px 8px;padding:6px 8px;margin-top:-12px">'
                    f'{rows}</div>',
                    unsafe_allow_html=True,
                )

            # ── Validation Timeline ───────────────────────────────────
            val_steps = get_validation_steps(store, proc_name)

            if val_steps:
                st.markdown(
                    f'<div style="background:#0D1117;border:1px solid #1F2937;'
                    f'border-radius:8px;padding:10px 14px;margin-bottom:8px;margin-top:8px">'
                    f'<span style="color:#F59E0B;font-weight:700;font-size:13px">'
                    f'🔍 Validation</span></div>',
                    unsafe_allow_html=True,
                )
                rows = ""
                for s in val_steps:
                    rows += _step_row(
                        s.get("step", "?"),
                        s.get("detail", ""),
                        s.get("status", "PENDING"),
                        s.get("started_at", ""),
                        s.get("ended_at", ""),
                        result=s.get("result", ""),
                    )
                st.markdown(
                    f'<div style="background:#0D1117;border:1px solid #1F2937;'
                    f'border-radius:0 0 8px 8px;padding:6px 8px;margin-top:-12px">'
                    f'{rows}</div>',
                    unsafe_allow_html=True,
                )

            # ── Error History ─────────────────────────────────────────
            errors = state.get("error_history", [])
            if errors:
                st.markdown(
                    f'<div style="background:#1A0000;border:1px solid #DC262644;'
                    f'border-radius:8px;padding:10px 14px;margin-top:8px">'
                    f'<span style="color:#F87171;font-weight:700;font-size:13px">'
                    f'⚠️ Errors ({len(errors)})</span></div>',
                    unsafe_allow_html=True,
                )
                for e in errors[-5:]:
                    ts = e.get("timestamp", "")[:19].replace("T", " ")
                    st.markdown(
                        f'<div style="background:#1F2937;border-radius:6px;'
                        f'padding:6px 10px;margin:4px 0 4px 16px">'
                        f'<span style="color:#F87171;font-size:11px;'
                        f'font-family:monospace">[{e.get("error_type","")}]</span>'
                        f'<span style="color:#D1D5DB;font-size:11px;'
                        f'margin-left:6px">{e.get("message","")[:150]}</span>'
                        f'<br><span style="color:#4B5563;font-size:10px">{ts}</span>'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

            # ── Result: Code preview + validation ─────────────────────
            dt1, dt2 = st.columns(2)

            with dt1:
                code, ext = get_converted_code(store, proc_name)
                if code:
                    lang = "python" if ext == ".py" else "sql"
                    st.markdown(f"**Converted Code** (`proc_{proc_name}{ext}`)")
                    st.code(code[:1500] + ("…" if len(code) > 1500 else ""),
                            language=lang)

            with dt2:
                val = get_validation_result(store, proc_name)
                if val:
                    outcome = val.get("outcome", "—")
                    oc = {"PASS":"#10B981","PARTIAL":"#84CC16",
                          "FAIL":"#EF4444"}.get(outcome, "#6B7280")
                    st.markdown(
                        f'<div style="background:#1F2937;border-radius:6px;'
                        f'padding:8px 12px">'
                        f'<span style="color:#9CA3AF;font-size:11px">'
                        f'Validation: </span>'
                        f'<span style="color:{oc};font-weight:700">{outcome}</span>'
                        f'<span style="color:#6B7280;font-size:11px;margin-left:8px">'
                        f'{", ".join(val.get("warnings",[])) or val.get("fail_reason","")}'
                        f'</span></div>',
                        unsafe_allow_html=True,
                    )

                # Conversion log warnings
                clog = get_conversion_log(store, proc_name)
                if clog and clog.get("todos"):
                    st.markdown(
                        f"**⚠️ {len(clog['todos'])} UNMAPPED Construct(s)**")
                    for todo in clog["todos"][:5]:
                        st.markdown(
                            f'<div style="background:#2D1B00;border:1px solid #92400E;'
                            f'border-radius:5px;padding:5px 10px;margin-bottom:3px;'
                            f'font-size:11px;font-family:monospace;color:#FCD34D">'
                            f'Line {todo.get("line","?")}: '
                            f'{todo.get("comment","")[:100]}</div>',
                            unsafe_allow_html=True,
                        )

# ── Auto-refresh footer ───────────────────────────────────────────────────────
st.markdown("---")
col_ts, col_btn = st.columns([3, 1])
with col_ts:
    updated = ck.get("updated_at", "")[:19].replace("T", " ")
    st.markdown(
        f'<span style="color:#4B5563;font-size:12px">'
        f'Last checkpoint: {updated}  •  '
        f'{"🟢 Pipeline running" if not is_done else "✅ Complete"}'
        f'</span>',
        unsafe_allow_html=True,
    )
with col_btn:
    if st.button("🔄 Refresh Now"):
        st.rerun()

if auto_refresh and not is_done:
    time.sleep(3)
    st.rerun()