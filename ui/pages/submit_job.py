"""
pages/submit_job.py
===================
Job submission page: upload README + SQL files, set config overrides, start pipeline.
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from ui.helpers import AGENT_STEPS, tool_badge, save_pipeline_state, clear_pipeline_state
from sql_migration.core.config_loader import get_config

st.markdown("## 🚀 Submit Migration Job")
st.markdown("Upload your source system files and start the conversion pipeline.")
st.markdown("---")

# Load model config
_cfg = get_config()
_default_model = _cfg.llm.primary_model

col1, col2 = st.columns([3, 2])

with col1:
    st.markdown('<p class="section-hdr">Required Documents</p>', unsafe_allow_html=True)

    readme_file = st.file_uploader(
        "README.docx — Source System Onboarding",
        type=["docx"],
        help="Fill in the README template before uploading. This is the primary dialect signal.",
    )

    sql_files = st.file_uploader(
        "SQL Source Files (.sql, .proc, .bteq)",
        type=["sql", "proc", "bteq", "txt"],
        accept_multiple_files=True,
        help="All stored procedure / function / script files to migrate.",
    )

    st.markdown('<p class="section-hdr">Optional — Improves Validation Quality</p>', unsafe_allow_html=True)
    sample_files = st.file_uploader(
        "Sample Input Data (CSV)",
        type=["csv"],
        accept_multiple_files=True,
        help="Sample rows for source tables. Enables LEVEL_3 validation (full reconciliation).",
    )

with col2:
    st.markdown('<p class="section-hdr">Pipeline Configuration</p>', unsafe_allow_html=True)

    run_id_prefix = st.text_input("Run ID Prefix", value="migration",
                                   help="Prefix for this run's artifact directory")
    dry_run = st.checkbox("Dry Run (no LLM calls)", value=False,
                           help="Mock all LLM responses — useful for testing the pipeline")
    procs_filter = st.text_area("Run Only These Procs (one per line)",
                                 height=80,
                                 help="Leave empty to process all procs found in source files",
                                 placeholder="sp_calc_interest\ngenerate_report")

    st.markdown('<p class="section-hdr">Model Configuration</p>', unsafe_allow_html=True)
    primary_model = st.text_input(
        "Primary Model",
        value=_default_model,
        help="LLM model name as served by your LiteLLM proxy. "
             f"Config default: {_default_model}",
    )

st.markdown("---")

# ── Pipeline steps preview ────────────────────────────────────────────────────
with st.expander("📋 What the pipeline will do (5 agents)", expanded=False):
    agent_colors = {
        "analysis": "#06B6D4", "planning": "#8B5CF6",
        "orchestrator": "#10B981", "conversion": "#EC4899", "validation": "#3B82F6",
    }
    agent_labels = {
        "analysis":     "0 · Analysis Agent (A0 Setup → A1 Extraction → A2 Per-Proc LLM → A3 MCP → A4 Synthesis)",
        "planning":     "1 · Planning Agent (Strategy → Chunks → Schema → Plan)",
        "orchestrator": "2 · Orchestrator (Dependency-gated dispatch + retry/replan)",
        "conversion":   "3 · Conversion Agent (per chunk — agentic tool loop)",
        "validation":   "4 · Validation Agent (per proc — dry-run execution)",
    }
    for agent_id, steps in AGENT_STEPS.items():
        color = agent_colors[agent_id]
        st.markdown(
            f'<div style="border-left:3px solid {color};padding:8px 12px;'
            f'background:#111827;border-radius:0 8px 8px 0;margin-bottom:8px">'
            f'<span style="color:{color};font-weight:700;font-size:13px">'
            f'{agent_labels[agent_id]}</span></div>',
            unsafe_allow_html=True,
        )
        for step_id, tool, desc in steps:
            st.markdown(
                f'<div class="step-row">'
                f'<code style="color:#9CA3AF;font-size:11px;width:28px">{step_id}</code>'
                f'{tool_badge(tool)}'
                f'<span style="color:#D1D5DB;font-size:12px">{desc}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# SUBMISSION
# ══════════════════════════════════════════════════════════════════════════════

can_submit = readme_file is not None and len(sql_files or []) > 0

if not can_submit:
    st.info("📄 Please upload the README.docx and at least one SQL source file to proceed.")

if can_submit and st.button("⚡ Start Migration Pipeline", type="primary", use_container_width=True):
    with st.spinner("Setting up run..."):
        # Create a unique run directory
        ts       = time.strftime("%Y%m%d_%H%M%S")
        run_id   = f"{run_id_prefix}_{ts}"
        jobs_dir = Path(__file__).resolve().parents[2] / "jobs" / run_id
        jobs_dir.mkdir(parents=True, exist_ok=True)
        (jobs_dir / "sql").mkdir(exist_ok=True)

        # Save uploaded files
        readme_path = jobs_dir / "README.docx"
        readme_path.write_bytes(readme_file.getvalue())

        sql_paths = []
        for f in sql_files:
            out = jobs_dir / "sql" / f.name
            out.write_bytes(f.getvalue())
            sql_paths.append(str(out))

        sample_paths = []
        if sample_files:
            (jobs_dir / "samples").mkdir(exist_ok=True)
            for f in sample_files:
                out = jobs_dir / "samples" / f.name
                out.write_bytes(f.getvalue())
                sample_paths.append(str(out))

        # Write job config
        job_config = {
            "run_id":        run_id,
            "readme_path":   str(readme_path),
            "sql_file_paths": sql_paths,
            "sample_paths":  sample_paths,
            "dry_run":       dry_run,
            "primary_model": primary_model,
            "procs_filter":  [p.strip() for p in procs_filter.splitlines() if p.strip()],
        }
        (jobs_dir / "job_config.json").write_text(json.dumps(job_config, indent=2))

        st.session_state["active_run_id"] = run_id
        st.session_state["monitor_run_id"] = run_id  # Sync pipeline monitor widget
        st.session_state["jobs_dir"]      = str(jobs_dir)

    # ── Launch pipeline as background subprocess ──────────────────────
    config_path = str(jobs_dir / "job_config.json")
    log_path = jobs_dir / "pipeline_output.log"
    project_root = str(Path(__file__).resolve().parents[2])

    cmd = [
        sys.executable, "-m", "sql_migration.main", "run",
        "--job-config", config_path,
    ]

    try:
        log_fh = open(log_path, "w")
        proc = subprocess.Popen(
            cmd,
            cwd=project_root,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            env={**os.environ, "PYTHONPATH": str(Path(project_root) / "src")},
        )
        # Store in session_state — survives page switches
        st.session_state["pipeline_pid"] = proc.pid
        st.session_state["pipeline_log"] = str(log_path)

        # Write to disk — survives browser refresh
        save_pipeline_state()

        st.rerun()  # Trigger log viewer immediately

    except Exception as e:
        st.error(f"Failed to start pipeline: {e}")
        st.markdown("**Run manually in your terminal:**")
        st.code(f"cd {project_root}\n{' '.join(cmd)}", language="bash")