"""
main.py
=======
CLI entry point for the SQL Migration System.

Usage:
    sql-migrate run --run-id migration_20250323 --readme README.docx --sql proc1.sql proc2.sql
    sql-migrate run --job-config jobs/migration_001/job_config.json
    sql-migrate ui         # Launch Streamlit UI
    sql-migrate status --run-id migration_20250323
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import click

# Make src importable. This file lives at src/sql_migration/main.py, so
# parents[2] is the project root (contains `ui/`, `inputs/`, `artifacts/`).
_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / "src"))

from sql_migration.core.artifact_store import ArtifactStore
from sql_migration.core.config_loader import get_config
from sql_migration.core.human_feedback import HumanFeedbackHandler
from sql_migration.core.logger import configure_logging, get_logger
from sql_migration.core.sandbox import Sandbox
from sql_migration.core.mcp_client import MCPClientSync


@click.group()
def cli():
    """SQL Migration System — migrate Oracle/DB2/Teradata SQL to PySpark/Trino."""
    pass


# ─────────────────────────────────────────────────────────────────────────────
# run command
# ─────────────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--run-id",      default=None,   help="Explicit run ID (auto-generated if omitted)")
@click.option("--readme",      default=None,   help="Path to README.docx")
@click.option("--sql",         multiple=True,  help="SQL source file paths")
@click.option("--job-config",  default=None,   help="Path to job_config.json (alternative to flags)")
@click.option("--dry-run",     is_flag=True,   help="Mock all LLM calls (testing)")
@click.option("--procs",       default="",     help="Comma-separated proc names to process (all if empty)")
def run(run_id, readme, sql, job_config, dry_run, procs):
    """Run the full migration pipeline."""

    # ── Load job config ───────────────────────────────────────────────────────
    job = {}
    if job_config:
        cfg_path = Path(job_config)
        if not cfg_path.exists():
            click.echo(f"Error: job config not found: {job_config}", err=True)
            sys.exit(1)
        job = json.loads(cfg_path.read_text())
        run_id   = run_id or job.get("run_id")
        readme   = readme or job.get("readme_path", "")
        sql      = sql or job.get("sql_file_paths", [])
        dry_run  = dry_run or job.get("dry_run", False)
        procs    = procs  or ",".join(job.get("procs_filter", []))

    if not run_id:
        ts     = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        run_id = f"migration_{ts}"

    # Build PipelineConfig from job_config (Streamlit UI structured inputs)
    from sql_migration.models.analysis import PipelineConfig
    pipeline_config = None
    if job.get("pipeline_config"):
        pc_data = job["pipeline_config"]
        # Handle comma-separated schemas from UI
        schemas = pc_data.get("input_schemas", [])
        if isinstance(schemas, str):
            schemas = [s.strip() for s in schemas.split(",") if s.strip()]
        out_schemas = pc_data.get("output_schemas", [])
        if isinstance(out_schemas, str):
            out_schemas = [s.strip() for s in out_schemas.split(",") if s.strip()]
        pipeline_config = PipelineConfig(
            source_type=pc_data.get("source_type", "stored_procedures"),
            output_format=pc_data.get("output_format", "AUTO"),
            source_engine=pc_data.get("source_engine", ""),
            declared_dialect=pc_data.get("declared_dialect", ""),
            udf_file_path=pc_data.get("udf_file_path", ""),
            input_catalog=pc_data.get("input_catalog", ""),
            input_schemas=schemas,
            output_catalog=pc_data.get("output_catalog", ""),
            output_schemas=out_schemas,
        )

    # README is optional if pipeline_config is provided (Streamlit UI flow)
    if not sql:
        click.echo("Error: --sql is required (or use --job-config)", err=True)
        sys.exit(1)
    if not readme and not pipeline_config:
        click.echo("Error: --readme or pipeline_config is required (or use --job-config)", err=True)
        sys.exit(1)

    # ── Apply config overrides ────────────────────────────────────────────────
    app_cfg = get_config()
    if dry_run:
        app_cfg.run.dry_run = True
    if procs:
        app_cfg.run.procs_filter = [p.strip() for p in procs.split(",") if p.strip()]

    # Apply model override from job config (UI selectbox)
    model_override = job.get("primary_model", "")
    if model_override:
        app_cfg.llm.primary_model = model_override

    # ── Configure logging ─────────────────────────────────────────────────────
    configure_logging(
        level         = app_cfg.logging.level,
        fmt           = app_cfg.logging.format,
        logs_dir      = app_cfg.paths.logs_dir,
        per_agent_files = app_cfg.logging.per_agent_files,
        include_prompts  = app_cfg.logging.include_prompts,
        include_responses = app_cfg.logging.include_responses,
        run_id        = run_id,
    )
    log = get_logger("main")
    log.info("pipeline_start", run_id=run_id, dry_run=dry_run)
    click.echo(f"\n⚙️  SQL Migration — Run: {run_id}\n")

    # ── Initialise shared objects ─────────────────────────────────────────────
    source_dir = str(Path(sql[0]).parent)

    # DatabaseStore: optional DB backend for structured queries (frozen procs,
    # run status tracking, etc). Gracefully degrades to filesystem-only if
    # DB initialization fails (e.g. missing dependencies, permissions).
    db = None
    try:
        from sql_migration.core.db_store import DatabaseStore
        db = DatabaseStore(run_id=run_id)
        log.info("database_store_initialized")
    except Exception as e:
        log.info("database_store_unavailable",
                 error=str(e)[:100],
                 msg="Running in filesystem-only mode")

    store      = ArtifactStore(run_id=run_id, db=db)
    sandbox    = Sandbox(source_dir=source_dir)
    mcp        = MCPClientSync()
    feedback   = HumanFeedbackHandler(store)

    # Track run status in DB
    store.update_run_status("RUNNING")

    # ── Stage progress tracker (for Streamlit UI) ─────────────────────────
    # Pre-orchestration stages (Analysis → Dep Gate → Planning → Preflight)
    # are invisible to the Pipeline Monitor because checkpoint.json doesn't
    # exist yet. This writes a lightweight stage_progress.json so the UI
    # can show which stage is active.
    # NOTE: _write_stage_progress is now inside OrchestratorAgent.

    from sql_migration.agents.orchestrator import OrchestratorAgent

    try:  # Track FAILED status on unhandled exception

        orch = OrchestratorAgent(
            store=store,
            sandbox=sandbox,
            mcp=mcp,
            feedback=feedback,
            run_id=run_id,
            readme_path=readme or "",
            sql_file_paths=list(sql),
            pipeline_config=pipeline_config,
        )
        result = orch.run()

        # ── Handle early exits ────────────────────────────────────────────────
        if result.status == "ABORTED_DEPS":
            return
        if result.status == "DRY_RUN_COMPLETE":
            return
        if result.status == "PARSER_EMPTY":
            click.echo("No SQL-bearing tasks found in SSIS package(s). Nothing to convert.")
            store.update_run_status("PARSER_EMPTY")
            return

        # ── Summary ───────────────────────────────────────────────────────────
        checkpoint = result.checkpoint
        run_status = result.status
        store.update_run_status(run_status)
        store.refresh_run_counts(checkpoint.model_dump())

        click.echo("\n" + "─" * 50)
        click.echo(f"✅  Migration complete — Run ID: {run_id}")
        click.echo(f"   Validated : {checkpoint.validated_count}")
        click.echo(f"   Partial   : {checkpoint.partial_count}")
        click.echo(f"   Frozen    : {checkpoint.frozen_count}")
        click.echo(f"   Skipped   : {checkpoint.skipped_count}")
        if result.has_module_grouping:
            click.echo(f"   Modules   : {checkpoint.total_procs} "
                       f"(from {result.source_proc_count} source procs)")
        click.echo(f"   Artifacts : {app_cfg.paths.artifact_store}/{run_id}")

        if checkpoint.frozen_count > 0:
            click.echo(f"\n⚠️   {checkpoint.frozen_count} proc(s) need human review.")
            click.echo(f"    Open the feedback UI: python3 ui/app.py")
            click.echo(f"    Navigate to 🔴 Frozen Procs and enter run ID: {run_id}")

        # ── Surface systematic error findings ─────────────────────────────────
        sys_errors = result.systematic_errors
        if sys_errors:
            click.echo(f"\n🚨  {len(sys_errors)} systematic cross-proc error(s) detected:")
            for se in sys_errors:
                click.echo(f"     [{se.suspected_source.upper()}] "
                           f"{se.signature[:60]}")
                proc_list = ", ".join(se.affected_procs[:5])
                if len(se.affected_procs) > 5:
                    proc_list += f" (+{len(se.affected_procs) - 5} more)"
                click.echo(f"     Affected: {proc_list}")
                click.echo(f"     → {se.recommendation[:150]}")
            click.echo(f"\n    These errors likely share a single root cause.")
            click.echo(f"    Fix the root cause and re-run to avoid repeated failures.")
            click.echo(f"    Details: {app_cfg.paths.artifact_store}/{run_id}/"
                       f"03_orchestrator/systematic_errors.json")

        # ── Token usage summary ───────────────────────────────────────────────
        token_usage = result.token_usage
        total_input = sum(u.get("input_tokens", 0) for u in token_usage.values())
        total_output = sum(u.get("output_tokens", 0) for u in token_usage.values())
        total_tokens = total_input + total_output
        if total_tokens > 0:
            click.echo(f"\n📊  Token usage:")
            click.echo(f"   Input  : {total_input:,}")
            click.echo(f"   Output : {total_output:,}")
            click.echo(f"   Total  : {total_tokens:,}")
            for agent_name, usage in token_usage.items():
                t = usage.get("total_tokens", 0)
                if t > 0:
                    click.echo(f"     {agent_name:12s}: {t:>8,} tokens")
            click.echo(f"   Estimated: {result.preflight_calls} calls "
                       f"(preflight) vs actual token consumption above")

        log.info("pipeline_complete",
                 run_id=run_id,
                 validated=checkpoint.validated_count,
                 frozen=checkpoint.frozen_count,
                 systematic_errors=len(sys_errors),
                 total_input_tokens=total_input,
                 total_output_tokens=total_output,
                 total_tokens=total_tokens)

    except Exception as e:
        store.update_run_status("FAILED")
        log.error("pipeline_failed", run_id=run_id, error=str(e)[:300])
        click.echo(f"\n❌  Pipeline FAILED: {e}")
        raise


# ─────────────────────────────────────────────────────────────────────────────
# ui command
# ─────────────────────────────────────────────────────────────────────────────

@cli.command()
def ui():
    """Launch the NiceGUI monitoring UI (port 8082, hardcoded in ui/app.py)."""
    ui_path = _ROOT / "ui" / "app.py"
    click.echo("🌐  Launching UI at http://localhost:8082")
    subprocess.run([sys.executable, str(ui_path)])


# ─────────────────────────────────────────────────────────────────────────────
# status command
# ─────────────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--run-id", required=True, help="Run ID to check")
def status(run_id):
    """Show current pipeline status for a run."""
    store = ArtifactStore(run_id=run_id)
    try:
        ck = store.read("orchestrator", "checkpoint.json")
    except FileNotFoundError:
        click.echo(f"No checkpoint found for run: {run_id}")
        sys.exit(1)

    procs = ck.get("procs", {})
    click.echo(f"\nRun: {run_id}  |  Updated: {ck.get('updated_at','')[:19]}")
    click.echo("─" * 60)

    status_icons = {
        "VALIDATED": "✅", "PARTIAL": "⚠️", "FROZEN": "🔴",
        "SKIPPED": "⏭️", "VALIDATING": "🔍", "CHUNK_CONVERTING": "⚙️",
        "PENDING": "⏳", "FAILED": "❌", "DISPATCHED": "🔄",
    }
    for proc_name, state in procs.items():
        s    = state.get("status", "?")
        icon = status_icons.get(s, "•")
        rp   = state.get("replan_count", 0)
        errs = len(state.get("error_history", []))
        click.echo(
            f"  {icon} {proc_name:<40} {s:<20}"
            + (f" replans={rp}" if rp else "")
            + (f" errors={errs}" if errs else "")
        )

    click.echo("─" * 60)
    total     = len(procs)
    validated = sum(1 for p in procs.values() if p["status"] in ("VALIDATED","PARTIAL"))
    frozen    = sum(1 for p in procs.values() if p["status"] == "FROZEN")
    click.echo(f"  Total: {total}  Validated/Partial: {validated}  Frozen: {frozen}")


# ─────────────────────────────────────────────────────────────────────────────
# resume command
# ─────────────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--run-id",       required=True, help="Run ID to resume")
@click.option("--ack-circuit",  is_flag=True,  help="Acknowledge circuit breaker and resume dispatching")
@click.option("--reason",       default="",    help="Reason for acknowledgement (logged for audit)")
def resume(run_id, ack_circuit, reason):
    """Resume a paused pipeline (e.g., after fixing adapter/schema and acknowledging circuit breaker)."""
    store = ArtifactStore(run_id=run_id)

    if ack_circuit:
        # Read the systematic errors to show what's being acknowledged
        try:
            sys_errors = store.read("orchestrator", "systematic_errors.json")
            circuit_state = sys_errors.get("circuit_state", "CLOSED")
            errors = sys_errors.get("systematic_errors", [])
        except (FileNotFoundError, KeyError):
            circuit_state = "UNKNOWN"
            errors = []

        if circuit_state != "OPEN":
            click.echo(f"Circuit breaker is {circuit_state} — no acknowledgement needed.")
            return

        click.echo(f"\n⚡ Circuit breaker is OPEN for run: {run_id}")
        click.echo(f"   {len(errors)} systematic error(s) detected:")
        for se in errors:
            click.echo(f"     [{se.get('suspected_source', '?').upper()}] "
                       f"{se.get('signature', '?')[:60]}")
            click.echo(f"     Affected: {', '.join(se.get('affected_procs', [])[:5])}")

        if not reason:
            reason = click.prompt(
                "\nReason for acknowledgement (what did you fix?)",
                default="Root cause addressed",
            )

        # Write the ack signal file for the Orchestrator to pick up
        from sql_migration.core.global_error_aggregator import GlobalErrorAggregator
        aggregator = GlobalErrorAggregator(store=store)
        # Load existing state so we can write the ack
        aggregator._circuit_state = "OPEN"
        aggregator.write_ack_signal(reason=reason, reset_procs=True)

        click.echo(f"\n✅ Circuit breaker acknowledgement written.")
        click.echo(f"   Reason: {reason}")
        click.echo(f"   The Orchestrator will resume dispatching on its next cycle.")
        click.echo(f"   Affected FROZEN procs will be reset to PENDING.")
    else:
        click.echo("Use --ack-circuit to acknowledge the circuit breaker and resume.")
        click.echo("Example: sql-migrate resume --run-id migration_20250323 --ack-circuit")


if __name__ == "__main__":
    cli()