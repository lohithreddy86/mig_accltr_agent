"""
agent.py  (Orchestrator)
========================
Pipeline entry point — owns the entire migration lifecycle.

Phase 1 (linear):
  Analysis → Dependency Gate → Planning → Pre-flight

Phase 2 (state machine):
  Dispatch chunks to Conversion Agent, validate, handle failures.

State machine per proc:
  PENDING → DISPATCHED → CHUNK_CONVERTING → CHUNK_DONE (× N chunks)
          → ALL_CHUNKS_DONE → VALIDATING
          → VALIDATED | PARTIAL | FAILED → (replan?) → FROZEN
          → SKIPPED  (if human resolves with mark_skip)
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import click

from sql_migration.core.artifact_store import ArtifactStore
from sql_migration.core.error_handling import ErrorClass, ErrorClassifier, ErrorTracker
from sql_migration.core.config_loader import get_config
from sql_migration.core.human_feedback import HumanFeedbackHandler
from sql_migration.core.logger import get_logger
from sql_migration.core.provenance import (
    ProvenanceTracker,
    build_callee_sig_with_provenance,
    compute_conversion_provenance,
)
from sql_migration.core.global_error_aggregator import GlobalErrorAggregator
from sql_migration.models import (
    AnalysisAgentInput,
    Checkpoint,
    DialectProfile,
    DispatchTask,
    FeedbackBundle,
    Plan,
    PipelineConfig,
    PlanningAgentInput,
    ProcPlan,
    ProcState,
    ReplanRequest,
    ValidationResult,
)
from sql_migration.models.common import (
    ConversionStrategy,
    ErrorEntry,
    ProcStatus,
    ResolutionAction,
    ValidationOutcome,
)

log = get_logger("orchestrator")


# ---------------------------------------------------------------------------
# Pipeline result (returned to main.py for summary output)
# ---------------------------------------------------------------------------
@dataclass
class PipelineResult:
    """Returned by OrchestratorAgent.run() with everything main.py needs."""
    checkpoint:        Checkpoint | None = None
    status:            str = "COMPLETE"    # COMPLETE / PARTIAL / DRY_RUN_COMPLETE
    systematic_errors: list = field(default_factory=list)
    token_usage:       dict = field(default_factory=dict)
    # Pre-orchestration outputs (for summary)
    total_procs:       int = 0
    total_lines:       int = 0
    total_chunks:      int = 0
    module_count:      int = 0
    source_proc_count: int = 0
    has_module_grouping: bool = False
    preflight_calls:   int = 0
    preflight_critical: int = 0


class OrchestratorAgent:
    """
    Pipeline entry point — runs Analysis through Validation.

    Usage:
        orch = OrchestratorAgent(store, sandbox, mcp, feedback,
                                 run_id, readme_path, sql_file_paths)
        result = orch.run()
    """

    def __init__(
        self,
        store:          ArtifactStore,
        sandbox:        "Sandbox",
        mcp:            "MCPClientSync",
        feedback:       HumanFeedbackHandler,
        run_id:         str,
        readme_path:    str = "",
        sql_file_paths: list[str] | None = None,
        pipeline_config: PipelineConfig | None = None,
    ) -> None:
        self.store          = store
        self.sandbox        = sandbox
        self.mcp            = mcp
        self.feedback       = feedback
        self._run_id        = run_id
        self._readme_path   = readme_path
        self._sql_file_paths = list(sql_file_paths or [])
        self._pipeline_config = pipeline_config
        self.cfg            = get_config().orchestrator
        self.plan_cfg       = get_config().planning
        self._plan:       Plan | None       = None
        self._checkpoint: Checkpoint | None = None
        self._error_trackers: dict[str, ErrorTracker] = {}
        self._aggregator: GlobalErrorAggregator | None = None
        self._provenance: ProvenanceTracker | None = None
        self._semantic_map = None
        # Sub-agents (created in run())
        self._ana_agent  = None
        self._plan_agent = None
        self._conv_agent = None
        self._val_agent  = None
        # Pre-orchestration outputs
        self._ana_output  = None
        self._plan_output = None
        self._preflight   = None
        self._is_ssis     = False

    # =========================================================================
    # Stage progress (for Streamlit UI)
    # =========================================================================

    _STAGES = ["analysis", "dependency_gate", "planning", "preflight", "orchestration"]

    def _write_stage_progress(self, current: str, status: str = "RUNNING",
                               summary: str = "", **extra):
        old_stages = {}
        try:
            existing = self.store.read("orchestrator", "stage_progress.json")
            old_stages = existing.get("stages", {})
        except Exception:
            pass

        stages = {}
        for s in self._STAGES:
            if s == current:
                stages[s] = {"status": status, "summary": summary, **extra}
                if status == "RUNNING":
                    stages[s]["started_at"] = datetime.now(timezone.utc).isoformat()
                break
            else:
                prev = old_stages.get(s, {})
                stages[s] = {"status": "DONE", "summary": prev.get("summary", "")}
        found = False
        for s in self._STAGES:
            if s == current:
                found = True
                continue
            if found and s not in stages:
                stages[s] = {"status": "PENDING"}
        try:
            self.store.write("orchestrator", "stage_progress.json", {
                "current_stage": current,
                "current_status": status,
                "stage_number": self._STAGES.index(current) + 1,
                "total_stages": len(self._STAGES),
                "stages": stages,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })
        except Exception:
            pass

    # =========================================================================
    # Sub-agent creation
    # =========================================================================

    def _create_agents(self):
        from sql_migration.agents.analysis   import AnalysisAgent
        from sql_migration.agents.planning   import PlanningAgent
        from sql_migration.agents.conversion import ConversionAgent
        from sql_migration.agents.validation import ValidationAgent

        self._ana_agent  = AnalysisAgent(store=self.store, sandbox=self.sandbox, mcp=self.mcp)
        self._plan_agent = PlanningAgent(store=self.store, sandbox=self.sandbox, mcp=self.mcp)
        self._conv_agent = ConversionAgent(store=self.store, sandbox=self.sandbox, mcp=self.mcp)
        self._val_agent  = ValidationAgent(store=self.store, sandbox=self.sandbox, mcp=self.mcp)

    def _create_support_systems(self):
        self._provenance = ProvenanceTracker(self.store)
        agg_cfg = get_config().global_error_aggregator
        self._aggregator = GlobalErrorAggregator(
            store=self.store,
            provenance_tracker=self._provenance,
            cross_proc_threshold=agg_cfg.cross_proc_threshold,
            max_failed_procs=agg_cfg.max_failed_procs,
        )

    # =========================================================================
    # Phase 1: Linear pre-orchestration stages
    # =========================================================================

    def _run_analysis(self):
        click.echo("🔬  Stage 1/4 — Analysis (setup + extraction + LLM)...")
        self._write_stage_progress("analysis", "RUNNING",
                                   "Parsing README, selecting adapter, analyzing source files...")
        ana_input = AnalysisAgentInput(
            readme_path=self._readme_path,
            sql_file_paths=self._sql_file_paths,
            run_id=self._run_id,
            pipeline_config=self._pipeline_config,
        )
        self._ana_output = self._ana_agent.run(ana_input)
        click.echo(f"     Procs found: {self._ana_output.manifest.total_procs}  "
                   f"Lines: {self._ana_output.manifest.total_lines}")
        try:
            sm_data = self.store.read("analysis", "semantic_map.json")
            n_discovered = len(sm_data.get("discovered_dependencies", []))
            if n_discovered:
                click.echo(f"     ⚠️  {n_discovered} unknown UDF(s) discovered by analysis")
        except Exception:
            pass
        self._write_stage_progress("analysis", "DONE",
                                   f"{self._ana_output.manifest.total_procs} procs, "
                                   f"{self._ana_output.manifest.total_lines} lines")

    def _run_dependency_gate(self):
        from sql_migration.core.dependency_gate import DependencyGate, DepAction

        self._write_stage_progress("dependency_gate", "RUNNING", "Checking external UDFs...")
        dep_gate = DependencyGate(self.store)
        gate_result = dep_gate.check()

        if gate_result.has_unresolved and not gate_result.all_resolved:
            dep_names = [d.dep_name for d in gate_result.deps]
            affected  = len(set(p for d in gate_result.deps for p in d.called_by))
            click.echo(f"  ⚠️  {len(dep_names)} unresolved external dep(s) found: "
                       f"{', '.join(dep_names[:5])}")
            click.echo(f"     Affects {affected} proc(s). "
                       f"Treating as TODO stubs in CLI mode.")
            dep_gate_cfg = get_config().dependency_gate
            auto_action = dep_gate_cfg.cli_auto_action
            if auto_action == "abort":
                click.echo("     Config says abort on unresolved deps. Stopping.")
                self.store.update_run_status("ABORTED_DEPS")
                return False  # Signal abort
            action = DepAction.STUB_TODO if auto_action == "stub_todo" else DepAction.SKIP_CALLERS
            resolutions = {
                d.dep_name: {"action": action}
                for d in gate_result.deps
            }
            gate_result = dep_gate.apply_resolutions(gate_result, resolutions)

            if gate_result.uploaded_files:
                click.echo(f"     Re-running Analysis with {len(gate_result.uploaded_files)} new files...")
                all_sql_files = self._sql_file_paths + gate_result.uploaded_files
                ana_input_v2 = AnalysisAgentInput(
                    readme_path=self._readme_path,
                    sql_file_paths=all_sql_files,
                    run_id=self._run_id,
                    pipeline_config=self._pipeline_config,
                )
                self._ana_output = self._ana_agent.run(ana_input_v2)
                self._sql_file_paths = all_sql_files
                click.echo(f"     Re-analysis: {self._ana_output.manifest.total_procs} procs")
        else:
            if gate_result.has_unresolved:
                click.echo("  ✅  Dependency gate: all external deps resolved from prior run.")
            else:
                click.echo("  ✅  Dependency gate: no external deps found.")
        self._write_stage_progress("dependency_gate", "DONE", "Resolved")
        return True  # Continue

    def _run_planning(self):
        click.echo("📐  Stage 2/4 — Planning...")
        self._write_stage_progress("planning", "RUNNING", "Selecting strategies + chunking...")
        plan_input = PlanningAgentInput(
            manifest_path="", complexity_report_path="",
            construct_map_path="", conversion_order_path="",
            run_id=self._run_id,
        )
        self._plan_output = self._plan_agent.run(plan_input)
        click.echo(f"     Total chunks: {self._plan_output.plan.total_chunks()}")
        if self._plan_output.module_grouping:
            mg = self._plan_output.module_grouping
            click.echo(f"     Modules: {len(mg.modules)} "
                       f"(from {self._plan_output.plan.total_source_procs()} source procs)")
            for m in mg.modules:
                click.echo(f"       {m.module_name}: {len(m.source_procs)} procs — {m.description[:60]}")
        else:
            click.echo(f"     Procs: {self._plan_output.plan.total_procs()} (no module grouping)")
        self._write_stage_progress("planning", "DONE",
                                   f"{self._plan_output.plan.total_chunks()} chunks across "
                                   f"{self._plan_output.plan.total_procs()} modules/procs")

    def _run_preflight(self) -> bool:
        """Returns False if dry-run (pipeline should stop)."""
        from sql_migration.core.preflight import PreflightAssessor

        self._write_stage_progress("preflight", "RUNNING", "Scoring risk per module...")
        assessor = PreflightAssessor(self.store)
        self._preflight = assessor.assess(self._run_id)

        if self._preflight.critical_count > 0:
            click.echo(f"  ⚠️  Pre-flight: {self._preflight.critical_count} CRITICAL-risk proc(s):")
            for r in self._preflight.proc_risks:
                if r.risk_level == "CRITICAL":
                    click.echo(f"       {r.proc_name}: {', '.join(r.risk_factors[:2])}")
        if self._preflight.recommendations:
            for rec in self._preflight.recommendations[:3]:
                click.echo(f"     {rec}")

        click.echo(f"     Estimated LLM calls: {self._preflight.estimated_total_calls} "
                   f"(worst-case with all corrections)")
        self._write_stage_progress("preflight", "DONE",
                                   f"{self._preflight.estimated_total_calls} est. LLM calls, "
                                   f"{self._preflight.critical_count} critical-risk")

        if get_config().run.dry_run:
            self.store.update_run_status("DRY_RUN_COMPLETE")
            self._write_stage_progress("preflight", "DONE", "Dry run — stopping before conversion")
            click.echo("\n🏁  Dry-run complete — preflight summary written. No LLM conversion calls made.")
            return False
        return True

    # =========================================================================
    # Replan / Reanalyze (previously in context_fixes.py)
    # =========================================================================

    def _run_replan(self, proc_name: str, notes: str) -> None:
        self._conv_agent.reset_proc_state(proc_name)
        replan_input = PlanningAgentInput(
            manifest_path="", complexity_report_path="",
            construct_map_path="", conversion_order_path="",
            run_id=self._run_id,
            replan_proc_name=proc_name,
            replan_notes=notes,
        )
        self._plan_agent.run(replan_input)

    def _run_reanalyze(self, uploaded_files: list[str]) -> Plan:
        from sql_migration.core.dependency_gate import DependencyGate, DepAction

        all_sql_files = self._sql_file_paths + uploaded_files

        for proc_name in list(self._conv_agent._chunk_codes.keys()):
            self._conv_agent.reset_proc_state(proc_name)

        ana_input = AnalysisAgentInput(
            readme_path=self._readme_path,
            sql_file_paths=all_sql_files,
            run_id=self._run_id,
            pipeline_config=self._pipeline_config,
        )
        self._ana_agent.run(ana_input)

        dep_gate = DependencyGate(self.store)
        gate_result = dep_gate.check()
        if gate_result.has_unresolved and not gate_result.all_resolved:
            resolutions = {
                d.dep_name: {"action": DepAction.STUB_TODO}
                for d in gate_result.deps
            }
            dep_gate.apply_resolutions(gate_result, resolutions)

        plan_input = PlanningAgentInput(
            manifest_path="", complexity_report_path="",
            construct_map_path="", conversion_order_path="",
            run_id=self._run_id,
        )
        plan_output = self._plan_agent.run(plan_input)
        return plan_output.plan

    # =========================================================================
    # Collect token usage from all agents
    # =========================================================================

    def _collect_token_usage(self) -> dict:
        usage = {}
        for name, agent in [
            ("analysis", self._ana_agent), ("planning", self._plan_agent),
            ("conversion", self._conv_agent), ("validation", self._val_agent),
        ]:
            if agent and hasattr(agent, "llm"):
                usage[name] = agent.llm.token_usage
        return usage

    # =========================================================================
    # Public entrypoint
    # =========================================================================

    def run(self) -> PipelineResult:
        """
        Run the full migration pipeline: Analysis → Dep Gate → Planning →
        Pre-flight → Conversion + Validation dispatch loop.

        Returns PipelineResult with checkpoint, token usage, and summary data.
        """
        log.info("pipeline_start", run_id=self._run_id)

        # ── Phase 1: Create agents ────────────────────────────────────────
        self._create_agents()

        # ── Phase 0: Parser (if SSIS package) ─────────────────────────────
        self._is_ssis = (
            self._pipeline_config is not None
            and self._pipeline_config.source_type == "ssis_package"
        )
        if self._is_ssis:
            click.echo("📦  Phase 0 — Parsing SSIS package...")
            self._write_stage_progress("parser", "RUNNING",
                                       "Parsing SSIS .dtsx/.ispac files...")
            from sql_migration.agents.parser import ParserAgent
            parser = ParserAgent(
                store=self.store,
                sandbox=self.sandbox,
                pipeline_config=self._pipeline_config,
            )
            parser_output = parser.run(
                input_file_paths=self._sql_file_paths,
            )
            # Replace sql_file_paths with extracted SQL from Parser
            self._sql_file_paths = parser_output.extracted_sql_paths
            self._write_stage_progress("parser", "DONE",
                                       f"Extracted {parser_output.total_sql_files} SQL files "
                                       f"from {parser_output.total_packages} package(s)")
            click.echo(f"     Packages: {parser_output.total_packages}  "
                       f"Tasks: {parser_output.total_tasks}  "
                       f"SQL files: {parser_output.total_sql_files}")

            if not parser_output.extracted_sql_paths:
                click.echo("⚠️  No SQL-bearing tasks found in SSIS package(s).")
                return PipelineResult(status="PARSER_EMPTY")

        # ── Phase 1a: Analysis ────────────────────────────────────────────
        self._run_analysis()

        # ── Phase 1b: Dependency Gate ─────────────────────────────────────
        if not self._run_dependency_gate():
            return PipelineResult(status="ABORTED_DEPS")

        # ── Phase 1c: Planning ────────────────────────────────────────────
        self._run_planning()

        # ── Phase 1d: Pre-flight ──────────────────────────────────────────
        if not self._run_preflight():
            return PipelineResult(
                status="DRY_RUN_COMPLETE",
                total_procs=self._ana_output.manifest.total_procs,
                total_lines=self._ana_output.manifest.total_lines,
                preflight_calls=self._preflight.estimated_total_calls,
                preflight_critical=self._preflight.critical_count,
                token_usage=self._collect_token_usage(),
            )

        # ── Phase 2: Dispatch loop (conversion + validation) ──────────────
        click.echo("🎯  Stage 3-4/4 — Orchestrating conversion + validation...")
        self._write_stage_progress("orchestration", "RUNNING",
                                   "Converting + validating — see proc table below")

        self._create_support_systems()
        self._plan       = self._load_plan()
        self._checkpoint = self._init_or_resume_checkpoint(self._run_id)
        self._semantic_map = self._load_semantic_map()

        # Load UDF implementations and external warnings from dialect_profile
        try:
            dp = self.store.read("analysis", "dialect_profile.json")
            self._udf_implementations = dp.get("udf_implementations", [])
            self._is_query_mode = dp.get("source_type") == "sql_queries"
            # Build external warnings from README Section 7 flags
            self._external_warnings: list[str] = []
            if dp.get("has_db_links"):
                self._external_warnings.append(
                    "SOURCE USES DB LINKS / REMOTE DB CALLS — no Trino equivalent. "
                    "Wrap in TODO comment."
                )
            if dp.get("has_external_calls"):
                self._external_warnings.append(
                    "SOURCE USES EXTERNAL SYSTEM CALLS — cannot be migrated to PySpark. "
                    "Wrap in TODO comment."
                )
            if dp.get("has_file_io"):
                self._external_warnings.append(
                    "SOURCE USES FILE I/O (e.g. UTL_FILE) — not available in Spark sandbox. "
                    "Wrap in TODO comment."
                )
            if dp.get("has_scheduler_calls"):
                self._external_warnings.append(
                    "SOURCE USES SCHEDULER / JOB CALLS — not available in Spark. "
                    "Wrap in TODO comment."
                )
        except Exception:
            self._udf_implementations = []
            self._external_warnings = []
            self._is_query_mode = False

        poll_interval = self.cfg.dispatch_poll_interval
        stop_on_frozen = get_config().run.stop_on_first_frozen

        while True:
            # v6: Reset per-cycle circuit breaker log flag
            self._circuit_logged_this_cycle = False
            _stop_early = False

            # ── v6: Poll for file-based circuit breaker acknowledgement ───
            # If the developer fixed the root cause and ran `sql-migrate resume`
            # or clicked "Acknowledge & Resume" in the UI, circuit_ack.json exists.
            # Apply it: transition OPEN → ACKNOWLEDGED, optionally reset affected procs.
            if self._aggregator and self._aggregator.is_circuit_open():
                if self._aggregator.check_and_apply_ack():
                    # Circuit acknowledged — reset affected FROZEN procs to PENDING
                    # so they re-enter the dispatch loop with (presumably) fixed artifacts
                    sys_errors = self._aggregator.get_systematic_errors()
                    affected = set()
                    for se in sys_errors:
                        affected.update(se.affected_procs)
                    for proc_name in affected:
                        state = self._checkpoint.procs.get(proc_name)
                        if state and state.status == ProcStatus.FROZEN:
                            state.chunks_done = []
                            state.retry_count = 0
                            state.replan_count = 0
                            state.current_chunk = None
                            if proc_name in self._error_trackers:
                                self._error_trackers[proc_name].reset()
                            self._transition(proc_name, ProcStatus.PENDING)
                            log.info("proc_reset_after_circuit_ack",
                                     proc=proc_name)
                    self._write_checkpoint()

            # ── First: poll all FROZEN procs for human resolutions ────────
            # Do this before is_complete() check so resolutions unblock pipeline
            for proc_name in list(self._plan.conversion_order):
                state = self._checkpoint.procs.get(proc_name)
                if state and state.status == ProcStatus.FROZEN:
                    self._check_and_apply_resolution(proc_name, state)

            if self._checkpoint.is_complete():
                break

            progress_made = False

            for proc_name in self._plan.conversion_order:
                state = self._checkpoint.procs.get(proc_name)
                if not state:
                    continue

                # ── Hard terminal states (no further action needed) ───────
                if state.status in {
                    ProcStatus.VALIDATED, ProcStatus.PARTIAL, ProcStatus.SKIPPED,
                }:
                    continue

                # ── FROZEN: poll for human resolution ─────────────────────
                if state.status == ProcStatus.FROZEN:
                    resolved = self._check_and_apply_resolution(proc_name, state)
                    if resolved:
                        progress_made = True
                    continue

                # ── v6: Circuit breaker check (Method 2) ──────────────────
                # If the GlobalErrorAggregator has detected a systematic
                # cross-proc error pattern, pause all pending dispatches.
                # Procs already in progress (CHUNK_CONVERTING) continue,
                # but no NEW procs are started until the developer resolves.
                if (self._aggregator
                        and self._aggregator.is_circuit_open()
                        and state.status == ProcStatus.PENDING):
                    # Only log once per loop iteration (first proc hit)
                    if not getattr(self, '_circuit_logged_this_cycle', False):
                        sys_errors = self._aggregator.get_systematic_errors()
                        log.warning(
                            "circuit_breaker_blocking_dispatch",
                            proc=proc_name,
                            systematic_errors=len(sys_errors),
                            affected_procs=[
                                se.affected_procs for se in sys_errors[:2]
                            ],
                            recommendation=(
                                sys_errors[0].recommendation[:200]
                                if sys_errors else ""
                            ),
                        )
                        self._circuit_logged_this_cycle = True
                    continue

                # ── Dependency gate ───────────────────────────────────────
                if not self._deps_clear(proc_name):
                    continue

                # ── Dispatch pending proc ─────────────────────────────────
                if state.status == ProcStatus.PENDING:
                    self._transition(proc_name, ProcStatus.DISPATCHED)
                    progress_made = True

                # ── Convert chunk by chunk ────────────────────────────────
                if state.status in {ProcStatus.DISPATCHED, ProcStatus.CHUNK_CONVERTING, ProcStatus.CHUNK_DONE}:
                    advanced = self._process_next_chunk(proc_name, state)
                    if advanced:
                        progress_made = True

                # ── All chunks done → validate ────────────────────────────
                if state.status == ProcStatus.ALL_CHUNKS_DONE:
                    self._transition(proc_name, ProcStatus.VALIDATING)
                    self._run_validation_for_proc(proc_name, state)
                    progress_made = True

                # ── Stop on first FROZEN if configured ───────────────────
                if (
                    stop_on_frozen
                    and state.status == ProcStatus.FROZEN
                ):
                    log.warning("stop_on_first_frozen", proc=proc_name)
                    self._write_checkpoint()
                    _stop_early = True
                    break

            if _stop_early:
                break

            if not progress_made:
                # No proc advanced — either waiting for deps or all done
                if self._checkpoint.is_complete():
                    break
                time.sleep(poll_interval)

        self._checkpoint.refresh_counts()
        self._write_checkpoint()

        # v6: Persist GlobalErrorAggregator summary
        if self._aggregator:
            try:
                self._aggregator._persist_systematic_errors()
            except Exception:
                pass

        log.info("orchestrator_complete",
                 validated=self._checkpoint.validated_count,
                 partial=self._checkpoint.partial_count,
                 frozen=self._checkpoint.frozen_count,
                 skipped=self._checkpoint.skipped_count,
                 systematic_errors=(
                     len(self._aggregator.get_systematic_errors())
                     if self._aggregator else 0
                 ))

        sys_errors = (self._aggregator.get_systematic_errors()
                      if self._aggregator else [])

        # Write final stage progress
        self._write_stage_progress("orchestration", "DONE",
                                   f"V:{self._checkpoint.validated_count} "
                                   f"P:{self._checkpoint.partial_count} "
                                   f"F:{self._checkpoint.frozen_count} "
                                   f"S:{self._checkpoint.skipped_count}")

        # ── Phase 3: DAG Generator (if SSIS package) ─────────────────────
        if getattr(self, "_is_ssis", False):
            click.echo("🔧  Phase 3 — DAG Generator (placeholder)...")
            self._write_stage_progress("dag_generator", "RUNNING",
                                       "Assembling Airflow DAG...")
            try:
                from sql_migration.agents.dag_generator import DagGeneratorAgent

                # Collect converted proc file paths
                converted_paths: dict[str, str] = {}
                if self._plan:
                    for proc_name in self._plan.conversion_order:
                        for ext in (".py", ".sql"):
                            p = self.store.path("conversion", f"proc_{proc_name}{ext}")
                            if p.exists():
                                converted_paths[proc_name] = str(p)
                                break

                dag_gen = DagGeneratorAgent(
                    store=self.store, sandbox=self.sandbox)
                dag_output = dag_gen.run(
                    converted_proc_paths=converted_paths,
                    scheduling_graph=self.store.read("parser", "scheduling_graph.json"),
                    connection_map=self.store.read("parser", "connection_map.json"),
                    package_manifest=self.store.read("parser", "package_manifest.json"),
                )
                self._write_stage_progress("dag_generator", "DONE", dag_output.status)
                click.echo(f"     DAG Generator: {dag_output.status}")
            except Exception as e:
                log.warning("dag_generator_failed", error=str(e)[:200])
                self._write_stage_progress("dag_generator", "FAILED", str(e)[:100])
                click.echo(f"     DAG Generator failed: {e}")

        return PipelineResult(
            checkpoint=self._checkpoint,
            status="PARTIAL" if self._checkpoint.frozen_count > 0 else "COMPLETE",
            systematic_errors=sys_errors,
            token_usage=self._collect_token_usage(),
            total_procs=self._ana_output.manifest.total_procs if self._ana_output else 0,
            total_lines=self._ana_output.manifest.total_lines if self._ana_output else 0,
            total_chunks=self._plan_output.plan.total_chunks() if self._plan_output else 0,
            module_count=(len(self._plan_output.module_grouping.modules)
                         if self._plan_output and self._plan_output.module_grouping else 0),
            source_proc_count=(self._plan_output.plan.total_source_procs()
                              if self._plan_output else 0),
            has_module_grouping=bool(self._plan_output and self._plan_output.module_grouping),
            preflight_calls=self._preflight.estimated_total_calls if self._preflight else 0,
            preflight_critical=self._preflight.critical_count if self._preflight else 0,
        )

    # =========================================================================
    # Dependency gate
    # =========================================================================

    def _deps_clear(self, proc_name: str) -> bool:
        """
        All dependencies must be VALIDATED or PARTIAL before dispatching.
        If any dependency is FROZEN → propagate freeze to this proc.

        v7: Uses ProcPlan.calls (module-level dependencies computed by P4)
        instead of the raw dependency_graph.json (proc-level edges). This
        works correctly for both module mode and legacy proc mode.
        """
        plan_entry = self._plan.get(proc_name)
        if not plan_entry:
            return True

        # ProcPlan.calls contains the correct dependencies:
        # - In module mode: other MODULE names (translated from proc calls by P4)
        # - In legacy mode: other proc names (same as before)
        deps = plan_entry.calls or []

        for dep in deps:
            dep_state = self._checkpoint.procs.get(dep)
            if not dep_state:
                continue
            if dep_state.status == ProcStatus.FROZEN:
                log.warning("freezing_proc_due_to_frozen_dep",
                            proc=proc_name, frozen_dep=dep)
                self._freeze_proc(proc_name,
                                  error=f"Dependency '{dep}' is FROZEN")
                return False
            if dep_state.status not in {
                ProcStatus.VALIDATED, ProcStatus.PARTIAL, ProcStatus.SKIPPED
            }:
                return False
        return True

    # =========================================================================
    # Chunk processing
    # =========================================================================

    def _process_next_chunk(self, proc_name: str, state: ProcState) -> bool:
        """
        Dispatch the next undone chunk for this proc.
        Returns True if a chunk was dispatched or completed.
        """
        plan_entry = self._plan.get(proc_name)
        if not plan_entry:
            return False

        for chunk in plan_entry.chunks:
            if chunk.chunk_id in state.chunks_done:
                continue  # Already done (resume after crash)

            # This is the next chunk to process
            self._transition(proc_name, ProcStatus.CHUNK_CONVERTING,
                             extra={"current_chunk": chunk.chunk_id})

            # Build callee signature map — look up each callee's converted
            # function signature so the C2 prompt can reference it.
            callee_sigs: dict[str, str] = {}
            for callee_name in (plan_entry.calls or []):
                callee_plan = self._plan.get(callee_name)
                if not callee_plan:
                    continue
                from sql_migration.models.common import ConversionStrategy as _CS
                if callee_plan.strategy in {_CS.PYSPARK_DF, _CS.PYSPARK_PIPELINE}:
                    # Try to extract just the function signature line from assembled code
                    raw_sig = self._load_callee_signature(callee_name)

                    # v6: Enrich signature with provenance confidence (Method 1)
                    # If the callee was converted with low confidence (PARTIAL,
                    # many self-corrections, unresolved deps), tag the signature
                    # with a warning so the LLM treats it cautiously.
                    if self._provenance:
                        prov = self._provenance.read_callee_provenance(callee_name)
                        enriched_sig, is_low = build_callee_sig_with_provenance(
                            callee_name, raw_sig, prov
                        )
                        if is_low:
                            log.debug("low_confidence_callee_sig",
                                      caller=proc_name, callee=callee_name,
                                      confidence=prov.confidence if prov else 0.0)
                        callee_sigs[callee_name] = enriched_sig
                    else:
                        callee_sigs[callee_name] = raw_sig
                else:
                    # TRINO_SQL callee — note it so the LLM knows to call it as SQL
                    callee_sigs[callee_name] = f"-- TRINO SQL proc: {callee_name}"

            # FIX 6: Merge live state vars from previous chunk's conversion output.
            # The plan's state_vars are regex-extracted from SOURCE SQL.
            # The conversion output may use different names (e.g. v_total → total_amount).
            # Merge both so the LLM knows about all variables.
            merged_state_vars = dict(chunk.state_vars)  # start with plan vars
            if state.chunks_done:
                last_done_id = state.chunks_done[-1]
                prev_result_path = self.store.path(
                    "conversion", f"result_{proc_name}_{last_done_id}.json"
                )
                if prev_result_path.exists():
                    try:
                        prev_data = json.loads(prev_result_path.read_text())
                        live_vars = prev_data.get("updated_state_vars", {})
                        merged_state_vars.update(live_vars)
                    except Exception:
                        pass

            task = DispatchTask(
                proc_name=proc_name,
                chunk_id=chunk.chunk_id,
                chunk_index=plan_entry.chunks.index(chunk),
                total_chunks=len(plan_entry.chunks),
                source_file=plan_entry.source_file,
                start_line=chunk.start_line,
                end_line=chunk.end_line,
                strategy=plan_entry.strategy,
                schema_context=chunk.schema_context,
                target_context=getattr(chunk, "target_context", {}) or {},
                construct_hints=chunk.construct_hints,
                state_vars=merged_state_vars,
                loop_guards=plan_entry.loop_guards,
                retry_number=state.retry_count,
                replan_notes=plan_entry.strategy_rationale
                             if "Replan notes:" in plan_entry.strategy_rationale else "",
                # Unknown UDFs — forwarded so C3 can detect raw calls to them
                unresolved_deps=plan_entry.unresolved_deps,
                # v6-fix: Enriched context per dep from Dependency Gate resolution
                dep_resolution_context=plan_entry.dep_resolution_context or {},
                # Callee function signatures for cross-proc call translation
                callee_signatures=callee_sigs,
                # Pre-written UDF implementations from README Section 3.2
                udf_implementations=self._udf_implementations,
                # v9: External dependency warnings from README Section 7
                external_warnings=self._external_warnings,
                # v5: Write directives for shared-write tables (from WriteSemanticsAnalyzer)
                write_directives=plan_entry.write_directives or {},
                # v7: Module grouping fields — source proc boundaries for C1 extraction
                source_procs=plan_entry.source_procs or [],
                source_ranges=[sr.model_dump() for sr in plan_entry.source_ranges]
                              if plan_entry.source_ranges else [],
                module_description=plan_entry.module_description or "",
                # v8: Semantic context from A1.5 Pipeline Semantic Map + plan enrichment
                semantic_context=self._build_semantic_context(proc_name, plan_entry),
            )

            # Write task file (decoupled agents read this)
            self.store.write(
                "orchestrator",
                f"task_{proc_name}_{chunk.chunk_id}.json",
                task.model_dump(),
            )

            # In integrated mode: call conversion directly
            chunk_result = self._dispatch_conversion(task)
            return self._handle_chunk_result(proc_name, chunk, chunk_result,
                                             state, plan_entry)

        # All chunks done
        if len(state.chunks_done) == len(plan_entry.chunks):
            self._transition(proc_name, ProcStatus.ALL_CHUNKS_DONE)
            return True

        return False

    def _dispatch_conversion(self, task: DispatchTask):
        """Call the Conversion Agent directly."""
        return self._conv_agent.run_chunk(task)

    def _handle_chunk_result(
        self,
        proc_name:   str,
        chunk,
        result,
        state:       ProcState,
        plan_entry:  ProcPlan,
    ) -> bool:
        """Route chunk result: success → next chunk, fail → retry or replan."""
        guards = plan_entry.loop_guards

        if result.status == "SUCCESS":
            state.chunks_done.append(chunk.chunk_id)
            state.retry_count = 0  # Reset retry counter on success
            self._transition(proc_name, ProcStatus.CHUNK_DONE,
                             extra={"current_chunk": None})

            # Write result to conversion store
            self.store.write(
                "conversion",
                f"result_{proc_name}_{chunk.chunk_id}.json",
                result.model_dump(),
            )

            log.info("chunk_done",
                     proc=proc_name, chunk=chunk.chunk_id,
                     todos=result.todo_count)
            return True

        # FAIL path
        combined_error = "; ".join(result.errors[:3])

        # Track error signatures for repeated-root-cause detection
        if proc_name not in self._error_trackers:
            self._error_trackers[proc_name] = ErrorTracker()
        tracker = self._error_trackers[proc_name]
        occurrence = tracker.record(combined_error)

        # Classify: UNSOLVABLE errors skip retries entirely
        error_class = ErrorClassifier.classify(combined_error)
        is_unsolvable = (error_class == ErrorClass.UNSOLVABLE)

        # v6: Feed to GlobalErrorAggregator for cross-proc pattern detection
        # This is the key integration point for Method 2 (circuit breaker).
        # The aggregator watches ACROSS procs — if the same error signature
        # appears in 3+ different procs, it trips the circuit breaker.
        systematic_error = None
        if self._aggregator:
            systematic_error = self._aggregator.record_proc_error(
                proc_name=proc_name,
                error_msg=combined_error,
                error_type="CHUNK_FAIL",
            )
            if systematic_error:
                log.warning(
                    "systematic_error_detected_in_chunk",
                    proc=proc_name,
                    signature=systematic_error.signature[:80],
                    affected_procs=systematic_error.affected_procs,
                    suspected_source=systematic_error.suspected_source,
                )

        state.add_error(
            error_type="CHUNK_FAIL",
            message=combined_error,
            chunk_id=chunk.chunk_id,
            attempt=state.retry_count + 1,
            # Attach signature and occurrence to error entry for Streamlit display
            signature=ErrorClassifier.get_signature(combined_error)[:80],
            occurrence=occurrence,
            error_class=error_class.value,
        )

        # Query mode: limited retry, no replan. Mark as PARTIAL after max retries.
        if getattr(self, '_is_query_mode', False):
            max_query_retries = get_config().conversion.query_mode_max_retries
            if state.retry_count < max_query_retries:
                state.retry_count += 1
                log.warning("query_retry",
                            query=proc_name, attempt=state.retry_count,
                            max=max_query_retries, errors=result.errors[:2])
                self._write_checkpoint()
                return True  # Will retry
            log.warning("query_conversion_failed",
                        query=proc_name, attempts=state.retry_count + 1,
                        errors=result.errors[:2])
            self._transition(proc_name, ProcStatus.PARTIAL,
                             extra={"error": combined_error[:200]})
            self._write_checkpoint()
            return True

        if not is_unsolvable and state.retry_count < guards.max_chunk_retry:
            state.retry_count += 1
            log.warning("chunk_retry",
                        proc=proc_name, chunk=chunk.chunk_id,
                        attempt=state.retry_count,
                        error_class=error_class.value,
                        occurrence=occurrence,
                        dominant_sig=tracker.dominant_signature,
                        errors=result.errors[:2])
            self._write_checkpoint()
            return True  # Will retry on next loop iteration

        # Chunk retries exhausted (or UNSOLVABLE) → trigger proc-level replan
        log.warning("chunk_retries_exhausted",
                    proc=proc_name, chunk=chunk.chunk_id,
                    error_class=error_class.value,
                    dominant_sig=tracker.dominant_signature,
                    dominant_count=tracker.dominant_count)
        state.retry_count = 0
        self._trigger_replan_or_freeze(proc_name, state, plan_entry,
                                       failure_type="CHUNK_FAIL",
                                       failed_code=result.converted_code,
                                       errors=result.errors,
                                       error_tracker_summary=tracker.summary())
        return True

    # =========================================================================
    # Validation
    # =========================================================================

    def _run_validation_for_proc(self, proc_name: str, state: ProcState) -> None:
        """Run validation for a proc whose all chunks are done."""
        converted_path = str(self.store.path(
            "conversion", f"proc_{proc_name}.py"
        ))
        if not Path(converted_path).exists():
            converted_path = str(self.store.path(
                "conversion", f"proc_{proc_name}.sql"
            ))

        if self._val_agent:
            val_result = self._val_agent.run(proc_name, converted_path)
        else:
            # File-based: poll for validation result
            val_result = self._poll_validation_result(proc_name)

        if not val_result:
            log.error("validation_result_missing", proc=proc_name)
            state.add_error("VALIDATION_MISSING",
                            "Validation result file not found")
            self._trigger_replan_or_freeze(proc_name, state,
                                           self._plan.get(proc_name),
                                           failure_type="VALIDATION_MISSING",
                                           failed_code="", errors=[])
            return

        # Write validation result to store
        self.store.write(
            "validation",
            f"validation_{proc_name}.json",
            val_result.model_dump(),
        )

        state.validation_outcome = val_result.outcome

        if val_result.outcome == ValidationOutcome.PASS:
            self._transition(proc_name, ProcStatus.VALIDATED)
            log.info("proc_validated", proc=proc_name)
            # v6: Write provenance for validated code (Method 1)
            self._write_proc_provenance(proc_name, val_result, state)

        elif val_result.outcome == ValidationOutcome.PARTIAL:
            self._transition(proc_name, ProcStatus.PARTIAL)
            log.info("proc_partial",
                     proc=proc_name,
                     warnings=val_result.warnings[:3],
                     has_manual_items=val_result.has_manual_items,
                     manual_item_count=len(val_result.manual_review_items))
            # v6: Write provenance for partial code (Method 1)
            self._write_proc_provenance(proc_name, val_result, state)

        elif val_result.outcome == ValidationOutcome.FAIL:
            log.warning("proc_validation_failed",
                        proc=proc_name,
                        fail_reason=val_result.fail_reason,
                        fail_columns=val_result.fail_columns)

            # v6: Feed validation failures to GlobalErrorAggregator
            if self._aggregator:
                self._aggregator.record_proc_error(
                    proc_name=proc_name,
                    error_msg=val_result.fail_reason,
                    error_type="VALIDATION_FAIL",
                )

            state.add_error("VALIDATION_FAIL",
                            val_result.fail_reason,
                            attempt=state.replan_count + 1)
            plan_entry = self._plan.get(proc_name)
            failed_code = self._load_converted_code(proc_name)
            self._trigger_replan_or_freeze(
                proc_name, state, plan_entry,
                failure_type="VALIDATION_FAIL",
                failed_code=failed_code,
                errors=[val_result.fail_reason],
                validation_report=val_result.model_dump(),
                manual_review_items=[t.model_dump() for t in
                                     val_result.manual_review_items],
            )

    def _poll_validation_result(self, proc_name: str) -> ValidationResult | None:
        """Poll for a validation result file (file-based mode)."""
        result_path = self.store.path("validation", f"validation_{proc_name}.json")
        timeout  = 600
        elapsed  = 0
        interval = self.cfg.dispatch_poll_interval

        while elapsed < timeout:
            if result_path.exists():
                data = json.loads(result_path.read_text())
                return ValidationResult(**data)
            time.sleep(interval)
            elapsed += interval
        return None

    # =========================================================================
    # Loop guards: replan or freeze
    # =========================================================================

    def _trigger_replan_or_freeze(
        self,
        proc_name:            str,
        state:                ProcState,
        plan_entry:           ProcPlan | None,
        failure_type:         str,
        failed_code:          str,
        errors:               list[str],
        validation_report:    dict | None = None,
        manual_review_items:  list[dict] | None = None,
        error_tracker_summary: dict | None = None,
    ) -> None:
        """
        Check loop guards and decide: replan (targeted) or freeze.
        This is the ONLY place where the FROZEN status is set.
        """
        guards = plan_entry.loop_guards if plan_entry else get_config().planning.loop_guards

        if state.replan_count < guards.frozen_after:
            state.replan_count += 1
            self._transition(proc_name, ProcStatus.FAILED)

            log.info("triggering_replan",
                     proc=proc_name,
                     replan_count=state.replan_count,
                     max=guards.frozen_after)

            # Write replan request for Planning Agent
            replan_req = ReplanRequest(
                proc_name=proc_name,
                replan_count=state.replan_count,
                failure_context={
                    "failure_type":          failure_type,
                    "errors":                errors,
                    "error_tracker_summary": error_tracker_summary or {},
                },
                failed_code=failed_code,
                validation_report=validation_report,
            )
            self.store.write(
                "orchestrator",
                f"replan_request_{proc_name}.json",
                replan_req.model_dump(),
            )

            # Trigger Planning Agent replan
            self._run_replan(proc_name, "")

            # Reset proc state and error tracker for re-processing after replan
            state.chunks_done   = []
            state.retry_count   = 0
            state.current_chunk = None
            if proc_name in self._error_trackers:
                self._error_trackers[proc_name].reset()
            self._transition(proc_name, ProcStatus.PENDING)

        else:
            self._freeze_proc(
                proc_name=proc_name,
                error=f"{failure_type}: {'; '.join(errors[:2])}",
                failed_code=failed_code,
                validation_report=validation_report,
                manual_review_items=manual_review_items or [],
            )

    def _freeze_proc(
        self,
        proc_name:           str,
        error:               str,
        failed_code:         str = "",
        validation_report:   dict | None = None,
        manual_review_items: list[dict] | None = None,
    ) -> None:
        """Mark proc FROZEN and submit for human review."""
        state = self._checkpoint.procs[proc_name]
        state.add_error("FROZEN", error)
        self._transition(proc_name, ProcStatus.FROZEN)

        # v6: Feed FROZEN event to GlobalErrorAggregator
        if self._aggregator:
            self._aggregator.record_proc_terminal(proc_name, "FROZEN")
            self._aggregator.record_proc_error(
                proc_name=proc_name,
                error_msg=error,
                error_type="FROZEN",
            )

        tracker = self._error_trackers.get(proc_name)
        log.warning("proc_frozen",
                    proc=proc_name,
                    replan_count=state.replan_count,
                    error=error,
                    dominant_error_sig=tracker.dominant_signature if tracker else None,
                    dominant_error_count=tracker.dominant_count if tracker else 0)

        # Build feedback bundle for Streamlit UI
        manifest_data    = self.store.read("analysis", "manifest.json")
        complexity_data  = self.store.read("analysis", "complexity_report.json")

        # v7: In module mode, proc_name is a module name — not in manifest.
        # Resolve to source procs from the plan entry and collect their info.
        plan_entry = self._plan.get(proc_name) if self._plan else None
        source_proc_names = (
            plan_entry.effective_source_procs if plan_entry else [proc_name]
        )

        # Collect manifest entries for all source procs in this module
        manifest_entries = [
            p for p in manifest_data.get("procs", [])
            if p.get("name") in set(source_proc_names)
        ]
        # Use first entry as primary (for backward compat), include all as list
        manifest_entry = manifest_entries[0] if manifest_entries else {}
        if len(manifest_entries) > 1:
            manifest_entry["_module_source_procs"] = [
                {"name": e.get("name"), "line_count": e.get("line_count", 0)}
                for e in manifest_entries
            ]

        # Collect complexity entries for all source procs
        complexity_entries = [
            s for s in complexity_data.get("scores", [])
            if s.get("name") in set(source_proc_names)
        ]
        complexity_entry = complexity_entries[0] if complexity_entries else {}
        if len(complexity_entries) > 1:
            # Use highest complexity score among members
            score_order = {"NEEDS_MANUAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}
            complexity_entry = max(
                complexity_entries,
                key=lambda e: score_order.get(e.get("score", "LOW"), 0),
            )

        # Collect all chunk codes for this proc
        chunk_codes: dict[str, str] = {}
        plan_entry = self._plan.get(proc_name)
        if plan_entry:
            for chunk in plan_entry.chunks:
                result_path = self.store.path(
                    "conversion", f"result_{proc_name}_{chunk.chunk_id}.json"
                )
                if result_path.exists():
                    try:
                        r = json.loads(result_path.read_text())
                        chunk_codes[chunk.chunk_id] = r.get("converted_code", "")
                    except Exception:
                        pass

        val_reports: dict[str, dict] = {}
        if validation_report:
            val_reports["final"] = validation_report

        from sql_migration.models.common import TodoItem
        todo_items = [TodoItem(**t) if isinstance(t, dict) else t
                      for t in (manual_review_items or [])]

        self.feedback.submit_for_review(
            proc_name=proc_name,
            manifest_entry=manifest_entry,
            complexity_entry=complexity_entry,
            chunk_codes=chunk_codes,
            validation_reports=val_reports,
            error_history=[e.model_dump() for e in state.error_history],
            manual_review_items=[t.model_dump() for t in todo_items],
            replan_count=state.replan_count,
            retry_count=state.retry_count,
        )

    # =========================================================================
    # Human feedback resolution
    # =========================================================================

    def _check_and_apply_resolution(
        self, proc_name: str, state: ProcState
    ) -> bool:
        """
        Check if a human has submitted a resolution for a FROZEN proc.
        Apply the resolution and resume the pipeline accordingly.
        Returns True if a resolution was found and applied.
        """
        resolution = self.feedback.poll_for_resolution(proc_name)
        if not resolution:
            return False

        action = resolution.get("action", "")
        notes  = resolution.get("notes", "")
        code   = resolution.get("corrected_code", "")

        log.info("applying_feedback_resolution",
                 proc=proc_name, action=action)

        if action == ResolutionAction.MARK_SKIP.value:
            self._transition(proc_name, ProcStatus.SKIPPED)
            log.info("proc_skipped_by_human", proc=proc_name)

        elif action == ResolutionAction.OVERRIDE_PASS.value:
            state.validation_outcome = ValidationOutcome.PARTIAL
            self._transition(proc_name, ProcStatus.PARTIAL)
            log.info("proc_override_passed_by_human", proc=proc_name)

        elif action == ResolutionAction.MANUAL_REWRITE_PROVIDED.value:
            if code.strip():
                # Write human-provided code directly to conversion store
                ext = ".py" if "def " in code or "spark" in code.lower() else ".sql"
                self.store.write_code(
                    "conversion",
                    f"proc_{proc_name}{ext}",
                    code,
                )
                # Reset for validation
                state.chunks_done   = list(
                    c.chunk_id for c in (self._plan.get(proc_name) or
                                         type("", (), {"chunks": []})()).chunks
                )
                state.replan_count  = 0
                state.retry_count   = 0
                self._transition(proc_name, ProcStatus.ALL_CHUNKS_DONE)
                log.info("proc_manual_rewrite_accepted", proc=proc_name)
            else:
                log.warning("manual_rewrite_empty", proc=proc_name)
                return False

        elif action == ResolutionAction.REPLAN_WITH_NOTES.value:
            # Reset proc and trigger a fresh replan with human notes
            state.chunks_done   = []
            state.retry_count   = 0
            state.replan_count  = 0  # Reset so full replan cycle available
            state.current_chunk = None

            self._run_replan(proc_name, notes)

            self._transition(proc_name, ProcStatus.PENDING)
            log.info("proc_replan_with_notes",
                     proc=proc_name, notes_len=len(notes))

        elif action == ResolutionAction.UPLOAD_SOURCE_AND_REANALYZE.value:
            # v6-fix (Fix 5): User uploaded missing source files (e.g. UDF).
            # Re-run Analysis with the new files → re-run Dependency Gate →
            # re-run Planning → reset ALL affected procs to PENDING.
            #
            # This handles the "second chance" case where the user missed
            # the pre-conversion Dependency Gate and the missing UDF caused
            # multiple procs to freeze.
            uploaded_files = resolution.get("uploaded_file_paths", [])
            if not uploaded_files:
                log.warning("upload_reanalyze_no_files", proc=proc_name)
                return False

            affected_procs = self._handle_upload_and_reanalyze(
                proc_name, uploaded_files, notes
            )
            log.info("upload_source_and_reanalyze_triggered",
                     trigger_proc=proc_name,
                     uploaded_files=len(uploaded_files),
                     affected_procs=sorted(affected_procs))

        else:
            log.error("unknown_resolution_action",
                      proc=proc_name, action=action)
            return False

        # ── Consume (archive) the resolution file ────────────────────────
        # Without this, if the proc re-freezes after a subsequent conversion
        # attempt, the Orchestrator would find and re-apply the STALE old
        # resolution without waiting for fresh human review. Particularly
        # dangerous for override_pass (auto-accepts) and
        # upload_source_and_reanalyze (triggers full reanalysis).
        self.store.consume_feedback_resolution(proc_name)

        return True

    # =========================================================================
    # v6-fix: Upload source and re-analyze (Fix 5)
    # =========================================================================

    def _handle_upload_and_reanalyze(
        self,
        trigger_proc: str,
        uploaded_files: list[str],
        notes: str = "",
    ) -> set[str]:
        """
        Handle UPLOAD_SOURCE_AND_REANALYZE resolution.

        This is the "second chance" path: the user missed the pre-conversion
        Dependency Gate (or was in CLI mode where deps auto-resolve as TODO),
        and multiple procs froze because of a missing UDF. Now the user
        uploads the missing file(s) and we re-run the affected pipeline stages.

        Flow:
          1. Read current dependency_resolution.json to find which deps were
             unresolved (and hence which procs were affected)
          2. Write uploaded file paths into a reanalyze_request.json artifact
             that main.py / the Streamlit launcher can pick up
          3. Reset ALL procs that had unresolved_deps matching the uploaded
             content back to PENDING (not just the trigger proc)
          4. If run_reanalyze callable is available (integrated mode), invoke it
          5. Otherwise, write the request file for the external runner

        Returns:
            Set of proc names that were reset to PENDING.
        """
        affected_procs: set[str] = set()

        # Find all procs with unresolved deps — they all need re-processing
        # after the new source files are analyzed
        for proc_name in self._plan.conversion_order:
            plan_entry = self._plan.get(proc_name)
            if not plan_entry:
                continue
            if plan_entry.unresolved_deps:
                affected_procs.add(proc_name)

        # Also include the trigger proc itself
        affected_procs.add(trigger_proc)

        # Write reanalyze request artifact for the pipeline runner
        # In integrated mode, this is consumed by main.py's re-analysis path.
        # In Streamlit mode, the pipeline monitor detects this and re-launches.
        reanalyze_request = {
            "trigger_proc":     trigger_proc,
            "uploaded_files":   uploaded_files,
            "affected_procs":   sorted(affected_procs),
            "notes":            notes,
            "requested_at":     datetime.now(timezone.utc).isoformat(),
            "action":           "reanalyze",
            # Stages to re-run: Analysis (with new files) → Dep Gate → Planning
            # Orchestrator then resumes from updated plan.json
            "stages":           ["analysis", "dependency_gate", "planning"],
        }
        self.store.write(
            "orchestrator",
            "reanalyze_request.json",
            reanalyze_request,
        )

        # ── Execute reanalysis ─────────────────────────────────────────────
        # Re-runs Analysis (new files) → Dependency Gate → Planning (updated plan).
        # After this, self._plan has the new strategy/chunks for affected procs.
        try:
            log.info("reanalyze_executing",
                     uploaded_files=len(uploaded_files),
                     trigger_proc=trigger_proc)
            updated_plan = self._run_reanalyze(uploaded_files)
            self._plan = updated_plan

            # Add any NEW procs that appeared in the reanalysis
            # (the uploaded UDF file may contain additional procs)
            for proc_name in updated_plan.conversion_order:
                if proc_name not in self._checkpoint.procs:
                    self._checkpoint.procs[proc_name] = ProcState(
                        proc_name=proc_name
                    )
                    affected_procs.add(proc_name)
                    log.info("new_proc_from_reanalysis", proc=proc_name)

            log.info("reanalyze_complete",
                     total_procs=updated_plan.total_procs(),
                     new_procs=[p for p in updated_plan.conversion_order
                                if p not in self._checkpoint.procs
                                or self._checkpoint.procs[p].status == ProcStatus.PENDING])
        except Exception as e:
            log.error("reanalyze_failed", error=str(e)[:300],
                      msg="Procs will be reset to PENDING but plan may be stale. "
                          "Re-run the pipeline manually if needed.")

        # Reset all affected procs to PENDING
        for proc_name in affected_procs:
            state = self._checkpoint.procs.get(proc_name)
            if not state:
                continue

            state.chunks_done   = []
            state.retry_count   = 0
            state.replan_count  = 0
            state.current_chunk = None
            state.error_history = []

            self._transition(proc_name, ProcStatus.PENDING)

            log.info("proc_reset_for_reanalysis",
                     proc=proc_name,
                     trigger=trigger_proc)

        # Clear per-proc error trackers for all affected procs
        for proc_name in affected_procs:
            if proc_name in self._error_trackers:
                self._error_trackers[proc_name].reset()

        # v6: Reset circuit breaker if it was tripped by the missing UDF
        if self._aggregator and self._aggregator.is_circuit_open():
            self._aggregator.acknowledge("reanalyze_triggered")
            log.info("circuit_breaker_reset_for_reanalysis",
                     trigger=trigger_proc)

        self._write_checkpoint()

        return affected_procs

    # =========================================================================
    # State machine helpers
    # =========================================================================

    def _transition(
        self,
        proc_name: str,
        new_status: ProcStatus,
        extra: dict | None = None,
    ) -> None:
        """
        Apply a state transition and checkpoint immediately.
        This is the only place ProcState.status is set.
        """
        state = self._checkpoint.procs[proc_name]
        old   = state.status

        state.status = new_status

        if extra:
            for k, v in extra.items():
                setattr(state, k, v)

        now = datetime.now(timezone.utc).isoformat()
        if new_status in {ProcStatus.DISPATCHED} and not state.started_at:
            state.started_at = now
        if new_status in {
            ProcStatus.VALIDATED, ProcStatus.PARTIAL,
            ProcStatus.FROZEN, ProcStatus.SKIPPED,
        }:
            state.completed_at = now

        self._checkpoint.updated_at = now

        log.debug("state_transition",
                  proc=proc_name,
                  from_status=old.value,
                  to_status=new_status.value)

        if self.cfg.checkpoint_on_every_state_change:
            self._write_checkpoint()

    # =========================================================================
    # Checkpoint I/O
    # =========================================================================

    def _init_or_resume_checkpoint(self, run_id: str) -> Checkpoint:
        """Load existing checkpoint (crash recovery) or init fresh."""
        now = datetime.now(timezone.utc).isoformat()

        if self.store.exists("orchestrator", "checkpoint.json"):
            try:
                data = self.store.read("orchestrator", "checkpoint.json")
                ck   = Checkpoint(**data)
                log.info("checkpoint_resumed",
                         run_id=run_id,
                         procs=len(ck.procs))
                return ck
            except Exception as e:
                log.warning("checkpoint_load_failed",
                            error=str(e),
                            msg="Starting fresh")

        # Fresh start — init all procs from plan
        ck = Checkpoint(
            run_id=run_id,
            started_at=now,
            updated_at=now,
        )
        procs_filter = set(get_config().run.procs_filter)

        for proc_name in self._plan.conversion_order:
            if procs_filter and proc_name not in procs_filter:
                continue
            ck.procs[proc_name] = ProcState(proc_name=proc_name)

        ck.refresh_counts()
        self._write_checkpoint_obj(ck)
        log.info("checkpoint_initialized",
                 run_id=run_id,
                 total_procs=len(ck.procs))
        return ck

    def _write_checkpoint(self) -> None:
        if self._checkpoint:
            self._checkpoint.refresh_counts()
            self._write_checkpoint_obj(self._checkpoint)
            # Also write the richer status snapshot for the Streamlit UI.
            # This includes has_feedback, is_resolved, and frozen_procs
            # that checkpoint.json doesn't have.
            try:
                snapshot = self.get_status_snapshot()
                self.store.write("orchestrator", "pipeline_status.json", snapshot)
            except Exception:
                pass  # Status snapshot is advisory — never block the pipeline

    def _write_checkpoint_obj(self, ck: Checkpoint) -> None:
        self.store.write("orchestrator", "checkpoint.json", ck.model_dump())

    # =========================================================================
    # Artifact loaders
    # =========================================================================

    def _load_callee_signature(self, callee_name: str) -> str:
        """
        Load the callee's interface for injection into a caller's C2 prompt.

        Prefers the structured CalleeInterface (written by C5 assembly)
        which captures params, return type, tables read/written, and strategy.
        Falls back to raw 4-line def extraction if CalleeInterface is not
        available (e.g., older runs, assembly failed before writing interface).

        Returns a formatted string suitable for prompt injection.
        """
        # Prefer structured CalleeInterface (v6-fix Gap 4)
        try:
            if self.store.exists("conversion", f"callee_interface_{callee_name}.json"):
                data = self.store.read("conversion", f"callee_interface_{callee_name}.json")
                from sql_migration.models.pipeline import CalleeInterface
                interface = CalleeInterface(**data)
                return interface.to_prompt_block()
        except Exception:
            pass  # Fall through to raw extraction

        # Fallback: extract first 4 lines of the def statement
        for ext in (".py", ".sql"):
            if self.store.exists("conversion", f"proc_{callee_name}{ext}"):
                try:
                    code = self.store.read_code("conversion", f"proc_{callee_name}{ext}")
                    lines = code.splitlines()
                    sig_lines = []
                    in_fn = False
                    for line in lines:
                        if line.strip().startswith("def proc_"):
                            in_fn = True
                        if in_fn:
                            sig_lines.append(line)
                            if len(sig_lines) >= 4:
                                break
                    if sig_lines:
                        return "\n".join(sig_lines)
                    return f"def proc_{callee_name}(spark: SparkSession) -> None: ..."
                except Exception:
                    pass
        # File not written yet (shouldn't happen — dep gate ensures callee is done)
        import re as _re
        fn_name = f"proc_{_re.sub(r'[^a-zA-Z0-9_]', '_', callee_name)}"
        return f"def {fn_name}(spark: SparkSession) -> None: ..."

    def _load_plan(self) -> Plan:
        data = self.store.read("planning", "plan.json")
        return Plan(**data)

    def _load_semantic_map(self):
        """Load A1.5 Pipeline Semantic Map (graceful — returns None if unavailable)."""
        try:
            if self.store.exists("analysis", "semantic_map.json"):
                data = self.store.read("analysis", "semantic_map.json")
                from sql_migration.models.semantic import PipelineSemanticMap
                sm = PipelineSemanticMap(**data)
                if sm.procs_analyzed > 0:
                    return sm
        except Exception:
            pass
        return None

    def _get_semantic_context(self, proc_name: str) -> str:
        """Get per-proc semantic context string for injection into DispatchTask."""
        if not self._semantic_map:
            return ""
        # For modules, try the module name first, then individual source procs
        ctx = self._semantic_map.context_for_conversion(proc_name)
        if ctx:
            return ctx
        # If module mode, try source procs within the module
        plan_entry = self._plan.get(proc_name) if self._plan else None
        if plan_entry and plan_entry.source_procs:
            parts = []
            for sp in plan_entry.effective_source_procs:
                sp_ctx = self._semantic_map.context_for_conversion(sp)
                if sp_ctx:
                    parts.append(sp_ctx)
            if parts:
                return "\n\n".join(parts)
        return ""

    def _build_semantic_context(self, proc_name: str, plan_entry) -> str:
        """
        Merge semantic context from two sources:
        1. Pipeline Semantic Map (from Analysis A4 — business purpose, stage)
        2. ProcPlan.semantic_context (from Planning — SSIS transformation chain, etc.)
        """
        map_ctx = self._get_semantic_context(proc_name)
        plan_ctx = plan_entry.semantic_context if plan_entry else ""
        if map_ctx and plan_ctx:
            return map_ctx + "\n\n" + plan_ctx
        return map_ctx or plan_ctx

    # =========================================================================
    # v6: Provenance writing (Method 1)
    # =========================================================================

    def _write_proc_provenance(
        self,
        proc_name: str,
        val_result,
        state: ProcState,
    ) -> None:
        """
        Compute and persist provenance for a proc that reached a terminal
        validated state (VALIDATED or PARTIAL).

        This provenance is read by _process_next_chunk when building callee
        signatures for caller procs — low-confidence callees get tagged.

        Gracefully does nothing if ProvenanceTracker is not configured.
        """
        if not self._provenance:
            return

        try:
            plan_entry = self._plan.get(proc_name)

            # Read conversion log for self-correction count and todo count
            total_self_corrections = 0
            todo_count = 0
            try:
                conv_log = self.store.read(
                    "conversion", f"conversion_log_{proc_name}.json"
                )
                total_self_corrections = conv_log.get("total_self_corrections", 0)
                todo_count = len(conv_log.get("todos", []))
            except (FileNotFoundError, KeyError):
                pass

            # Check for write conflicts
            has_write_conflict = False
            if plan_entry and plan_entry.tables_written:
                try:
                    wc_data = self.store.read("planning", "write_conflicts.json")
                    conflict_tables = set(wc_data.keys()) if isinstance(wc_data, dict) else set()
                    has_write_conflict = bool(
                        set(plan_entry.tables_written or []) & conflict_tables
                    )
                except (FileNotFoundError, KeyError):
                    pass

            prov = compute_conversion_provenance(
                proc_name=proc_name,
                run_id=state.run_id if hasattr(state, 'run_id') else "",
                validation_outcome=val_result.outcome,
                validation_level=val_result.validation_level,
                total_self_corrections=total_self_corrections,
                todo_count=todo_count,
                has_unresolved_deps=bool(plan_entry and plan_entry.unresolved_deps),
                has_write_conflict=has_write_conflict,
                replan_count=state.replan_count,
            )

            self._provenance.write(prov)

            log.debug("provenance_written",
                      proc=proc_name,
                      confidence=prov.confidence,
                      tags=prov.lineage_tags)
        except Exception as e:
            # Provenance is advisory — never block the pipeline
            log.debug("provenance_write_failed",
                      proc=proc_name, error=str(e)[:100])

    def _load_converted_code(self, proc_name: str) -> str:
        """Load assembled converted code for a proc (if it exists)."""
        for ext in (".py", ".sql"):
            path = self.store.path("conversion", f"proc_{proc_name}{ext}")
            if path.exists():
                return path.read_text(encoding="utf-8")
        return ""

    # =========================================================================
    # Pipeline status (for Streamlit UI)
    # =========================================================================

    def get_status_snapshot(self) -> dict:
        """
        Return a serializable snapshot of pipeline status for the UI.
        Called by Streamlit on each poll cycle.
        """
        if not self._checkpoint:
            try:
                data = self.store.read("orchestrator", "checkpoint.json")
                self._checkpoint = Checkpoint(**data)
            except Exception:
                return {"error": "No checkpoint found"}

        ck = self._checkpoint
        ck.refresh_counts()

        return {
            "run_id":         ck.run_id,
            "started_at":     ck.started_at,
            "updated_at":     ck.updated_at,
            "total_procs":    ck.total_procs,
            "validated":      ck.validated_count,
            "partial":        ck.partial_count,
            "frozen":         ck.frozen_count,
            "skipped":        ck.skipped_count,
            "pending":        len(ck.pending_procs()),
            "in_progress":    sum(1 for p in ck.procs.values()
                                  if p.status in {
                                      ProcStatus.DISPATCHED,
                                      ProcStatus.CHUNK_CONVERTING,
                                      ProcStatus.CHUNK_DONE,
                                      ProcStatus.ALL_CHUNKS_DONE,
                                      ProcStatus.VALIDATING,
                                  }),
            "is_complete":    ck.is_complete(),
            "procs": [
                {
                    "name":          name,
                    "status":        state.status.value,
                    "chunks_done":   len(state.chunks_done),
                    "total_chunks":  len(self._plan.get(name).chunks)
                                     if self._plan and self._plan.get(name) else 0,
                    "replan_count":  state.replan_count,
                    "retry_count":   state.retry_count,
                    "error_count":   len(state.error_history),
                    "current_chunk": state.current_chunk,
                    "started_at":    state.started_at,
                    "completed_at":  state.completed_at,
                    "validation":    state.validation_outcome.value
                                     if state.validation_outcome else None,
                    "has_feedback":  self.store.exists(
                                         "feedback",
                                         f"developer_feedback_{name}.json"
                                     ),
                    "is_resolved":   self.store.exists(
                                         "feedback",
                                         f"feedback_resolved_{name}.json"
                                     ),
                }
                for name, state in ck.procs.items()
            ],
            "frozen_procs":   self.feedback.all_pending_frozen(),
        }