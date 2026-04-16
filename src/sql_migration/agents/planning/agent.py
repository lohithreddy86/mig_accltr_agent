"""
agent.py  (Planning Agent)
===========================
Implements Planning Agent steps P1 through P4 (plus P1.5):

  P1    LLM      — Strategy selection per proc (TRINO_SQL / PYSPARK_DF /
                    PYSPARK_PIPELINE / MANUAL_SKELETON).
                    Uses semantic_map for business-logic-aware strategy choices
                    when available.  Applies output_format overrides from README
                    and a strategy compatibility gate (caller must match callee
                    strategy family).
  P1.5  LLM      — Module grouping: groups related procs into cohesive output
                    modules (optional, config-gated). Falls back to one-proc-
                    per-file when disabled or on failure.
  P2    SANDBOX   — Chunk boundary computation for procs > max_lines.
                    Uses adapter.transaction_patterns for dialect-agnostic splitting.
  P3    MCP       — Live schema fetch per chunk (SELECT * LIMIT 0).
  P4    SANDBOX   — Plan assembly: inject construct_hints, schema_context,
                    semantic_context, UDF implementations, write directives,
                    and loop_guards per chunk.

Targeted replan:
  If agent_input.replan_proc_name is set, only P1+P4 re-run for that one proc.
  The existing plan.json is loaded and only the affected proc's entry is replaced.
  This is the path the Orchestrator uses after a proc fails and needs a new strategy.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sql_migration.core.artifact_store import ArtifactStore
from sql_migration.core.config_loader import get_config
from sql_migration.core.llm_client import LLMClient
from sql_migration.core.logger import get_logger
from sql_migration.core.mcp_client import MCPClientSync
from sql_migration.core.sandbox import Sandbox
from sql_migration.models import (
    AnalysisAgentOutput,
    ChunkPlan,
    ComplexityReport,
    ConstructMap,
    ConversionOrder,
    DialectAdapter,
    DialectProfile,
    Manifest,
    ModuleGroupEntry,
    ModuleGroupingPlan,
    Plan,
    PlanningAgentInput,
    PlanningAgentOutput,
    ProcChunkPlan,
    ProcPlan,
    SourceProcRange,
    StrategyMap,
    TableRegistry,
    build_llm_module_grouping_input,
    build_llm_strategy_input,
    parse_llm_module_grouping_output,
    parse_llm_strategy_output,
)
from sql_migration.models.common import (
    ChunkInfo,
    ComplexityScore,
    ConversionStrategy,
    LoopGuards,
)
from sql_migration.models.planning import ProcStrategyContext

log = get_logger("planning")

# Strategy tier ordering for module-level strategy selection.
# Higher tier = more complex conversion strategy. When a module contains
# procs with different strategies, the module uses the HIGHEST tier.
# String comparison of enum values gives wrong order (T > P alphabetically).
_STRATEGY_TIER: dict[ConversionStrategy, int] = {
    ConversionStrategy.TRINO_SQL:        1,
    ConversionStrategy.PYSPARK_DF:       2,
    ConversionStrategy.PYSPARK_PIPELINE: 3,
    ConversionStrategy.MANUAL_SKELETON:  4,
}


class PlanningAgent:
    """
    Planning Agent — decides HOW to convert each proc, optionally groups
    procs into output modules, and builds the complete per-chunk plan with
    injected schema, construct, semantic, UDF, and write-directive context.

    Supports full planning runs, targeted single-proc replanning, output
    format overrides, and cross-proc strategy compatibility enforcement.
    """

    def __init__(
        self,
        store:   ArtifactStore,
        sandbox: Sandbox,
        mcp:     MCPClientSync | None = None,
    ) -> None:
        self.store   = store
        self.sandbox = sandbox
        self.mcp     = mcp or MCPClientSync()
        self.llm     = LLMClient(agent="planning")
        self.cfg     = get_config().planning
        self.global_cfg = get_config()

    # =========================================================================
    # Public entrypoint
    # =========================================================================

    def run(self, agent_input: PlanningAgentInput) -> PlanningAgentOutput:
        """
        Full planning run (or targeted replan for one proc).
        """
        run_id = agent_input.run_id
        log.info("planning_start", run_id=run_id,
                 replan_proc=agent_input.replan_proc_name)

        # Load all upstream artifacts
        manifest       = self._load_manifest()
        complexity      = self._load_complexity_report()
        construct_map  = self._load_construct_map()
        conv_order     = self._load_conversion_order()
        dialect_profile = self._load_dialect_profile()
        adapter        = self._load_adapter(dialect_profile.adapter_path)
        table_registry = self._load_table_registry()

        # v8: Load Pipeline Semantic Map from A1.5 (if available).
        # Graceful degradation: if A1.5 failed or was skipped, semantic_map
        # is None and P1/P1.5/C2 fall back to structural-only reasoning.
        semantic_map = self._load_semantic_map()

        # Targeted replan path: only re-plan the one failed proc
        if agent_input.replan_proc_name:
            return self._targeted_replan(
                proc_name=agent_input.replan_proc_name,
                replan_notes=agent_input.replan_notes,
                manifest=manifest,
                complexity=complexity,
                construct_map=construct_map,
                conv_order=conv_order,
                adapter=adapter,
                table_registry=table_registry,
                run_id=run_id,
            )

        # Full planning run
        is_query = dialect_profile.is_query_mode

        if is_query:
            # ── Query mode: force TRINO_SQL, skip P1 LLM + P1.5 + P2 ─────
            # Queries don't need PySpark wrapping — always produce Trino SQL.
            if dialect_profile.output_format == "PYSPARK":
                log.warning("query_mode_output_format_override",
                            requested="PYSPARK", forced="TRINO_SQL",
                            reason="SQL queries cannot be wrapped in PySpark. "
                                   "Overriding output_format to TRINO_SQL.")
            log.info("planning_query_mode", queries=manifest.total_procs)

            # Force all strategies to TRINO_SQL
            from sql_migration.models.planning import StrategyEntry, StrategyMap
            strategy_entries = []
            for proc in manifest.procs:
                strategy_entries.append(StrategyEntry(
                    name=proc.name,
                    strategy=ConversionStrategy.TRINO_SQL,
                    rationale="Query mode — forced TRINO_SQL",
                ))
            strategy_map = StrategyMap(strategies=strategy_entries)
            self.store.write("planning", "strategy_map.json", strategy_map.model_dump())

            # Single chunk per query, no module grouping
            module_grouping = None

            # Build simple chunk plan — one chunk per query
            from sql_migration.models.common import ChunkInfo
            from sql_migration.models.planning import ProcChunkPlan, ChunkPlan
            chunk_plan = ChunkPlan(procs={})
            for proc in manifest.procs:
                chunk_plan.procs[proc.name] = ProcChunkPlan(
                    proc_name=proc.name,
                    chunks=[ChunkInfo(
                        chunk_id=f"{proc.name}_c0",
                        start_line=proc.start_line,
                        end_line=proc.end_line,
                        line_count=proc.line_count,
                        tables=proc.tables_read + proc.tables_written,
                    )],
                    is_single_chunk=True,
                )
            self.store.write("planning", "chunk_plan.json", chunk_plan.model_dump())

            # P3: schema fetch (still needed for column types)
            chunk_plan = self._p3_fetch_schemas(chunk_plan, table_registry)

            # P4: assemble plan (skip write semantics)
            plan = self._p4_assemble_plan(
                manifest=manifest,
                strategy_map=strategy_map,
                chunk_plan=chunk_plan,
                construct_map=construct_map,
                conv_order=conv_order,
                dialect_profile=dialect_profile,
                module_grouping=module_grouping,
                run_id=run_id,
            )
            self.store.write("planning", "plan.json", plan.model_dump())
            log.info("planning_query_mode_complete",
                     total_queries=plan.total_procs(),
                     total_chunks=plan.total_chunks())

            return PlanningAgentOutput(
                plan=plan,
                strategy_map=strategy_map,
                chunk_plan=chunk_plan,
                module_grouping=None,
            )

        if dialect_profile.is_ssis_mode:
            # ── SSIS mode: force PYSPARK_DF, skip P1 LLM + P1.5 + P2 ─────
            log.info("planning_ssis_mode", tasks=manifest.total_procs)

            from sql_migration.models.planning import StrategyEntry, StrategyMap
            strategy_entries = []
            for proc in manifest.procs:
                strategy_entries.append(StrategyEntry(
                    name=proc.name,
                    strategy=ConversionStrategy.PYSPARK_DF,
                    rationale="SSIS mode — forced PYSPARK_DF",
                ))
            strategy_map = StrategyMap(strategies=strategy_entries)
            self.store.write("planning", "strategy_map.json", strategy_map.model_dump())

            module_grouping = None

            # Single chunk per task — SSIS tasks are self-contained
            from sql_migration.models.common import ChunkInfo
            from sql_migration.models.planning import ProcChunkPlan, ChunkPlan
            chunk_plan = ChunkPlan(procs={})

            # Read task_context for destination tables (need schema fetch)
            task_ctx: dict[str, dict] = {}
            try:
                task_ctx = self.store.read("parser", "task_context.json")
            except Exception:
                pass

            for proc in manifest.procs:
                tables = list(set(proc.tables_read + proc.tables_written))
                # Add destination tables from task_context so P3 fetches their schema
                ctx = task_ctx.get(proc.name, {})
                dest = ctx.get("destination", {})
                if dest and dest.get("table"):
                    dest_table = dest["table"]
                    if dest_table not in tables:
                        tables.append(dest_table)

                chunk_plan.procs[proc.name] = ProcChunkPlan(
                    proc_name=proc.name,
                    chunks=[ChunkInfo(
                        chunk_id=f"{proc.name}_c0",
                        start_line=proc.start_line,
                        end_line=proc.end_line,
                        line_count=proc.line_count,
                        tables=tables,
                    )],
                    is_single_chunk=True,
                )
            self.store.write("planning", "chunk_plan.json", chunk_plan.model_dump())

            # P3: schema fetch
            chunk_plan = self._p3_fetch_schemas(chunk_plan, table_registry)

            # P4: assemble plan
            plan = self._p4_assemble_plan(
                manifest=manifest,
                strategy_map=strategy_map,
                chunk_plan=chunk_plan,
                construct_map=construct_map,
                conv_order=conv_order,
                dialect_profile=dialect_profile,
                run_id=run_id,
                module_grouping=module_grouping,
            )

            # Post-P4: enrich plan entries with SSIS task_context metadata
            if task_ctx:
                import json as _json
                enriched_count = 0
                for proc_name, proc_plan in plan.procs.items():
                    ctx = task_ctx.get(proc_name, {})
                    if not ctx:
                        continue

                    # Inject destination as write_directive
                    dest = ctx.get("destination")
                    if dest and dest.get("table"):
                        col_map = ctx.get("column_mappings", {})
                        directive = (
                            f"WRITE to {dest['table']} "
                            f"mode={dest.get('mode', 'overwrite')}"
                        )
                        if col_map:
                            directive += f" columns={_json.dumps(col_map)}"
                        proc_plan.write_directives[dest["table"]] = directive

                    # Inject transformation chain as semantic_context
                    parts = []
                    if ctx.get("transformation_chain"):
                        parts.append(
                            "SSIS Data Flow transformations to replicate:\n"
                            + _json.dumps(ctx["transformation_chain"], indent=2))
                    if ctx.get("ssis_expressions"):
                        parts.append(
                            "SSIS Expressions to convert:\n"
                            + _json.dumps(ctx["ssis_expressions"], indent=2))
                    if ctx.get("column_mappings"):
                        parts.append(
                            "Column mappings (source→destination):\n"
                            + _json.dumps(ctx["column_mappings"], indent=2))
                    if parts:
                        existing = proc_plan.semantic_context or ""
                        proc_plan.semantic_context = (
                            existing + ("\n\n" if existing else "") + "\n\n".join(parts)
                        )
                        enriched_count += 1

                log.info("ssis_task_context_injected",
                         enriched_procs=enriched_count)

            self.store.write("planning", "plan.json", plan.model_dump())
            log.info("planning_ssis_mode_complete",
                     total_tasks=plan.total_procs(),
                     total_chunks=plan.total_chunks())

            return PlanningAgentOutput(
                plan=plan,
                strategy_map=strategy_map,
                chunk_plan=chunk_plan,
                module_grouping=None,
            )

        # ── P1: LLM strategy selection ────────────────────────────────────────
        strategy_map = self._p1_select_strategies(
            manifest, complexity, construct_map, semantic_map=semantic_map,
        )
        self.store.write("planning", "strategy_map.json", strategy_map.model_dump())
        log.info("p1_complete",
                 strategies={e.strategy.value: sum(
                     1 for s in strategy_map.strategies if s.strategy.value == e.strategy.value
                 ) for e in strategy_map.strategies},
                 shared_utilities=strategy_map.shared_utilities)

        # ── Output format enforcement ──────────────────────────────────────────
        # If the user specified a desired output format in the README,
        # override LLM strategy choices to match (except MANUAL_SKELETON).
        desired = dialect_profile.output_format
        if desired and desired != "AUTO":
            overrides = 0
            for entry in strategy_map.strategies:
                if entry.strategy == ConversionStrategy.MANUAL_SKELETON:
                    continue  # Never override MANUAL
                if desired == "TRINO_SQL" and entry.strategy != ConversionStrategy.TRINO_SQL:
                    entry.strategy = ConversionStrategy.TRINO_SQL
                    entry.rationale += f" [overridden to TRINO_SQL per README output_format]"
                    overrides += 1
                elif desired == "PYSPARK":
                    if entry.strategy == ConversionStrategy.TRINO_SQL:
                        entry.strategy = ConversionStrategy.PYSPARK_DF
                        entry.rationale += f" [overridden to PYSPARK_DF per README output_format]"
                        overrides += 1
                    # Keep PYSPARK_PIPELINE if already assigned — it's still PySpark
            if overrides:
                log.info("output_format_override",
                         desired=desired, overrides=overrides)
                self.store.write("planning", "strategy_map.json",
                                 strategy_map.model_dump())

        # ── Strategy compatibility gate ─────────────────────────────────────────
        # If a callee is PYSPARK_* and a caller is TRINO_SQL, the caller's
        # converted SQL cannot directly invoke the callee's Python function.
        # GATE: force the caller to escalate to the callee's strategy family
        # rather than letting it proceed and fail at validation.
        # (Downgrading the callee would be wrong — it was assigned PYSPARK
        # for a reason: cursor logic, complexity, etc.)
        strat_by_name = {e.name: e.strategy for e in strategy_map.strategies}
        alignments_made = 0
        for entry in strategy_map.strategies:
            caller_strat = entry.strategy
            for callee_name in (manifest.get_proc(entry.name).calls
                                if manifest.get_proc(entry.name) else []):
                callee_strat = strat_by_name.get(callee_name)
                if callee_strat is None:
                    continue
                # TRINO_SQL caller → PYSPARK_* callee: incompatible at runtime
                if (caller_strat == ConversionStrategy.TRINO_SQL
                        and callee_strat in {
                            ConversionStrategy.PYSPARK_DF,
                            ConversionStrategy.PYSPARK_PIPELINE,
                        }):
                    # Escalate the caller to match the callee's strategy
                    old_strat = entry.strategy
                    entry.strategy = callee_strat
                    strat_by_name[entry.name] = callee_strat
                    entry.rationale += (
                        f" | ESCALATED from {old_strat.value}: caller invokes "
                        f"'{callee_name}' ({callee_strat.value}) which requires "
                        f"Python runtime."
                    )
                    alignments_made += 1
                    caller_strat = callee_strat  # Update for further callee checks

                    log.info(
                        "strategy_incompatibility_resolved",
                        caller=entry.name,
                        old_strategy=old_strat.value,
                        new_strategy=callee_strat.value,
                        callee=callee_name,
                        callee_strategy=callee_strat.value,
                        reason=(
                            f"TRINO_SQL proc cannot invoke Python function at runtime. "
                            f"Escalated caller to {callee_strat.value} to match callee."
                        ),
                    )
        if alignments_made:
            # Re-write strategy map with corrected strategies
            self.store.write("planning", "strategy_map.json", strategy_map.model_dump())
            log.info("strategy_alignment_complete",
                     alignments=alignments_made)

        # ── P1.5: Module grouping (v7) ────────────────────────────────────────
        # Group related procs into output modules so the conversion produces
        # 4-5 cohesive files instead of 23 isolated proc files.
        # When disabled: each proc becomes its own single-proc module (backward compat).
        module_grouping = self._p1_5_group_into_modules(
            manifest=manifest,
            strategy_map=strategy_map,
            conv_order=conv_order,
        )
        if module_grouping:
            self.store.write("planning", "module_grouping.json",
                             module_grouping.model_dump())
            log.info("p1_5_complete",
                     modules=len(module_grouping.modules),
                     source_procs=sum(len(m.source_procs) for m in module_grouping.modules))

        # ── P2: Chunk boundaries ──────────────────────────────────────────────
        chunk_plan = self._p2_compute_chunks(
            manifest, strategy_map, adapter, module_grouping,
        )
        self.store.write("planning", "chunk_plan.json", chunk_plan.model_dump())
        log.info("p2_complete",
                 total_chunks=sum(
                     pp.chunk_count for pp in chunk_plan.procs.values()
                 ))

        # ── P3: Live schema fetch per chunk ───────────────────────────────────
        chunk_plan = self._p3_fetch_schemas(chunk_plan, table_registry)
        log.info("p3_complete", msg="schema context injected into all chunks")

        # ── P4: Assemble master plan with construct hints + loop guards ───────
        # v6-fix: Load dependency gate resolution for dep context enrichment
        dep_resolution = self._load_dependency_resolution()
        if dep_resolution.get("all_resolved"):
            log.info("p4_dep_resolution_loaded",
                     stub_count=len(dep_resolution.get("stub_deps", [])),
                     inline_count=len(dep_resolution.get("inline_bodies", {})),
                     skip_count=len(dep_resolution.get("skip_deps", [])))

        plan = self._p4_assemble_plan(
            manifest=manifest,
            strategy_map=strategy_map,
            chunk_plan=chunk_plan,
            construct_map=construct_map,
            conv_order=conv_order,
            dialect_profile=dialect_profile,
            run_id=run_id,
            dep_resolution=dep_resolution,
            module_grouping=module_grouping,
        )
        self.store.write("planning", "plan.json", plan.model_dump())

        # ── Write-conflict detection ──────────────────────────────────────────
        conflicts = plan.write_conflicts()
        if conflicts:
            for tbl, writers in conflicts.items():
                log.warning(
                    "write_conflict_detected",
                    table=tbl,
                    writers=writers,
                    writer_count=len(writers),
                    hint=(
                        f"{len(writers)} procs all write to '{tbl}'. "
                        "In the source DB this may be intended (sequential MERGE/INSERT). "
                        "In PySpark each proc that uses mode('overwrite') will truncate "
                        "the previous proc's data. Review write semantics after conversion."
                    ),
                )
            # Store for Streamlit UI display
            self.store.write(
                "planning", "write_conflicts.json",
                {tbl: writers for tbl, writers in conflicts.items()}
            )

        # ── v5: Write Semantics Analysis ──────────────────────────────────────
        # When procs share output tables, classify DML intent per proc and
        # generate MANDATORY write directives for conversion prompts.
        # Anti-hallucination: LLM defaults to mode("overwrite") from training.
        # With 23 procs sharing one table, only the last proc's data survives.
        ws_cfg = get_config().write_semantics
        if conflicts and ws_cfg.enabled:
            from sql_migration.core.write_semantics import WriteSemanticsAnalyzer
            ws_analyzer = WriteSemanticsAnalyzer(
                self.store, self.sandbox, llm=self.llm,
                default_write_mode=ws_cfg.default_write_mode,
                low_confidence_threshold=ws_cfg.low_confidence_threshold,
            )
            write_semantics = ws_analyzer.analyze(
                write_conflicts=conflicts,
                conversion_order=plan.conversion_order,
            )
            # Inject write directives into each affected proc's plan
            for proc_name, proc_plan in plan.procs.items():
                directives = write_semantics.get_directive_for_proc(
                    proc_name, proc_plan.strategy.value,
                )
                if directives:
                    proc_plan.write_directives = directives
            # Re-persist the enriched plan
            self.store.write("planning", "plan.json", plan.model_dump())
            log.info("write_semantics_injected",
                     tables=len(write_semantics.tables),
                     procs_with_directives=sum(
                         1 for p in plan.procs.values() if p.write_directives
                     ))

        log.info("planning_complete",
                 total_procs=plan.total_procs(),
                 total_chunks=plan.total_chunks(),
                 write_conflicts=len(conflicts))

        return PlanningAgentOutput(
            plan=plan,
            strategy_map=strategy_map,
            chunk_plan=chunk_plan,
            module_grouping=module_grouping,
        )

    # =========================================================================
    # Step P1 — LLM strategy selection
    # =========================================================================

    def _p1_select_strategies(
        self,
        manifest:      Manifest,
        complexity:    ComplexityReport,
        construct_map: ConstructMap,
        semantic_map = None,
    ) -> StrategyMap:
        """
        Build per-proc strategy context (no raw code) and call LLM.
        When semantic_map is available, includes business logic context
        so strategy selection considers historical patterns and
        recommended PySpark approaches, not just structural flags.
        """
        contexts: list[ProcStrategyContext] = []

        for proc in manifest.procs:
            comp_entry = complexity.get(proc.name)
            comp_score = comp_entry.score if comp_entry else ComplexityScore.LOW
            comp_rationale = comp_entry.rationale if comp_entry else ""
            shared_candidate = comp_entry.shared_utility_candidate if comp_entry else False

            # Compute construct coverage for this proc
            body_fns  = proc.dialect_functions_in_body or []
            coverage  = construct_map.hint_for_chunk(body_fns)
            unmapped  = proc.unmapped_count or sum(
                1 for v in coverage.values() if "UNMAPPED" in v
            )

            ctx = ProcStrategyContext(
                name=proc.name,
                line_count=proc.line_count,
                complexity=comp_score,
                complexity_rationale=comp_rationale,
                tables_read=proc.tables_read,
                tables_written=proc.tables_written,
                has_cursor=proc.has_cursor,
                has_dynamic_sql=proc.has_dynamic_sql,
                has_unsupported=proc.has_unsupported,
                unmapped_count=unmapped,
                construct_coverage=coverage,
                shared_utility_candidate=shared_candidate,
                calls=proc.calls,
            )
            contexts.append(ctx)

        # Build semantic context for the strategy LLM call
        semantic_context = ""
        if semantic_map:
            semantic_context = semantic_map.context_for_planning()

        _, messages = build_llm_strategy_input(
            proc_contexts=contexts,
            shared_utility_threshold=self.cfg.shared_utility_min_callers,
            semantic_context=semantic_context,
        )

        log.info("p1_llm_calling",
                 model=self.llm.model,
                 proc_count=len(contexts))
        raw = self.llm.call(messages=messages, expect_json=True)
        return parse_llm_strategy_output(raw)

    # =========================================================================
    # Step P1.5 — Module grouping (v7)
    # =========================================================================

    def _p1_5_group_into_modules(
        self,
        manifest:     Manifest,
        strategy_map: StrategyMap,
        conv_order:   ConversionOrder,
    ) -> ModuleGroupingPlan | None:
        """
        Group related source procs into output modules.

        When enabled: LLM analyzes all procs' data flow, dependencies, and
        write patterns to produce 4-5 cohesive modules instead of 23 files.

        When disabled (or on failure): returns None. P4 falls back to creating
        one plan entry per proc (backward compatible behavior).

        Returns ModuleGroupingPlan or None.
        """
        cfg = self.cfg.module_grouping
        if not cfg.enabled:
            log.info("p1_5_skipped", reason="module_grouping.enabled=false")
            return None

        if len(manifest.procs) <= 1:
            log.info("p1_5_skipped", reason="single proc — no grouping needed")
            return None

        # Build proc contexts for the LLM (metadata only, no raw code)
        proc_contexts = []
        for proc in manifest.procs:
            strat_ent = strategy_map.get(proc.name)
            proc_contexts.append({
                "name":           proc.name,
                "line_count":     proc.line_count,
                "tables_read":    proc.tables_read,
                "tables_written": proc.tables_written,
                "calls":          proc.calls,
                "has_cursor":     proc.has_cursor,
                "strategy":       strat_ent.strategy.value if strat_ent else "PYSPARK_DF",
            })

        # Build dependency edges
        try:
            dep_graph = self.store.read("analysis", "dependency_graph.json")
            dep_edges = dep_graph.get("edges", [])
        except (FileNotFoundError, KeyError):
            dep_edges = []

        # Build write conflict map (tables written by multiple procs)
        from collections import defaultdict
        table_writers: dict[str, list[str]] = defaultdict(list)
        for proc in manifest.procs:
            for tbl in proc.tables_written:
                table_writers[tbl].append(proc.name)
        write_conflicts = {t: w for t, w in table_writers.items() if len(w) > 1}

        # Strategy map for LLM context
        strat_dict = {e.name: e.strategy.value for e in strategy_map.strategies}

        messages = build_llm_module_grouping_input(
            proc_contexts=proc_contexts,
            dependency_edges=dep_edges,
            write_conflict_tables=write_conflicts,
            strategy_map=strat_dict,
            max_procs_per_module=cfg.max_procs_per_module,
            merge_write_conflicts=cfg.merge_write_conflicts,
        )

        log.info("p1_5_llm_calling",
                 model=self.llm.model,
                 proc_count=len(proc_contexts),
                 write_conflict_tables=len(write_conflicts))

        try:
            raw = self.llm.call(messages=messages, expect_json=True)
            grouping = parse_llm_module_grouping_output(raw)
        except Exception as e:
            log.warning("p1_5_llm_failed",
                        error=str(e)[:200],
                        fallback="each proc becomes its own module")
            return None

        # Validate: every proc must appear in exactly one module
        all_grouped_procs = set()
        all_manifest_procs = {p.name for p in manifest.procs}
        for m in grouping.modules:
            for p in m.source_procs:
                if p in all_grouped_procs:
                    log.warning("p1_5_duplicate_proc",
                                proc=p, module=m.module_name,
                                fallback="disabling module grouping")
                    return None
                all_grouped_procs.add(p)

        # Procs not assigned to any module — create single-proc modules for them
        missing = all_manifest_procs - all_grouped_procs
        if missing:
            log.info("p1_5_ungrouped_procs",
                     count=len(missing),
                     procs=sorted(missing)[:5])
            for proc_name in sorted(missing):
                proc = manifest.get_proc(proc_name)
                grouping.modules.append(ModuleGroupEntry(
                    module_name=proc_name,
                    description=f"Single-proc module for {proc_name}",
                    source_procs=[proc_name],
                    output_tables=proc.tables_written if proc else [],
                    rationale="Not grouped by LLM — standalone module",
                ))
                if proc_name not in grouping.execution_order:
                    grouping.execution_order.append(proc_name)

        # ── Module-level cycle detection + topological sort ────────────
        # The LLM produces execution_order, but may introduce cycles that
        # didn't exist at the proc level (e.g., proc1 in module_A calls
        # proc2 in module_B, and proc3 in module_B calls proc4 in module_A).
        # A module-level cycle causes the Orchestrator to deadlock forever
        # (_deps_clear(A) waits for B, _deps_clear(B) waits for A).
        #
        # Fix: translate proc-level dep edges → module-level edges, detect
        # cycles via Kahn's algorithm, and produce a safe topological order.
        # If cycles exist: fall back to single-proc mode (no grouping).

        # Build proc → module mapping
        proc_to_module: dict[str, str] = {}
        for m in grouping.modules:
            for pn in m.source_procs:
                proc_to_module[pn] = m.module_name

        # Translate proc-level edges to module-level edges
        module_edges: set[tuple[str, str]] = set()
        for edge in dep_edges:
            caller_proc = edge[0] if isinstance(edge, (list, tuple)) else edge
            callee_proc = edge[1] if isinstance(edge, (list, tuple)) else edge
            caller_mod = proc_to_module.get(caller_proc)
            callee_mod = proc_to_module.get(callee_proc)
            if caller_mod and callee_mod and caller_mod != callee_mod:
                # callee must finish before caller → edge: callee_mod → caller_mod
                module_edges.add((callee_mod, caller_mod))

        # Kahn's algorithm for topological sort + cycle detection
        module_names = {m.module_name for m in grouping.modules}
        in_degree: dict[str, int] = {mn: 0 for mn in module_names}
        adjacency: dict[str, list[str]] = {mn: [] for mn in module_names}
        for src, dst in module_edges:
            if src in module_names and dst in module_names:
                adjacency[src].append(dst)
                in_degree[dst] += 1

        queue = [mn for mn in module_names if in_degree[mn] == 0]
        topo_order: list[str] = []
        while queue:
            # Stable sort: pick alphabetically first among zero-in-degree nodes
            queue.sort()
            node = queue.pop(0)
            topo_order.append(node)
            for neighbor in adjacency[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(topo_order) != len(module_names):
            # Cycle detected — identify the modules involved
            cycle_modules = sorted(module_names - set(topo_order))
            log.warning(
                "p1_5_module_level_cycle_detected",
                cycle_modules=cycle_modules,
                module_edges=[(s, d) for s, d in module_edges
                              if s in cycle_modules or d in cycle_modules],
                fallback="disabling module grouping to prevent orchestrator deadlock",
                hint=(
                    "The LLM grouped procs into modules that have circular "
                    "dependencies.  Falling back to single-proc mode.  "
                    "To fix: adjust module grouping constraints or manually "
                    "specify grouping in config."
                ),
            )
            return None

        # Use the validated topological order
        grouping.execution_order = topo_order

        log.info("p1_5_grouping_result",
                 modules=len(grouping.modules),
                 avg_procs_per_module=round(len(all_manifest_procs) / max(len(grouping.modules), 1), 1),
                 write_conflicts_resolved=len(write_conflicts),
                 module_edges=len(module_edges),
                 execution_order=topo_order[:5])

        return grouping

    # =========================================================================
    # Step P2 — Chunk boundary computation
    # =========================================================================

    def _p2_compute_chunks(
        self,
        manifest:        Manifest,
        strategy_map:    StrategyMap,
        adapter:         DialectAdapter,
        module_grouping: ModuleGroupingPlan | None = None,
    ) -> ChunkPlan:
        """
        Compute chunk boundaries.

        When module_grouping is active: creates chunk entries keyed by MODULE
        name. Each source proc within a module becomes one or more chunks
        (a proc is never split — consistent with the grouping constraint).
        The chunks are ordered: proc1_c0, proc1_c1, proc2_c0, etc.

        When module_grouping is None: original per-proc behavior (backward compat).
        """
        chunk_plan = ChunkPlan()

        if module_grouping:
            # ── Module mode: iterate modules ──────────────────────────────────
            for mod in module_grouping.modules:
                module_chunks: list[ChunkInfo] = []

                # Determine dominant strategy for this module
                # (use highest-tier strategy among member procs)
                mod_strategy = ConversionStrategy.TRINO_SQL
                for proc_name in mod.source_procs:
                    se = strategy_map.get(proc_name)
                    if se and _STRATEGY_TIER.get(se.strategy, 0) > _STRATEGY_TIER.get(mod_strategy, 0):
                        mod_strategy = se.strategy

                for proc_name in mod.source_procs:
                    proc = manifest.get_proc(proc_name)
                    if not proc:
                        continue

                    strategy_entry = strategy_map.get(proc_name)
                    strategy = strategy_entry.strategy if strategy_entry else mod_strategy

                    # Each source proc gets its own chunk(s) within the module.
                    # Chunk IDs: {module_name}_{proc_name}_c0, c1, ...
                    # This preserves proc boundaries — a proc is never split across
                    # module boundaries (constraint from P1.5).

                    if (strategy == ConversionStrategy.MANUAL_SKELETON
                            or proc.line_count <= self.cfg.chunk_max_lines):
                        # Single chunk for this proc within the module
                        module_chunks.append(ChunkInfo(
                            chunk_id=f"{mod.module_name}_{proc_name}_c0",
                            start_line=proc.start_line,
                            end_line=proc.end_line,
                            line_count=proc.line_count,
                        ))
                    else:
                        # Large proc: compute sub-chunks via sandbox
                        result = self.sandbox.run(
                            script="extraction/compute_chunks.py",
                            args={
                                "proc_name":    proc_name,
                                "source_file":  proc.source_file,
                                "start_line":   proc.start_line,
                                "end_line":     proc.end_line,
                                "adapter":      adapter.model_dump(),
                                "target_lines": self.cfg.chunk_target_lines,
                                "max_lines":    self.cfg.chunk_max_lines,
                                "min_lines":    self.cfg.chunk_min_lines,
                            },
                        )
                        if result.success:
                            data = result.stdout_json
                            for c in data.get("chunks", []):
                                # Prefix chunk IDs with module name for uniqueness
                                module_chunks.append(ChunkInfo(
                                    chunk_id=f"{mod.module_name}_{c['chunk_id']}",
                                    start_line=c["start_line"],
                                    end_line=c["end_line"],
                                    line_count=c["line_count"],
                                    state_vars=c.get("state_vars", {}),
                                ))
                        else:
                            # Fallback: single chunk for this proc
                            module_chunks.append(ChunkInfo(
                                chunk_id=f"{mod.module_name}_{proc_name}_c0",
                                start_line=proc.start_line,
                                end_line=proc.end_line,
                                line_count=proc.line_count,
                            ))

                chunk_plan.procs[mod.module_name] = ProcChunkPlan(
                    proc_name=mod.module_name,
                    chunks=module_chunks,
                    is_single_chunk=len(module_chunks) == 1,
                )
                log.debug("p2_module_chunks",
                          module=mod.module_name,
                          source_procs=len(mod.source_procs),
                          chunks=len(module_chunks))

            return chunk_plan

        # ── Legacy mode: iterate procs directly (unchanged) ───────────────────
        for proc in manifest.procs:
            strategy_entry = strategy_map.get(proc.name)
            strategy = strategy_entry.strategy if strategy_entry else ConversionStrategy.TRINO_SQL

            # MANUAL_SKELETON: always single chunk (scaffolded with TODOs)
            if strategy == ConversionStrategy.MANUAL_SKELETON:
                chunk_plan.procs[proc.name] = ProcChunkPlan(
                    proc_name=proc.name,
                    chunks=[ChunkInfo(
                        chunk_id=f"{proc.name}_c0",
                        start_line=proc.start_line,
                        end_line=proc.end_line,
                        line_count=proc.line_count,
                    )],
                    is_single_chunk=True,
                )
                continue

            # Small procs: single chunk, no sandbox call needed
            if proc.line_count <= self.cfg.chunk_max_lines:
                chunk_plan.procs[proc.name] = ProcChunkPlan(
                    proc_name=proc.name,
                    chunks=[ChunkInfo(
                        chunk_id=f"{proc.name}_c0",
                        start_line=proc.start_line,
                        end_line=proc.end_line,
                        line_count=proc.line_count,
                    )],
                    is_single_chunk=True,
                )
                continue

            # Large procs: compute boundaries via sandbox
            result = self.sandbox.run(
                script="extraction/compute_chunks.py",
                args={
                    "proc_name":    proc.name,
                    "source_file":  proc.source_file,
                    "start_line":   proc.start_line,
                    "end_line":     proc.end_line,
                    "adapter":      adapter.model_dump(),
                    "target_lines": self.cfg.chunk_target_lines,
                    "max_lines":    self.cfg.chunk_max_lines,
                    "min_lines":    self.cfg.chunk_min_lines,
                },
            )

            if not result.success:
                log.warning("p2_chunk_compute_failed",
                            proc=proc.name, stderr=result.stderr[:200])
                # Fallback: single chunk
                chunk_plan.procs[proc.name] = ProcChunkPlan(
                    proc_name=proc.name,
                    chunks=[ChunkInfo(
                        chunk_id=f"{proc.name}_c0",
                        start_line=proc.start_line,
                        end_line=proc.end_line,
                        line_count=proc.line_count,
                    )],
                    is_single_chunk=True,
                )
                continue

            data   = result.stdout_json
            chunks = [
                ChunkInfo(
                    chunk_id=c["chunk_id"],
                    start_line=c["start_line"],
                    end_line=c["end_line"],
                    line_count=c["line_count"],
                    state_vars=c.get("state_vars", {}),
                )
                for c in data.get("chunks", [])
            ]
            chunk_plan.procs[proc.name] = ProcChunkPlan(
                proc_name=proc.name,
                chunks=chunks,
                is_single_chunk=data.get("is_single_chunk", True),
            )
            log.debug("p2_chunks_computed",
                      proc=proc.name,
                      chunks=len(chunks))

        return chunk_plan

    # =========================================================================
    # Step P3 — Live schema fetch per chunk
    # =========================================================================

    def _p3_fetch_schemas(
        self,
        chunk_plan:     ChunkPlan,
        table_registry: TableRegistry,
    ) -> ChunkPlan:
        """
        For each chunk: identify referenced tables and fetch live Trino schema.
        Uses 'SELECT * FROM table LIMIT 0' — catches views and computed columns
        that desc_table misses.

        v6-fix (Gap 2): Results are cached in _schema_cache so each table is
        probed at most once. For 23 procs × 2 chunks × 3 shared tables,
        this reduces MCP calls from ~138 to ~3.
        """
        from sql_migration.models.common import TableStatus

        # Per-run schema cache: {table_src_name → schema_ctx_dict}
        # Shared across all procs/chunks in this planning run.
        schema_cache: dict[str, dict] = {}

        for proc_name, proc_chunk_plan in chunk_plan.procs.items():
            for chunk in proc_chunk_plan.chunks:
                schema_ctx: dict[str, Any] = {}

                for table_src in chunk.tables or []:
                    # Cache hit — reuse previous probe result
                    if table_src in schema_cache:
                        schema_ctx[table_src] = schema_cache[table_src]
                        continue

                    entry = table_registry.get(table_src)
                    if not entry:
                        continue

                    if entry.status == TableStatus.EXISTS_IN_TRINO:
                        # Live schema probe
                        try:
                            probe_q = (
                                self.global_cfg.mcp.schema_probe_query
                                .format(table=entry.trino_fqn)
                            )
                            rows = self.mcp.run_query(probe_q)
                            columns = entry.columns
                            result = {
                                "trino_fqn": entry.trino_fqn,
                                "columns":   [c.name for c in columns],
                                "types":     {c.name: c.type for c in columns},
                            }
                        except Exception as e:
                            log.debug("p3_schema_probe_failed",
                                      table=table_src,
                                      error=str(e))
                            result = {
                                "trino_fqn": entry.trino_fqn,
                                "columns":   [c.name for c in entry.columns],
                                "types":     {c.name: c.type for c in entry.columns},
                            }
                        schema_cache[table_src] = result
                        schema_ctx[table_src] = result

                    elif entry.status == TableStatus.TEMP_TABLE:
                        result = {
                            "trino_fqn": "",
                            "is_temp":   True,
                            "columns":   [],
                            "types":     {},
                        }
                        schema_cache[table_src] = result
                        schema_ctx[table_src] = result

                chunk.schema_context = schema_ctx

        if schema_cache:
            log.info("p3_schema_cache_stats",
                     unique_tables=len(schema_cache),
                     msg="MCP calls reduced to 1 per unique table")

        return chunk_plan

    # =========================================================================
    # Step P4 — Plan assembly
    # =========================================================================

    def _p4_assemble_plan(
        self,
        manifest:        Manifest,
        strategy_map:    StrategyMap,
        chunk_plan:      ChunkPlan,
        construct_map:   ConstructMap,
        conv_order:      ConversionOrder,
        dialect_profile: DialectProfile,
        run_id:          str,
        replan_notes:    str = "",
        dep_resolution:  dict | None = None,
        module_grouping: ModuleGroupingPlan | None = None,
    ) -> Plan:
        """
        Assemble the final plan.json.

        When module_grouping is active: creates one ProcPlan per MODULE,
        with source_procs, source_ranges, and module_description populated.
        Tables, calls, unresolved_deps are unioned across all member procs.

        When module_grouping is None: original per-proc behavior (backward compat).
        """
        dep_resolution = dep_resolution or {}
        loop_guards = LoopGuards(
            max_chunk_retry=self.cfg.loop_guards.max_chunk_retry,
            max_replan_depth=self.cfg.loop_guards.max_replan_depth,
            frozen_after=self.cfg.loop_guards.frozen_after,
        )

        # v6-fix (Fix 6): Compute procs to exclude via SKIP_CALLERS resolution
        skip_callers_procs: set[str] = set()
        skip_deps = set(dep_resolution.get("skip_deps", []))
        if skip_deps:
            for dep_info in dep_resolution.get("deps", []):
                if dep_info.get("dep_name") in skip_deps:
                    skip_callers_procs.update(dep_info.get("called_by", []))
            if skip_callers_procs:
                log.info("skip_callers_exclusion",
                         skip_deps=list(skip_deps),
                         excluded_procs=sorted(skip_callers_procs))

        procs: dict[str, ProcPlan] = {}

        if module_grouping:
            # ── Module mode ───────────────────────────────────────────────────

            # Build proc → module name mapping for cross-module call translation
            proc_to_module: dict[str, str] = {}
            for m in module_grouping.modules:
                for pn in m.source_procs:
                    proc_to_module[pn] = m.module_name

            for mod in module_grouping.modules:
                module_name = mod.module_name

                # Skip modules where ALL member procs are excluded
                member_procs = [
                    manifest.get_proc(pn) for pn in mod.source_procs
                    if pn not in skip_callers_procs and manifest.get_proc(pn)
                ]
                if not member_procs:
                    log.info("module_skipped_all_excluded", module=module_name)
                    continue

                # Determine dominant strategy (highest tier among members)
                mod_strategy = ConversionStrategy.TRINO_SQL
                mod_rationale_parts = []
                mod_shared = False
                for pe in member_procs:
                    se = strategy_map.get(pe.name)
                    if se:
                        if _STRATEGY_TIER.get(se.strategy, 0) > _STRATEGY_TIER.get(mod_strategy, 0):
                            mod_strategy = se.strategy
                        if se.shared_utility:
                            mod_shared = True
                        mod_rationale_parts.append(f"{pe.name}:{se.strategy.value}")

                # Union tables, calls, deps across member procs
                all_tables_written = sorted(set(
                    t for pe in member_procs for t in (pe.tables_written or [])
                ))
                # Calls: translate proc-level calls to MODULE-level calls
                # If sp_a (in module_staging) calls sp_d (in module_calc),
                # then module_staging's calls = ['module_calc']
                internal_procs = set(mod.source_procs)
                external_module_calls = set()
                for pe in member_procs:
                    for c in (pe.calls or []):
                        if c not in internal_procs:
                            target_module = proc_to_module.get(c)
                            if target_module and target_module != module_name:
                                external_module_calls.add(target_module)
                            elif not target_module:
                                # Callee not in any module (could be external dep)
                                external_module_calls.add(c)
                all_calls = sorted(external_module_calls)
                all_unresolved = sorted(set(
                    d for pe in member_procs for d in (pe.unresolved_deps or [])
                ))

                # Apply dependency resolution across all member procs
                filtered_unresolved, resolved_context = self._build_dep_resolution_context(
                    all_unresolved, dep_resolution,
                )

                # Build source_ranges for extraction
                source_ranges = [
                    SourceProcRange(
                        proc_name=pe.name,
                        source_file=pe.source_file,
                        start_line=pe.start_line,
                        end_line=pe.end_line,
                        line_count=pe.line_count,
                    )
                    for pe in member_procs
                ]

                # Get chunk plan for this module
                mod_chunk_plan = chunk_plan.procs.get(module_name)
                if not mod_chunk_plan:
                    log.warning("p4_no_chunks_for_module", module=module_name)
                    continue

                # Enrich chunks with construct hints from all member procs
                enriched_chunks: list[ChunkInfo] = []
                cumulative_state_vars: dict[str, str] = {}

                # Union all dialect functions across member procs for hint generation
                all_body_fns = []
                for pe in member_procs:
                    all_body_fns.extend(pe.dialect_functions_in_body or [])
                all_body_fns = list(set(all_body_fns))

                for chunk in mod_chunk_plan.chunks:
                    hints = construct_map.hint_for_chunk(
                        all_body_fns, strategy=mod_strategy.value
                    )
                    chunk_tables = list(chunk.schema_context.keys()) if chunk.schema_context else []

                    enriched = ChunkInfo(
                        chunk_id=chunk.chunk_id,
                        start_line=chunk.start_line,
                        end_line=chunk.end_line,
                        line_count=chunk.line_count,
                        tables=chunk_tables,
                        state_vars=cumulative_state_vars.copy(),
                        schema_context=chunk.schema_context or {},
                        construct_hints=hints,
                    )
                    enriched_chunks.append(enriched)
                    cumulative_state_vars.update(chunk.state_vars or {})

                # Use first member proc's source_file as primary (for backward compat)
                primary_source = member_procs[0].source_file if member_procs else ""

                proc_plan = ProcPlan(
                    proc_name=module_name,
                    source_file=primary_source,
                    strategy=mod_strategy,
                    strategy_rationale=(
                        f"Module [{', '.join(mod_rationale_parts)}]. "
                        f"{mod.rationale}"
                        + (f" | Replan notes: {replan_notes}" if replan_notes else "")
                    ),
                    chunks=enriched_chunks,
                    loop_guards=loop_guards,
                    shared_utility=mod_shared,
                    # v7: Module grouping fields
                    source_procs=[pe.name for pe in member_procs],
                    source_ranges=source_ranges,
                    module_description=mod.description,
                    # Unioned deps
                    unresolved_deps=filtered_unresolved,
                    dep_resolution_context=resolved_context,
                    calls=all_calls,
                    tables_written=all_tables_written,
                )
                procs[module_name] = proc_plan

            # Use module execution order
            final_order = [
                mn for mn in module_grouping.execution_order
                if mn in procs
            ]
            # Add any modules not in execution_order (shouldn't happen, but safety)
            for mn in procs:
                if mn not in final_order:
                    final_order.append(mn)

            return Plan(
                procs=procs,
                conversion_order=final_order,
                run_id=run_id,
                module_grouping=module_grouping,
            )

        # ── Legacy mode: per-proc plan assembly (unchanged) ───────────────────
        for proc_entry in manifest.procs:
            name      = proc_entry.name

            if name in skip_callers_procs:
                log.info("proc_skipped_by_dep_gate",
                         proc=name,
                         reason="SKIP_CALLERS dependency resolution")
                continue

            strat_ent = strategy_map.get(name)
            strategy  = strat_ent.strategy if strat_ent else ConversionStrategy.TRINO_SQL
            rationale = strat_ent.rationale if strat_ent else ""
            is_shared = strat_ent.shared_utility if strat_ent else False

            proc_chunk_plan = chunk_plan.procs.get(name)
            if not proc_chunk_plan:
                continue

            raw_unresolved = proc_entry.unresolved_deps or []
            filtered_unresolved, resolved_context = self._build_dep_resolution_context(
                raw_unresolved, dep_resolution,
            )

            enriched_chunks: list[ChunkInfo] = []
            cumulative_state_vars: dict[str, str] = {}

            for idx, chunk in enumerate(proc_chunk_plan.chunks):
                body_fns = proc_entry.dialect_functions_in_body or []
                hints    = construct_map.hint_for_chunk(body_fns, strategy=strategy.value)
                chunk_tables = list(chunk.schema_context.keys()) if chunk.schema_context else []

                enriched = ChunkInfo(
                    chunk_id=chunk.chunk_id,
                    start_line=chunk.start_line,
                    end_line=chunk.end_line,
                    line_count=chunk.line_count,
                    tables=chunk_tables,
                    state_vars=cumulative_state_vars.copy(),
                    schema_context=chunk.schema_context or {},
                    construct_hints=hints,
                )
                enriched_chunks.append(enriched)
                cumulative_state_vars.update(chunk.state_vars or {})

            proc_plan = ProcPlan(
                proc_name=proc_entry.name,
                source_file=proc_entry.source_file,
                strategy=strategy,
                strategy_rationale=rationale + (f" | Replan notes: {replan_notes}" if replan_notes else ""),
                chunks=enriched_chunks,
                loop_guards=loop_guards,
                shared_utility=is_shared,
                unresolved_deps=filtered_unresolved,
                dep_resolution_context=resolved_context,
                calls=proc_entry.calls or [],
                tables_written=proc_entry.tables_written or [],
                output_table=dialect_profile.proc_io_tables.get(
                    name, {}
                ).get("output_table", ""),
            )
            procs[name] = proc_plan

        # Respect topological order from Analysis Agent
        order = conv_order.order
        order = [n for n in order if n not in skip_callers_procs]
        shared = [n for n in order if procs.get(n) and procs[n].shared_utility]
        rest   = [n for n in order if n not in set(shared)]
        final_order = shared + rest

        return Plan(
            procs=procs,
            conversion_order=final_order,
            run_id=run_id,
        )

    # =========================================================================
    # Targeted replan (single proc)
    # =========================================================================

    def _targeted_replan(
        self,
        proc_name:    str,
        replan_notes: str,
        manifest:     Manifest,
        complexity:   ComplexityReport,
        construct_map: ConstructMap,
        conv_order:   ConversionOrder,
        adapter:      DialectAdapter,
        table_registry: TableRegistry,
        run_id:       str,
    ) -> PlanningAgentOutput:
        """
        Re-plan only one proc/module. Load existing plan.json and replace the
        entry for proc_name with a freshly planned version.

        v7: When module_grouping is active, proc_name is a MODULE name.
        We resolve it to its member source procs via the existing plan,
        then rebuild the module's strategy and chunks from those procs.
        """
        log.info("targeted_replan_start", proc=proc_name, notes=replan_notes)

        # Load existing plan
        existing_plan_data = self.store.read("planning", "plan.json")
        existing_plan      = Plan(**existing_plan_data)

        # v7: Resolve module name → source procs if this is a module
        existing_entry = existing_plan.get(proc_name)
        source_proc_names = (
            existing_entry.effective_source_procs
            if existing_entry else [proc_name]
        )

        # Look up all member procs in the manifest
        member_procs = [manifest.get_proc(pn) for pn in source_proc_names]
        member_procs = [p for p in member_procs if p is not None]

        if not member_procs:
            raise ValueError(
                f"Replan target '{proc_name}' — none of its source procs "
                f"{source_proc_names} found in manifest"
            )

        # For single-proc (legacy) or single-member module, use the proc directly
        # For multi-member module, we build combined context
        primary_proc = member_procs[0]

        # v6-fix (C3.1-C3.4): Load failure context from previous attempt
        replan_request = self._load_replan_request(proc_name)
        failure_context_str = self._build_replan_failure_context(
            replan_request, replan_notes, existing_entry,
        )

        # P1: Re-select strategy — build combined context for all member procs
        strategy_contexts = []
        for proc_entry in member_procs:
            comp_entry = complexity.get(proc_entry.name)
            body_fns   = proc_entry.dialect_functions_in_body or []
            coverage   = construct_map.hint_for_chunk(body_fns)
            unmapped   = sum(1 for v in coverage.values() if "UNMAPPED" in v)

            single_ctx = ProcStrategyContext(
                name=proc_entry.name,
                line_count=proc_entry.line_count,
                complexity=comp_entry.score if comp_entry else ComplexityScore.MEDIUM,
                complexity_rationale=(comp_entry.rationale if comp_entry else "")
                                      + (f" | Replan notes: {replan_notes}" if replan_notes else ""),
                tables_read=proc_entry.tables_read,
                tables_written=proc_entry.tables_written,
                has_cursor=proc_entry.has_cursor,
                has_dynamic_sql=proc_entry.has_dynamic_sql,
                has_unsupported=proc_entry.has_unsupported,
                unmapped_count=unmapped,
                construct_coverage=coverage,
                shared_utility_candidate=comp_entry.shared_utility_candidate if comp_entry else False,
                calls=proc_entry.calls,
            )
            strategy_contexts.append(single_ctx)

        _, messages = build_llm_strategy_input(
            proc_contexts=strategy_contexts,
            shared_utility_threshold=self.cfg.shared_utility_min_callers,
            replan_context=failure_context_str,
        )
        log.info("replan_p1_llm_calling", proc=proc_name,
                 member_procs=len(member_procs),
                 has_failure_context=bool(failure_context_str))
        raw = self.llm.call(messages=messages, expect_json=True)
        new_strategy_map = parse_llm_strategy_output(raw)

        # P2: Re-compute chunks for member procs
        single_manifest = type(manifest)(
            procs=member_procs,
            source_files=manifest.source_files,
            dialect_id=manifest.dialect_id,
        )

        # v7: If this is a module, rebuild module-level chunks
        # Reconstruct a mini ModuleGroupingPlan for just this module
        mini_module_grouping = None
        if existing_entry and existing_entry.is_module:
            mini_module_grouping = ModuleGroupingPlan(
                modules=[ModuleGroupEntry(
                    module_name=proc_name,
                    description=existing_entry.module_description,
                    source_procs=source_proc_names,
                    output_tables=existing_entry.tables_written,
                )],
                execution_order=[proc_name],
            )

        new_chunk_plan = self._p2_compute_chunks(
            single_manifest, new_strategy_map, adapter, mini_module_grouping,
        )
        new_chunk_plan = self._p3_fetch_schemas(new_chunk_plan, table_registry)

        # P4: Build new proc plan and inject into existing plan
        dialect_profile = self._load_dialect_profile()
        dep_resolution = self._load_dependency_resolution()
        new_plan = self._p4_assemble_plan(
            manifest=single_manifest,
            strategy_map=new_strategy_map,
            chunk_plan=new_chunk_plan,
            construct_map=construct_map,
            conv_order=conv_order,
            dialect_profile=dialect_profile,
            run_id=run_id,
            replan_notes=replan_notes,
            dep_resolution=dep_resolution,
            module_grouping=mini_module_grouping,
        )

        # Replace the entry in existing plan (keyed by module/proc name)
        if proc_name in new_plan.procs:
            existing_plan.procs[proc_name] = new_plan.procs[proc_name]
        self.store.write("planning", "plan.json", existing_plan.model_dump())

        log.info("targeted_replan_complete", proc=proc_name)
        return PlanningAgentOutput(
            plan=existing_plan,
            strategy_map=new_strategy_map,
            chunk_plan=new_chunk_plan,
        )

    # =========================================================================
    # Artifact loaders
    # =========================================================================

    def _load_manifest(self) -> Manifest:
        data = self.store.read("analysis", "manifest.json")
        return Manifest(**data)

    def _load_complexity_report(self) -> ComplexityReport:
        data = self.store.read("analysis", "complexity_report.json")
        return ComplexityReport(**data)

    def _load_construct_map(self) -> ConstructMap:
        data = self.store.read("analysis", "construct_map.json")
        return ConstructMap(**data)

    def _load_conversion_order(self) -> ConversionOrder:
        data = self.store.read("analysis", "conversion_order.json")
        return ConversionOrder(**data)

    def _load_dialect_profile(self) -> DialectProfile:
        data = self.store.read("analysis", "dialect_profile.json")
        return DialectProfile(**data)

    def _load_table_registry(self) -> TableRegistry:
        data = self.store.read("analysis", "table_registry.json")
        return TableRegistry(**data)

    def _load_semantic_map(self):
        """
        Load A1.5 Pipeline Semantic Map (if available).
        Returns PipelineSemanticMap or None if not found (graceful degradation).
        """
        try:
            if self.store.exists("analysis", "semantic_map.json"):
                data = self.store.read("analysis", "semantic_map.json")
                from sql_migration.models.semantic import PipelineSemanticMap
                sm = PipelineSemanticMap(**data)
                if sm.procs_analyzed > 0:
                    log.info("semantic_map_loaded",
                             procs=sm.procs_analyzed,
                             stages=len(sm.stages))
                    return sm
                log.info("semantic_map_empty", msg="A1.5 produced empty map — using structural only")
        except Exception as e:
            log.info("semantic_map_not_available",
                     error=str(e)[:100],
                     msg="Planning proceeds without semantic context")
        return None

    def _load_adapter(self, adapter_path: str) -> DialectAdapter:
        full_path = (
            Path(self.global_cfg.paths.adapters_dir) / Path(adapter_path).name
        )
        if not full_path.exists():
            full_path = Path(adapter_path)
        if not full_path.exists():
            full_path = Path(self.global_cfg.paths.adapters_dir) / "generic.json"
        return DialectAdapter(**json.loads(full_path.read_text(encoding="utf-8")))

    def _load_dependency_resolution(self) -> dict:
        """
        Load Dependency Gate resolution artifact (v6-fix).

        Returns the full resolution dict with keys:
          - stub_deps: list[str] — treat as TODO
          - skip_deps: list[str] — skip all callers
          - inline_bodies: dict[str, str] — actual UDF source code
          - uploaded_files: list[str] — files added via upload
          - deps: list[dict] — full dep info with resolution per dep

        Returns empty dict if no resolution file exists (backward compatible).
        """
        try:
            return self.store.read("analysis", "dependency_resolution.json")
        except (FileNotFoundError, KeyError):
            return {}

    def _build_dep_resolution_context(
        self,
        unresolved_deps: list[str],
        dep_resolution: dict,
    ) -> tuple[list[str], dict[str, str]]:
        """
        Apply Dependency Gate resolution to a proc's unresolved deps (v6-fix).

        For each dep in unresolved_deps:
          - INLINE_BODY: remove from unresolved list (it's now known),
            add body to context so C2 can translate it
          - STUB_TODO: keep in unresolved list, add explicit TODO instruction
          - SKIP_CALLERS: handled at plan level (not per-dep)
          - No resolution: keep as-is with generic instruction

        Returns:
            (filtered_unresolved_deps, dep_resolution_context)
        """
        if not dep_resolution:
            return unresolved_deps, {}

        stub_deps = set(dep_resolution.get("stub_deps", []))
        inline_bodies = dep_resolution.get("inline_bodies", {})

        filtered_deps: list[str] = []
        context: dict[str, str] = {}

        for dep_name in unresolved_deps:
            if dep_name in inline_bodies:
                # INLINE_BODY: dep is now resolved — LLM gets the actual body
                body = inline_bodies[dep_name]
                context[dep_name] = (
                    f"UDF DEFINITION (provided by developer):\n"
                    f"{body}\n"
                    f"Translate this function alongside the proc code. "
                    f"Do NOT emit a TODO marker for this function."
                )
                # Do NOT add to filtered_deps — it's no longer unresolved
            elif dep_name in stub_deps:
                # STUB_TODO: explicit instruction to emit TODO
                filtered_deps.append(dep_name)
                context[dep_name] = (
                    f"UNKNOWN UDF: {dep_name} — definition not available. "
                    f"Do NOT guess its behavior. Emit: "
                    f"# TODO: UNMAPPED — original: {dep_name}(...) "
                    f"— UDF definition unknown"
                )
            else:
                # No resolution or unknown — keep as unresolved with generic instruction
                filtered_deps.append(dep_name)
                context[dep_name] = (
                    f"UNRESOLVED: {dep_name} — not found in any source file. "
                    f"Emit: # TODO: UNMAPPED — original: {dep_name}(...) "
                    f"— UDF definition unknown"
                )

        return filtered_deps, context

    # =========================================================================
    # Replan failure context (v6-fix: C3.1–C3.4)
    # =========================================================================

    def _load_replan_request(self, proc_name: str) -> dict:
        """
        Load replan_request_{proc}.json written by the Orchestrator.

        Contains: failure_context (errors, error_tracker_summary),
        failed_code, validation_report, human_notes, replan_count.

        Returns empty dict if file not found (e.g. human-initiated replan
        without a prior automatic failure).
        """
        try:
            return self.store.read(
                "orchestrator", f"replan_request_{proc_name}.json"
            )
        except (FileNotFoundError, KeyError):
            return {}

    def _build_replan_failure_context(
        self,
        replan_request: dict,
        replan_notes: str,
        previous_plan_entry=None,
    ) -> str:
        """
        Build a structured failure context string for injection into the
        P1 replan LLM prompt.

        Combines:
          - Previous strategy that failed (from existing plan)
          - Failure type and specific errors (from replan_request)
          - Error tracker summary: dominant error signature + count
          - Validation report: fail_reason, missing columns
          - Failed code snippet (first 20 lines — enough to see the pattern)
          - Human notes (from feedback resolution)

        Returns empty string if no failure context available (first plan,
        or human-initiated replan with no prior failure).
        """
        parts: list[str] = []

        # Previous strategy
        if previous_plan_entry:
            prev_strategy = getattr(previous_plan_entry, "strategy", None)
            if prev_strategy:
                strategy_val = prev_strategy.value if hasattr(prev_strategy, "value") else str(prev_strategy)
                parts.append(f"PREVIOUS STRATEGY: {strategy_val}")

        if not replan_request and not replan_notes:
            return ""

        # Replan count
        replan_count = replan_request.get("replan_count", 0)
        if replan_count:
            parts.append(f"REPLAN ATTEMPT: {replan_count}")

        # Failure context
        fc = replan_request.get("failure_context", {})
        if fc:
            failure_type = fc.get("failure_type", "UNKNOWN")
            parts.append(f"FAILURE TYPE: {failure_type}")

            errors = fc.get("errors", [])
            if errors:
                parts.append("ERRORS FROM PREVIOUS ATTEMPT:")
                for i, err in enumerate(errors[:5]):
                    parts.append(f"  {i+1}. {str(err)[:200]}")

            tracker = fc.get("error_tracker_summary", {})
            if tracker:
                dom_sig = tracker.get("dominant_signature", "")
                dom_count = tracker.get("dominant_count", 0)
                if dom_sig:
                    parts.append(
                        f"DOMINANT ERROR (appeared {dom_count}x): {dom_sig[:150]}"
                    )

        # Validation report
        val_report = replan_request.get("validation_report")
        if val_report:
            fail_reason = val_report.get("fail_reason", "")
            if fail_reason:
                parts.append(f"VALIDATION FAILURE: {fail_reason[:200]}")

            # Missing columns — the #1 error class
            col_diffs = val_report.get("column_diffs", [])
            missing_cols = [
                d.get("column", "?") for d in col_diffs
                if d.get("status") == "MISSING_IN_TRINO"
            ]
            if missing_cols:
                parts.append(
                    f"MISSING COLUMNS IN TRINO: {missing_cols[:10]}"
                )

        # Failed code snippet (first 20 lines — shows the pattern to avoid)
        failed_code = replan_request.get("failed_code", "")
        if failed_code:
            snippet = "\n".join(failed_code.splitlines()[:20])
            parts.append(
                f"FAILED CODE (first 20 lines — avoid this pattern):\n{snippet}"
            )

        # Human notes
        human_notes = replan_request.get("human_notes", "") or replan_notes
        if human_notes:
            parts.append(f"DEVELOPER NOTES: {human_notes}")

        if not parts:
            return ""

        return "\n".join(parts)