"""
planning.py
===========
Pydantic I/O schemas for the Planning Agent.

Agent steps: P1 (strategy selection) → P2 (chunk boundaries) → P3 (schema fetch)
             → P4 (plan assembly with loop guards)

LLM I/O functions:
  - build_llm_strategy_input()
  - parse_llm_strategy_output()
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from sql_migration.models.common import (
    ChunkInfo,
    ComplexityScore,
    ConversionStrategy,
    LoopGuards,
)


# ---------------------------------------------------------------------------
# P1 — LLM strategy selection: INPUT builder
# ---------------------------------------------------------------------------

class ProcStrategyContext(BaseModel):
    """Per-proc context sent to LLM for strategy selection. No raw code."""
    name:               str
    line_count:         int
    complexity:         ComplexityScore
    complexity_rationale: str           = ""
    tables_read:        list[str]       = Field(default_factory=list)
    tables_written:     list[str]       = Field(default_factory=list)
    has_cursor:         bool            = False
    has_dynamic_sql:    bool            = False
    has_unsupported:    list[str]       = Field(default_factory=list)
    unmapped_count:     int             = 0
    construct_coverage: dict[str, str]  = Field(default_factory=dict)
    # {fn_name: "COALESCE" | "⚠️ UNMAPPED"}
    shared_utility_candidate: bool      = False
    calls:              list[str]       = Field(default_factory=list)
    # Oracle procedural indicators (safe defaults — DB2 path unaffected)
    has_transaction_control: bool       = False
    has_db_links:            bool       = False
    has_exception_handling:  bool       = False
    has_execute_immediate:   bool       = False
    dbms_call_count:         int        = 0
    package_name:            str | None = None


def build_llm_strategy_input(
    proc_contexts: list[ProcStrategyContext],
    shared_utility_threshold: int,
    replan_context: str = "",
    semantic_context: str = "",
) -> tuple[list[ProcStrategyContext], list[dict]]:
    """
    Build LLM messages for strategy selection.
    LLM receives proc metadata + construct coverage. No raw code.

    Args:
        replan_context: If non-empty, this is a REPLAN — includes failure
                        context from the previous attempt so the LLM can
                        choose a different strategy informed by what went wrong.
        semantic_context: Pipeline Semantic Map context (from A1.5).
                          When available, tells the LLM about business logic,
                          historical patterns, and recommended approaches so
                          strategy selection is informed by what the code DOES,
                          not just what it structurally contains.

    Returns:
        (contexts, messages_list_for_llm_client)
    """
    import json

    system_prompt = (
        "You are selecting the best conversion strategy for SQL procedures. "
        "Output strictly valid JSON."
    )

    # Build replan block only when context is available
    replan_block = ""
    if replan_context:
        replan_block = f"""

⚠️ REPLAN — PREVIOUS ATTEMPT FAILED
This proc has already been attempted with a different strategy that failed.
Use the failure context below to choose a DIFFERENT strategy that avoids
the same failure mode. Do NOT pick the same strategy unless you are certain
the root cause was not strategy-related.

{replan_context}
--- END REPLAN CONTEXT ---
"""

    # Build semantic context block (from A1.5 Pipeline Semantic Map)
    semantic_block = ""
    if semantic_context:
        semantic_block = f"""

=== PIPELINE SEMANTIC ANALYSIS (from A1.5 — business logic understanding) ===
Use this to make INFORMED strategy choices. If the semantic analysis says a
cursor is a historical workaround, prefer TRINO_SQL with a JOIN over PYSPARK_DF
with a collect loop. If it recommends collapsing multiple procs into a single
DataFrame pipeline, prefer PYSPARK_PIPELINE for those procs.

{semantic_context}
--- END SEMANTIC ANALYSIS ---
"""

    user_content = f"""Select the conversion strategy for each procedure.

STRATEGIES:
  TRINO_SQL        : Pure SQL translation. For LOW complexity, simple SELECT/INSERT/UPDATE.
  PYSPARK_DF       : PySpark DataFrame API. For cursor-based row-by-row processing, MEDIUM.
  PYSPARK_PIPELINE : Multi-step PySpark ETL. For complex MEDIUM or HIGH complexity.
  MANUAL_SKELETON  : Scaffold with TODO markers. For NEEDS_MANUAL only.

ESCALATION RULES:
  - unmapped_count > 6 → NEEDS_MANUAL regardless of other factors
  - unmapped_count > 3 → escalate one tier (TRINO_SQL→PYSPARK_DF, PYSPARK_DF→PYSPARK_PIPELINE)
  - shared_utility_candidate=true → prefer TRINO_SQL or PYSPARK_DF (never PYSPARK_PIPELINE)
  - NEEDS_MANUAL complexity → MANUAL_SKELETON always
  - If semantic analysis recommends a specific approach for a proc, PREFER that approach
    over the default escalation rules (the semantic analysis has read the actual code).

PROCEDURAL INDICATOR RULES (Oracle PL/SQL and similar procedural dialects):
  - has_transaction_control=true AND has_exception_handling=true →
    Strongly prefer PYSPARK_PIPELINE (procedural orchestration needs Python control flow)
  - has_db_links=true → Prefer PYSPARK_PIPELINE (external data source abstraction needs Python)
  - has_execute_immediate=true → Prefer PYSPARK_DF or PYSPARK_PIPELINE (dynamic SQL needs Python)
  - dbms_call_count > 0 → Escalate one tier (control-plane constructs need Python wrappers)
  - package_name is set → Prefer grouping with other procs from the same package

Shared utility threshold: a proc called by {shared_utility_threshold}+ others is shared.
{replan_block}{semantic_block}
PROCEDURE CONTEXTS (no raw code):
{json.dumps([c.model_dump() for c in proc_contexts], indent=2)}

OUTPUT FORMAT — strictly valid JSON:
{{
  "strategies": [
    {{
      "name": "proc_name",
      "strategy": "TRINO_SQL|PYSPARK_DF|PYSPARK_PIPELINE|MANUAL_SKELETON",
      "rationale": "one sentence",
      "shared_utility": true/false,
      "estimated_chunks": 1
    }}
  ],
  "shared_utilities": ["proc_name_1", "proc_name_2"],
  "notes": "any cross-proc observations"
}}"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_content},
    ]
    return proc_contexts, messages


# ---------------------------------------------------------------------------
# P1 — LLM strategy selection: OUTPUT parser
# ---------------------------------------------------------------------------

class StrategyEntry(BaseModel):
    name:              str
    strategy:          ConversionStrategy
    rationale:         str  = ""
    shared_utility:    bool = False
    estimated_chunks:  int  = 1


class StrategyMap(BaseModel):
    """
    Per-proc strategy assignments.
    Written to artifacts/02_planning/strategy_map.json.
    """
    strategies:      list[StrategyEntry]   = Field(default_factory=list)
    shared_utilities: list[str]            = Field(default_factory=list)
    notes:           str                   = ""

    def get(self, proc_name: str) -> StrategyEntry | None:
        return next((s for s in self.strategies if s.name == proc_name), None)


def parse_llm_strategy_output(raw_json: dict) -> StrategyMap:
    """Parse and validate LLM strategy selection output."""
    strategies = []
    for item in raw_json.get("strategies", []):
        strategies.append(StrategyEntry(
            name=item["name"],
            strategy=ConversionStrategy(item["strategy"]),
            rationale=item.get("rationale", ""),
            shared_utility=item.get("shared_utility", False),
            estimated_chunks=item.get("estimated_chunks", 1),
        ))
    return StrategyMap(
        strategies=strategies,
        shared_utilities=raw_json.get("shared_utilities", []),
        notes=raw_json.get("notes", ""),
    )


# ---------------------------------------------------------------------------
# P2 — Chunk plan
# ---------------------------------------------------------------------------

class ProcChunkPlan(BaseModel):
    """Chunk breakdown for a single proc."""
    proc_name:  str
    chunks:     list[ChunkInfo] = Field(default_factory=list)
    is_single_chunk: bool       = True  # True if proc fits in one chunk

    @property
    def chunk_count(self) -> int:
        return len(self.chunks)


class ChunkPlan(BaseModel):
    """
    Full chunk plan for all procs.
    Written to artifacts/02_planning/chunk_plan.json.
    """
    procs: dict[str, ProcChunkPlan] = Field(default_factory=dict)

    def get(self, proc_name: str) -> ProcChunkPlan | None:
        return self.procs.get(proc_name)


# ---------------------------------------------------------------------------
# P1.5 — Module grouping (groups related source procs into output modules)
# ---------------------------------------------------------------------------

class SourceProcRange(BaseModel):
    """Line range for one source proc within a module."""
    proc_name:   str
    source_file: str
    start_line:  int
    end_line:    int
    line_count:  int = 0


class ModuleGroupEntry(BaseModel):
    """One module definition from the P1.5 LLM output."""
    module_name:    str
    description:    str                = ""
    source_procs:   list[str]          = Field(default_factory=list)
    output_tables:  list[str]          = Field(default_factory=list)
    rationale:      str                = ""


class ModuleGroupingPlan(BaseModel):
    """
    Full P1.5 output: how source procs are grouped into output modules.
    Written to artifacts/02_planning/module_grouping.json.
    """
    modules:         list[ModuleGroupEntry] = Field(default_factory=list)
    execution_order: list[str]              = Field(default_factory=list)
    notes:           str                    = ""

    def module_for_proc(self, proc_name: str) -> ModuleGroupEntry | None:
        """Find which module contains a given source proc."""
        for m in self.modules:
            if proc_name in m.source_procs:
                return m
        return None

    def get_module(self, module_name: str) -> ModuleGroupEntry | None:
        return next((m for m in self.modules if m.module_name == module_name), None)


def build_llm_module_grouping_input(
    proc_contexts: list[dict],
    dependency_edges: list[tuple[str, str]],
    write_conflict_tables: dict[str, list[str]],
    strategy_map: dict[str, str],
    max_procs_per_module: int = 8,
    merge_write_conflicts: bool = True,
) -> list[dict]:
    """
    Build LLM messages for P1.5 module grouping.

    The LLM receives all proc metadata, the dependency graph, and write conflict
    info. It groups related procs into output modules. Each source proc appears
    in exactly one module. No proc is ever split.

    Args:
        proc_contexts: List of dicts with proc metadata (name, tables_read,
                       tables_written, calls, complexity, line_count, strategy).
        dependency_edges: (caller, callee) edges from the dependency graph.
        write_conflict_tables: {table: [proc1, proc2, ...]} for shared-write tables.
        strategy_map: {proc_name: strategy_value} from P1.
        max_procs_per_module: Hard cap on module size.
        merge_write_conflicts: If true, instruct LLM to put write-conflict procs together.

    Returns:
        messages_list_for_llm_client
    """
    import json

    system_prompt = (
        "You are designing the module architecture for a SQL-to-PySpark migration. "
        "Group related stored procedures into cohesive output modules. "
        "Output strictly valid JSON."
    )

    write_conflict_block = ""
    if write_conflict_tables and merge_write_conflicts:
        wc_lines = ["WRITE CONFLICT TABLES (procs writing to the same table):"]
        for tbl, writers in write_conflict_tables.items():
            wc_lines.append(f"  {tbl}: {writers}")
        wc_lines.append(
            "RULE: Procs that write to the same table SHOULD be in the same module "
            "to avoid cross-module write coordination issues."
        )
        write_conflict_block = chr(10).join(wc_lines)

    user_content = f"""Group these source SQL procedures into output modules.

HARD CONSTRAINTS:
1. Every source proc appears in EXACTLY ONE module. Never split a proc across modules.
2. Maximum {max_procs_per_module} procs per module.
3. If proc A calls proc B, they CAN be in the same module OR B's module must come
   before A's module in execution_order.
4. Module names must be valid Python identifiers (snake_case, no spaces).

GROUPING GUIDELINES:
- Group procs that share the same output tables (they form a logical pipeline).
- Group procs that have caller→callee relationships (reduces cross-module interfaces).
- Group procs by data flow stage (extraction, transformation, loading).
- Keep shared utility procs (called by many others) in their own small module.
- Prefer fewer, larger modules over many small ones (reduces inter-module complexity).

{write_conflict_block}

DEPENDENCY GRAPH (caller → callee):
{json.dumps(dependency_edges, indent=2)}

STRATEGY ASSIGNMENTS (from P1):
{json.dumps(strategy_map, indent=2)}

SOURCE PROCEDURES:
{json.dumps(proc_contexts, indent=2)}

OUTPUT FORMAT — strictly valid JSON:
{{
  "modules": [
    {{
      "module_name": "snake_case_name",
      "description": "One sentence: what this module does in the pipeline",
      "source_procs": ["proc_a", "proc_b", "proc_c"],
      "output_tables": ["table_written_by_this_module"],
      "rationale": "Why these procs belong together"
    }}
  ],
  "execution_order": ["module_1", "module_2", "module_3"],
  "notes": "Any cross-module observations"
}}"""

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_content},
    ]


def parse_llm_module_grouping_output(raw_json: dict) -> ModuleGroupingPlan:
    """Parse and validate LLM module grouping output."""
    modules = []
    for item in raw_json.get("modules", []):
        modules.append(ModuleGroupEntry(
            module_name=item["module_name"],
            description=item.get("description", ""),
            source_procs=item.get("source_procs", []),
            output_tables=item.get("output_tables", []),
            rationale=item.get("rationale", ""),
        ))
    return ModuleGroupingPlan(
        modules=modules,
        execution_order=raw_json.get("execution_order", []),
        notes=raw_json.get("notes", ""),
    )


# ---------------------------------------------------------------------------
# P4 — Final plan (master conversion plan)
# ---------------------------------------------------------------------------

class ProcPlan(BaseModel):
    """
    Complete conversion plan for a single conversion unit (proc or module).

    When module_grouping is enabled, proc_name is the MODULE name and
    source_procs lists the original stored procedures merged into this module.
    When disabled (or single-proc module), source_procs is empty or [proc_name]
    and the behavior is identical to the pre-module-grouping version.

    Contains everything the Conversion Agent needs — no further lookups required.
    """
    proc_name:       str
    source_file:     str
    strategy:        ConversionStrategy
    strategy_rationale: str              = ""
    chunks:          list[ChunkInfo]     = Field(default_factory=list)
    loop_guards:     LoopGuards          = Field(default_factory=LoopGuards)
    shared_utility:  bool                = False

    # ── v7: Module grouping fields ───────────────────────────────────────────
    # When module_grouping is active, a ProcPlan represents a MODULE that
    # contains 1+ source procs merged into a single output file.
    # source_procs: original proc names in this module (empty = legacy single-proc)
    source_procs:     list[str]          = Field(default_factory=list)
    # source_ranges: line boundaries for each source proc (for C1 extraction)
    source_ranges:    list[SourceProcRange] = Field(default_factory=list)
    # module_description: LLM-generated description of what this module does
    module_description: str              = ""

    @property
    def is_module(self) -> bool:
        """True if this plan entry represents a multi-proc module."""
        return len(self.source_procs) > 1

    @property
    def effective_source_procs(self) -> list[str]:
        """Source proc names — falls back to [proc_name] for single-proc plans."""
        return self.source_procs if self.source_procs else [self.proc_name]

    # ── Existing fields (unchanged) ──────────────────────────────────────────

    # Calls to functions/procs not found in any source file (unknown UDFs).
    unresolved_deps:  list[str]          = Field(default_factory=list)

    # v6-fix: Enriched context per unresolved dep from Dependency Gate resolution.
    dep_resolution_context: dict[str, str] = Field(default_factory=dict)

    # Calls to other procs/modules in the manifest.
    calls:            list[str]          = Field(default_factory=list)

    # Tables this module writes to (union across all source procs).
    tables_written:   list[str]          = Field(default_factory=list)

    # v9: Declared output table from README proc_io_tables (used by validation V4).
    output_table:     str                = ""

    # v5: Write directives for shared-write tables (from WriteSemanticsAnalyzer).
    write_directives: dict[str, str]     = Field(default_factory=dict)

    # v10: Extra semantic context for conversion prompt (SSIS transformation chain, etc.)
    semantic_context: str                = ""

    # Set dynamically during Orchestrator dispatch
    prior_state_vars: dict[str, str]     = Field(default_factory=dict)


class Plan(BaseModel):
    """
    Master conversion plan.
    Written to artifacts/02_planning/plan.json.
    The Orchestrator's primary input.

    When module_grouping is enabled, `procs` is keyed by MODULE name
    and `conversion_order` lists module names. The Orchestrator dispatches
    modules exactly as it previously dispatched procs — the interface is
    identical.
    """
    procs:            dict[str, ProcPlan]    = Field(default_factory=dict)
    conversion_order: list[str]              = Field(default_factory=list)
    run_id:           str                    = ""
    # v7: Persisted module grouping for traceability (None if grouping disabled)
    module_grouping:  ModuleGroupingPlan | None = None

    def get(self, name: str) -> ProcPlan | None:
        return self.procs.get(name)

    def total_chunks(self) -> int:
        return sum(len(p.chunks) for p in self.procs.values())

    def total_procs(self) -> int:
        """Number of conversion units (modules or procs)."""
        return len(self.procs)

    def total_source_procs(self) -> int:
        """Number of original source procedures across all modules."""
        count = 0
        for p in self.procs.values():
            count += len(p.effective_source_procs)
        return count

    def write_conflicts(self) -> dict[str, list[str]]:
        """
        Find tables written by more than one conversion unit.
        When module grouping is active, write conflicts within a module are
        already resolved by grouping — this only surfaces CROSS-module conflicts.
        """
        from collections import defaultdict
        table_writers: dict[str, list[str]] = defaultdict(list)
        for proc_name, plan in self.procs.items():
            for tbl in plan.tables_written:
                table_writers[tbl].append(proc_name)
        return {tbl: writers for tbl, writers in table_writers.items()
                if len(writers) > 1}

    def find_module_for_proc(self, proc_name: str) -> str | None:
        """Given an original source proc name, find which plan entry contains it."""
        for name, plan in self.procs.items():
            if proc_name in plan.effective_source_procs:
                return name
        return None


# ---------------------------------------------------------------------------
# Planning Agent I/O (top-level)
# ---------------------------------------------------------------------------

class PlanningAgentInput(BaseModel):
    """Input to the Planning Agent."""
    manifest_path:          str
    complexity_report_path: str
    construct_map_path:     str
    conversion_order_path:  str
    run_id:                 str
    # For targeted replan (single module/proc only):
    replan_proc_name:       str | None = None
    replan_notes:           str        = ""


class PlanningAgentOutput(BaseModel):
    """Output of the Planning Agent."""
    plan:             Plan
    strategy_map:     StrategyMap
    chunk_plan:       ChunkPlan
    module_grouping:  ModuleGroupingPlan | None = None
