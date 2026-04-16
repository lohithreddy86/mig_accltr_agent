"""
analysis.py
===========
Pydantic I/O schemas for the Analysis Agent.

Agent steps: A1 (extraction) → A2 (table resolution) → A3 (dependency graph)
             → A4 (deep extraction) → A5 (LLM complexity scoring)

LLM I/O functions:
  - build_llm_complexity_input()
  - parse_llm_complexity_output()
"""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator

from sql_migration.models.common import (
    ColumnInfo,
    ComplexityScore,
    TableRegistryEntry,
    TableStatus,
)


# ---------------------------------------------------------------------------
# A1 — Structural extraction output
# ---------------------------------------------------------------------------

class ProcEntry(BaseModel):
    """
    One stored procedure/function extracted from source files.
    Populated by A1 (generic extraction), enriched by A4 (deep extraction).
    """
    # Identity
    name:           str
    source_file:    str
    start_line:     int
    end_line:       int
    line_count:     int

    # Tables referenced
    tables_read:     list[str] = Field(default_factory=list)
    tables_written:  list[str] = Field(default_factory=list)

    # Calls
    calls:           list[str] = Field(default_factory=list)   # Other proc names called
    unresolved_deps: list[str] = Field(default_factory=list)   # Called but not in manifest

    # Flags (set by generic extractor using adapter patterns)
    has_cursor:             bool = False
    has_dynamic_sql:        bool = False
    has_exception_handler:  bool = False
    has_unsupported:        list[str] = Field(default_factory=list)  # Which unsupported constructs

    # Deep extraction detail (A4 — only for HIGH/NEEDS_MANUAL candidates)
    cursor_nesting_depth:       int | None   = None
    dynamic_sql_patterns_found: list[str]    = Field(default_factory=list)
    unsupported_occurrences:    dict[str, int] = Field(default_factory=dict)

    # Construct coverage (set by Planning Agent before strategy selection)
    dialect_functions_in_body: list[str] = Field(default_factory=list)
    unmapped_count:            int        = 0

    # v10 Task 2: LLM-suggested natural split point for chunking
    chunking_hint:             str        = ""

    # Oracle procedural indicators (set by extract_structure.py for Oracle adapter)
    # All have safe defaults — existing DB2/SQL Server artifacts remain valid
    has_transaction_control:    bool       = False   # COMMIT/ROLLBACK detected
    has_db_links:               bool       = False   # @dblink references found
    has_exception_handling:     bool       = False   # EXCEPTION WHEN OTHERS
    has_execute_immediate:      bool       = False   # EXECUTE IMMEDIATE
    dbms_calls:                 list[str]  = Field(default_factory=list)  # DBMS_SCHEDULER etc.
    db_link_refs:               list[str]  = Field(default_factory=list)  # @Dbloans, @lnk_dbmis
    oracle_hints:               list[str]  = Field(default_factory=list)  # /*+ APPEND +*/
    package_name:               str | None = None    # Package body container name

    @property
    def all_tables(self) -> list[str]:
        return list(set(self.tables_read + self.tables_written))

    @property
    def needs_deep_extract(self) -> bool:
        from sql_migration.core.config_loader import get_config
        cfg = get_config().analysis.deep_extract_triggers
        return (
            self.line_count >= cfg.min_line_count
            or (cfg.has_dynamic_sql and self.has_dynamic_sql)
            or len(self.has_unsupported) >= cfg.has_unsupported_min
        )


# ---------------------------------------------------------------------------
# A2 — Per-proc LLM analysis output (v10 Task 2)
# ---------------------------------------------------------------------------

class TableRef(BaseModel):
    """
    One table reference found in a proc body, classified by the A2 LLM.
    The LLM reads the actual SQL and distinguishes real tables from CTEs,
    cursor names, variables, and views — which regex cannot do.
    """
    name:           str                                    # Table name as it appears in code
    classification: str = "REAL_TABLE"                     # REAL_TABLE | CTE_ALIAS | CURSOR_NAME | VARIABLE | VIEW | TEMP_TABLE
    used_as:        str = "read"                           # read | write | both
    notes:          str = ""                               # e.g. "dynamic SQL target — actual table unknown"


class PerProcAnalysis(BaseModel):
    """
    Structured output from one A2 per-proc LLM call.
    Contains everything the LLM discovered by reading the full proc body.
    Merged into final artifacts by A4 synthesis.
    """
    # Identity (echoed from proc_inventory)
    name:               str
    source_file:        str    = ""
    start_line:         int    = 0
    end_line:           int    = 0
    line_count:         int    = 0

    # Table references — classified by LLM
    tables:             list[TableRef]          = Field(default_factory=list)

    # Calls to other procs/functions
    calls:              list[str]               = Field(default_factory=list)
    unresolved_deps:    list[str]               = Field(default_factory=list)

    # Flags
    has_cursor:             bool     = False
    has_dynamic_sql:        bool     = False
    has_exception_handler:  bool     = False
    has_unsupported:        list[str] = Field(default_factory=list)

    # Dialect functions found in body (from adapter list)
    dialect_functions_in_body: list[str] = Field(default_factory=list)

    # Complexity assessment
    complexity_score:   str  = "LOW"     # LOW | MEDIUM | HIGH | NEEDS_MANUAL
    complexity_rationale: str = ""

    # Semantic understanding
    business_purpose:   str  = ""
    pipeline_stage:     str  = ""        # extraction | transformation | loading | utility
    key_business_rules: list[str] = Field(default_factory=list)
    recommended_pyspark_approach: str = ""

    # Chunking hint (for Planning P2)
    chunking_hint:      str  = ""        # e.g. "Natural split at line 180 after COMMIT"

    # Column-level function analysis (for conversion precision)
    # Each dict: {column, table, source_type, functions_applied, expression, trino_equivalent}
    column_functions:   list[dict] = Field(default_factory=list)

    # Summary for rolling context
    summary:            str  = ""        # Compact summary for next proc's context

    @property
    def real_tables_read(self) -> list[str]:
        return [t.name for t in self.tables
                if t.classification == "REAL_TABLE" and t.used_as in ("read", "both")]

    @property
    def real_tables_written(self) -> list[str]:
        return [t.name for t in self.tables
                if t.classification == "REAL_TABLE" and t.used_as in ("write", "both")]

    @property
    def all_real_tables(self) -> list[str]:
        return sorted(set(t.name for t in self.tables if t.classification == "REAL_TABLE"))

    @property
    def temp_tables(self) -> list[str]:
        return [t.name for t in self.tables if t.classification == "TEMP_TABLE"]


# ---------------------------------------------------------------------------
# A3 — Table registry output
# ---------------------------------------------------------------------------

class TableRegistry(BaseModel):
    """
    Registry of all tables referenced across all procs, resolved against Trino.
    Written to artifacts/01_analysis/table_registry.json.
    """
    entries: dict[str, TableRegistryEntry] = Field(default_factory=dict)
    # Key: source table name (as it appears in source SQL)

    def get(self, source_name: str) -> TableRegistryEntry | None:
        return self.entries.get(source_name)

    def exists_in_trino(self, source_name: str) -> bool:
        entry = self.entries.get(source_name)
        return entry is not None and entry.status == TableStatus.EXISTS_IN_TRINO

    def trino_fqn(self, source_name: str) -> str:
        entry = self.entries.get(source_name)
        return entry.trino_fqn if entry else source_name

    def missing_tables(self) -> list[str]:
        return [k for k, v in self.entries.items() if v.status == TableStatus.MISSING]

    def all_trino_tables(self) -> list[str]:
        return [v.trino_fqn for v in self.entries.values()
                if v.status == TableStatus.EXISTS_IN_TRINO]


# ---------------------------------------------------------------------------
# A3 — Dependency graph output
# ---------------------------------------------------------------------------

class DependencyGraph(BaseModel):
    """
    Directed call graph of procedures.
    Written to artifacts/01_analysis/dependency_graph.json.
    """
    edges:        list[tuple[str, str]] = Field(default_factory=list)
    # (caller, callee) — caller depends on callee being converted first
    cycles:       list[list[str]]       = Field(default_factory=list)
    external_deps: dict[str, list[str]] = Field(default_factory=dict)
    # {proc_name: [called_procs_not_in_manifest]}


class ConversionOrder(BaseModel):
    """
    Topologically sorted list of proc names.
    Written to artifacts/01_analysis/conversion_order.json.
    Dependencies appear before dependents.
    """
    order:          list[str] = Field(default_factory=list)
    has_cycles:     bool      = False
    cycle_warning:  str       = ""


# ---------------------------------------------------------------------------
# A5 — LLM complexity scoring: INPUT builder
# ---------------------------------------------------------------------------

class ProcSummaryForLLM(BaseModel):
    """Compact proc summary sent to LLM for complexity scoring. NO raw code."""
    name:                    str
    line_count:              int
    tables_read_count:       int
    tables_written_count:    int
    calls_count:             int
    has_cursor:              bool
    cursor_nesting_depth:    int | None
    has_dynamic_sql:         bool
    dynamic_sql_complexity:  str = ""  # "table_names" | "column_lists" | "full_queries" | ""
    has_exception_handler:   bool
    has_unsupported:         list[str]
    unresolved_deps:         list[str]
    in_cycle:                bool = False


def build_llm_complexity_input(
    procs:                 list[ProcEntry],
    unsupported_constructs: list[str],
    circular_deps:         list[list[str]],
) -> tuple[list[ProcSummaryForLLM], list[dict]]:
    """
    Build LLM messages for complexity scoring.
    LLM receives ONLY metadata summaries — never raw code.

    Returns:
        (summaries, messages_list_for_llm_client)
    """
    cycle_proc_names = {name for cycle in circular_deps for name in cycle}

    summaries = [
        ProcSummaryForLLM(
            name=p.name,
            line_count=p.line_count,
            tables_read_count=len(p.tables_read),
            tables_written_count=len(p.tables_written),
            calls_count=len(p.calls),
            has_cursor=p.has_cursor,
            cursor_nesting_depth=p.cursor_nesting_depth,
            has_dynamic_sql=p.has_dynamic_sql,
            dynamic_sql_complexity=_infer_dyn_complexity(p),
            has_exception_handler=p.has_exception_handler,
            has_unsupported=p.has_unsupported,
            unresolved_deps=p.unresolved_deps,
            in_cycle=p.name in cycle_proc_names,
        )
        for p in procs
    ]

    import json
    system_prompt = (
        "You are scoring SQL procedure migration complexity. "
        "You receive metadata ONLY — no source code. "
        "Output strictly valid JSON."
    )

    user_content = f"""Score the migration complexity of each procedure below.

SCORES: LOW | MEDIUM | HIGH | NEEDS_MANUAL

SCORING RULES:
- Any construct in unsupported_list → NEEDS_MANUAL
- in_cycle = true → NEEDS_MANUAL
- unresolved_deps not empty → HIGH (at minimum)
- cursor_nesting_depth > 2 → HIGH
- has_dynamic_sql AND dynamic_sql_complexity = "table_names" → NEEDS_MANUAL
- has_dynamic_sql AND dynamic_sql_complexity = "full_queries" → NEEDS_MANUAL
- has_dynamic_sql AND dynamic_sql_complexity = "column_lists" → HIGH
- unmapped_count > 6 → NEEDS_MANUAL (regardless of other factors)
- unmapped_count > 3 → escalate score one tier
- BULK COLLECT / FORALL in has_unsupported → HIGH
- Simple SELECT/INSERT/UPDATE, no cursors, no dynamic SQL → LOW

ADAPTER UNSUPPORTED CONSTRUCTS (any of these → NEEDS_MANUAL):
{unsupported_constructs}

CIRCULAR DEPENDENCIES (these procs → NEEDS_MANUAL):
{[p for cycle in circular_deps for p in cycle]}

PROCEDURE METADATA (no raw code):
{json.dumps([s.model_dump() for s in summaries], indent=2)}

OUTPUT FORMAT — strictly valid JSON:
{{
  "scores": [
    {{
      "name": "proc_name",
      "score": "LOW|MEDIUM|HIGH|NEEDS_MANUAL",
      "rationale": "one concise sentence",
      "priority_flag": true/false,
      "shared_utility_candidate": true/false
    }}
  ]
}}

Mark shared_utility_candidate=true for procs that appear to be utility helpers
called by many others (short, no DML, few tables).
Mark priority_flag=true for procs that are dependencies of many others."""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_content},
    ]
    return summaries, messages


def _infer_dyn_complexity(proc: ProcEntry) -> str:
    """Infer what is being built dynamically from extracted patterns."""
    patterns = [p.upper() for p in proc.dynamic_sql_patterns_found]
    if any("SELECT" in p or "INSERT" in p for p in patterns):
        return "full_queries"
    if any("COLUMN" in p or "FIELD" in p for p in patterns):
        return "column_lists"
    if patterns:
        return "table_names"
    return ""


# ---------------------------------------------------------------------------
# A5 — LLM complexity scoring: OUTPUT parser
# ---------------------------------------------------------------------------

class ComplexityEntry(BaseModel):
    """Complexity score for a single proc."""
    name:                    str
    score:                   ComplexityScore
    rationale:               str = ""
    priority_flag:           bool = False
    shared_utility_candidate: bool = False


class ComplexityReport(BaseModel):
    """
    Per-proc complexity scores.
    Written to artifacts/01_analysis/complexity_report.json.
    """
    scores: list[ComplexityEntry] = Field(default_factory=list)

    def get(self, proc_name: str) -> ComplexityEntry | None:
        return next((s for s in self.scores if s.name == proc_name), None)

    def by_score(self, score: ComplexityScore) -> list[ComplexityEntry]:
        return [s for s in self.scores if s.score == score]

    def shared_utilities(self) -> list[str]:
        return [s.name for s in self.scores if s.shared_utility_candidate]

    def priority_procs(self) -> list[str]:
        return [s.name for s in self.scores if s.priority_flag]


def parse_llm_complexity_output(raw_json: dict) -> ComplexityReport:
    """Parse and validate the LLM complexity scoring response."""
    scores = []
    for item in raw_json.get("scores", []):
        scores.append(ComplexityEntry(
            name=item["name"],
            score=ComplexityScore(item["score"]),
            rationale=item.get("rationale", ""),
            priority_flag=item.get("priority_flag", False),
            shared_utility_candidate=item.get("shared_utility_candidate", False),
        ))
    return ComplexityReport(scores=scores)


# ---------------------------------------------------------------------------
# Analysis Agent I/O (top-level)
# ---------------------------------------------------------------------------

class PipelineConfig(BaseModel):
    """
    Structured user inputs from Streamlit UI.
    Replaces README.docx parsing — these fields were previously extracted
    from the README by parse_readme.py in sandbox.
    """
    source_type:       str       = "stored_procedures"  # stored_procedures | sql_queries | ssis_package
    output_format:     str       = "AUTO"               # AUTO | TRINO_SQL | PYSPARK
    source_engine:     str       = ""                    # e.g. "Oracle 19c", "DB2 11.5"
    declared_dialect:  str       = ""                    # e.g. "PL/SQL", "T-SQL", "DB2 SQL"
    udf_file_path:     str       = ""                    # path to uploaded .sql/.py/.txt file (optional)
    input_catalog:     str       = ""                    # e.g. "lakehouse"
    input_schemas:     list[str] = Field(default_factory=list)  # e.g. ["core", "staging"]


class AnalysisAgentInput(BaseModel):
    """Input to the Analysis Agent (v10: absorbs Detection setup)."""
    readme_path:     str       = ""            # OPTIONAL — backward compat for CLI
    sql_file_paths:  list[str]
    run_id:          str
    pipeline_config: PipelineConfig | None = None  # If provided, skip README parsing


class Manifest(BaseModel):
    """
    Complete structural index of all source procs.
    Written to artifacts/01_analysis/manifest.json.
    The central artifact — referenced by Planning, Orchestrator, Conversion.
    """
    procs:         list[ProcEntry]          = Field(default_factory=list)
    table_registry: dict[str, dict]         = Field(default_factory=dict)
    source_files:   list[str]               = Field(default_factory=list)
    total_procs:    int                     = 0
    total_lines:    int                     = 0
    dialect_id:     str                     = ""

    @model_validator(mode="after")
    def set_totals(self) -> "Manifest":
        self.total_procs = len(self.procs)
        self.total_lines = sum(p.line_count for p in self.procs)
        return self

    def get_proc(self, name: str) -> ProcEntry | None:
        return next((p for p in self.procs if p.name == name), None)

    def proc_names(self) -> list[str]:
        return [p.name for p in self.procs]


class AnalysisAgentOutput(BaseModel):
    """Output of the Analysis Agent."""
    manifest:          Manifest
    table_registry:    TableRegistry
    dependency_graph:  DependencyGraph
    conversion_order:  ConversionOrder
    complexity_report: ComplexityReport
