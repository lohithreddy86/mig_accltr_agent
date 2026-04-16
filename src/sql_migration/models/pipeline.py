"""
orchestrator.py / conversion.py / validation.py — combined in one file
=======================================================================
Pydantic I/O schemas for the Orchestrator, Conversion, and Validation agents.

These three are tightly coupled through the task/result dispatch cycle:
  Orchestrator writes task_{proc}_{chunk}.json
  Conversion reads it, writes result_{proc}_{chunk}.json + conversion artifacts
  Validation reads conversion artifacts, writes validation_{proc}.json
  Orchestrator reads validation result and updates checkpoint

LLM I/O functions for Conversion:
  - build_llm_conversion_input()
  - build_llm_self_correction_input()

LLM I/O functions for Validation:
  - build_llm_semantic_diff_input()
  - parse_llm_semantic_diff_output()
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from sql_migration.models.common import (
    ChunkInfo,
    ColumnDiffStatus,
    ConversionStrategy,
    ErrorEntry,
    LoopGuards,
    ProcStatus,
    TodoItem,
    ValidationLevel,
    ValidationOutcome,
)


# ===========================================================================
# ORCHESTRATOR
# ===========================================================================

class ProcState(BaseModel):
    """
    Live state of a single proc in the Orchestrator's state machine.
    Written to checkpoint.json after every state transition.
    """
    proc_name:       str
    status:          ProcStatus     = ProcStatus.PENDING
    chunks_done:     list[str]      = Field(default_factory=list)
    current_chunk:   str | None     = None
    retry_count:     int            = 0
    replan_count:    int            = 0
    error_history:   list[ErrorEntry] = Field(default_factory=list)
    started_at:      str            = ""
    completed_at:    str            = ""
    validation_outcome: ValidationOutcome | None = None

    def add_error(self, error_type: str, message: str, chunk_id: str = "",
                  attempt: int = 1, signature: str = "", occurrence: int = 1,
                  error_class: str = "") -> None:
        from datetime import datetime, timezone
        self.error_history.append(ErrorEntry(
            error_type=error_type,
            message=message,
            agent="orchestrator",
            chunk_id=chunk_id,
            attempt=attempt,
            timestamp=datetime.now(timezone.utc).isoformat(),
            signature=signature,
            occurrence=occurrence,
            error_class=error_class,
        ))


class Checkpoint(BaseModel):
    """
    Full pipeline checkpoint.
    Written to artifacts/03_orchestrator/checkpoint.json after every state change.
    """
    run_id:     str
    procs:      dict[str, ProcState] = Field(default_factory=dict)
    started_at: str                  = ""
    updated_at: str                  = ""

    # Pipeline-level counts (for UI display)
    total_procs:     int = 0
    validated_count: int = 0
    frozen_count:    int = 0
    skipped_count:   int = 0
    partial_count:   int = 0

    def refresh_counts(self) -> None:
        self.total_procs     = len(self.procs)
        self.validated_count = sum(1 for p in self.procs.values()
                                   if p.status == ProcStatus.VALIDATED)
        self.frozen_count    = sum(1 for p in self.procs.values()
                                   if p.status == ProcStatus.FROZEN)
        self.skipped_count   = sum(1 for p in self.procs.values()
                                   if p.status == ProcStatus.SKIPPED)
        self.partial_count   = sum(1 for p in self.procs.values()
                                   if p.status == ProcStatus.PARTIAL)

    def is_complete(self) -> bool:
        # FROZEN is treated as terminal by the state machine.
        # The orchestrator loop handles resolution polling separately.
        terminal = {ProcStatus.VALIDATED, ProcStatus.PARTIAL,
                    ProcStatus.FROZEN, ProcStatus.SKIPPED}
        return all(p.status in terminal for p in self.procs.values())

    def pending_procs(self) -> list[str]:
        return [n for n, p in self.procs.items() if p.status == ProcStatus.PENDING]

    def frozen_procs(self) -> list[str]:
        return [n for n, p in self.procs.items() if p.status == ProcStatus.FROZEN]


# ---------------------------------------------------------------------------
# Orchestrator dispatch task
# ---------------------------------------------------------------------------

class DispatchTask(BaseModel):
    """
    Written by Orchestrator to trigger a Conversion Agent run.
    Written to artifacts/03_orchestrator/task_{name}_{chunk}.json

    When module_grouping is active, proc_name is the MODULE name and
    source_procs/source_ranges carry the original proc boundaries.
    """
    proc_name:       str
    chunk_id:        str
    chunk_index:     int
    total_chunks:    int
    source_file:     str
    start_line:      int
    end_line:        int
    strategy:        ConversionStrategy
    schema_context:  dict[str, Any]    = Field(default_factory=dict)
    construct_hints: dict[str, str]    = Field(default_factory=dict)
    state_vars:      dict[str, str]    = Field(default_factory=dict)
    loop_guards:      LoopGuards        = Field(default_factory=LoopGuards)
    retry_number:     int               = 0
    replan_notes:     str               = ""   # From human feedback
    # Calls to functions/procs not found in the source manifest (unknown UDFs, external libs)
    # Forwarded from ProcEntry.unresolved_deps so C3 can detect raw calls to them
    unresolved_deps:  list[str]         = Field(default_factory=list)

    # v6-fix: Enriched context per unresolved dep from Dependency Gate.
    dep_resolution_context: dict[str, str] = Field(default_factory=dict)

    # Callee signature map — keyed by callee proc/module name.
    callee_signatures: dict[str, str]   = Field(default_factory=dict)

    # Pre-written UDF implementations from README Section 3.2.
    # Each dict: {name: str, code: str}
    udf_implementations: list[dict[str, str]] = Field(default_factory=list)

    # v9: External dependency warnings from README Section 7.
    # Injected into C2 prompt so LLM wraps unsupported constructs in TODO.
    external_warnings: list[str] = Field(default_factory=list)

    # v5: Write directives for shared-write tables (from WriteSemanticsAnalyzer).
    write_directives: dict[str, str]    = Field(default_factory=dict)

    # ── v7: Module grouping fields ───────────────────────────────────────────
    # When this task is for a module (multiple source procs), source_procs
    # lists the original proc names and source_ranges gives their line boundaries.
    # The Conversion Agent uses these to extract all procs' code for conversion.
    source_procs:      list[str]        = Field(default_factory=list)
    source_ranges:     list[dict]       = Field(default_factory=list)
    # {proc_name, source_file, start_line, end_line}
    module_description: str             = ""

    # v8: Semantic context from A1.5 Pipeline Semantic Map.
    # Injected per-proc/module by the Orchestrator at dispatch time.
    # Contains: pipeline intent, proc business purpose, data flow,
    # historical patterns, recommended PySpark approach.
    semantic_context:   str             = ""

    @property
    def is_module(self) -> bool:
        return len(self.source_procs) > 1


# ---------------------------------------------------------------------------
# Orchestrator replan request
# ---------------------------------------------------------------------------

class ReplanRequest(BaseModel):
    """
    Written by Orchestrator when a proc needs targeted replanning.
    Written to artifacts/03_orchestrator/replan_request_{proc}.json
    """
    proc_name:       str
    replan_count:    int
    failure_context: dict[str, Any]   = Field(default_factory=dict)
    failed_code:     str              = ""
    validation_report: dict | None    = None
    human_notes:     str              = ""   # From feedback resolution


# ===========================================================================
# CONVERSION AGENT
# ===========================================================================

class ChunkConversionResult(BaseModel):
    """
    Result of converting one chunk — written back to Orchestrator.
    Written to artifacts/04_conversion/result_{proc}_{chunk}.json
    """
    proc_name:      str
    chunk_id:       str
    status:         str                # "SUCCESS" | "FAIL" | "CONVERSION_FAILED"
    converted_code: str                = ""
    errors:         list[str]          = Field(default_factory=list)
    todo_count:     int                = 0
    todo_items:     list[TodoItem]     = Field(default_factory=list)
    updated_state_vars: dict[str, str] = Field(default_factory=dict)
    self_correction_attempts: int      = 0
    duration_s:     float              = 0.0


class ConversionLog(BaseModel):
    """
    Log of the full conversion process for a proc (all chunks).
    Written to artifacts/04_conversion/conversion_log_{proc}.json
    """
    proc_name:    str
    strategy:     ConversionStrategy
    chunks:       list[ChunkConversionResult] = Field(default_factory=list)
    todos:        list[TodoItem]              = Field(default_factory=list)
    warnings:     list[str]                  = Field(default_factory=list)
    total_self_corrections: int              = 0
    assembled:    bool                       = False
    assembly_error: str                      = ""


# ---------------------------------------------------------------------------
# Conversion LLM input/output functions
# ---------------------------------------------------------------------------

def build_llm_conversion_input(
    task:       DispatchTask,
    chunk_code: str,
) -> list[dict]:
    """
    Build LLM messages for chunk conversion.
    This is the primary conversion prompt — all context is injected here.
    LLM never needs to "remember" dialect, schema, or prior state.

    Returns:
        messages_list_for_llm_client
    """
    import json

    # Build unresolved-dep rule only when there are unknown UDFs
    # v6-fix: Use dep_resolution_context for enriched per-dep instructions
    # when available (INLINE_BODY provides actual UDF source, STUB_TODO
    # provides explicit "emit TODO" instruction), falling back to generic
    # rule when no resolution context exists.
    unresolved_rule = ""
    dep_context = getattr(task, 'dep_resolution_context', {}) or {}

    if task.unresolved_deps or dep_context:
        rule_parts = []

        # Deps with enriched resolution context (STUB_TODO with explicit instructions)
        for dep_name in task.unresolved_deps:
            if dep_name in dep_context:
                rule_parts.append(f"    {dep_context[dep_name]}")
            else:
                rule_parts.append(
                    f"    {dep_name}: UNKNOWN UDF — definition not available. "
                    f"Emit: # TODO: UNMAPPED — original: {dep_name}(...) — UDF definition unknown"
                )

        # Deps with INLINE_BODY resolution (no longer in unresolved_deps,
        # but their context is in dep_resolution_context)
        for dep_name, ctx in dep_context.items():
            if dep_name not in task.unresolved_deps:
                # This is an INLINE_BODY dep — provide the actual UDF body
                rule_parts.append(f"    {ctx}")

        if rule_parts:
            unresolved_rule = (
                f"\n3b. EXTERNAL DEPENDENCIES (resolved by developer):\n"
                + chr(10).join(rule_parts)
                + "\n    Do NOT leave raw calls to unknown functions in the output."
            )

    # v7: Module-aware system prompt
    if task.is_module:
        system_prompt = f"""You are converting a MODULE of {len(task.source_procs)} related SQL procedures into a single cohesive {_target_format(task.strategy)} output.

MODULE: {task.proc_name}
DESCRIPTION: {task.module_description or 'Combined module from related source procedures'}
SOURCE PROCS: {', '.join(task.source_procs)}

Rules:
1. Output ONLY valid {_target_format(task.strategy)} code. No markdown. No explanations.
2. Combine the logic of ALL source procedures into one clean, well-structured module.
   Do NOT produce separate functions per source proc unless it improves clarity.
   Merge shared logic. Eliminate redundant variable declarations.
3. Use construct mappings EXACTLY as provided. Do not invent alternatives.
4. For UNMAPPED functions: emit a TODO comment preserving the original call.
   Format: # TODO: UNMAPPED — original: FUNC_NAME(args) — no equivalent known{unresolved_rule}
5. Use schema_context column names exactly as provided — do not guess column names.
6. Do not re-declare variables listed in prior_chunk_variables.
7. This is chunk {task.chunk_index + 1} of {task.total_chunks} for module '{task.proc_name}'."""
    else:
        system_prompt = f"""You are converting a {task.strategy.value} SQL chunk to its target format.

Rules:
1. Output ONLY valid {_target_format(task.strategy)} code. No markdown. No explanations.
2. Use construct mappings EXACTLY as provided. Do not invent alternatives.
3. For UNMAPPED functions: emit a TODO comment preserving the original call.
   Format: # TODO: UNMAPPED — original: FUNC_NAME(args) — no Trino/PySpark equivalent known{unresolved_rule}
4. Use schema_context column names exactly as provided — do not guess column names.
5. Do not re-declare variables listed in prior_chunk_variables.
6. This is chunk {task.chunk_index + 1} of {task.total_chunks} for proc '{task.proc_name}'."""

    # Build callee-context block when this proc calls other converted procs
    callee_block = ""
    if task.callee_signatures:
        lines = [
            "CALLEE PROC SIGNATURES (already converted — call these, do not re-implement):"
        ]
        for callee_name, sig in task.callee_signatures.items():
            if sig.startswith("-- TRINO"):
                lines.append(
                    f"  {callee_name}: this is a Trino SQL proc — "
                    f"invoke it as a separate SQL statement or WITH clause."
                )
            else:
                # Show the def line so the LLM knows the exact callable name + args
                first_line = sig.splitlines()[0] if sig else f"def proc_{callee_name}(spark)"
                lines.append(f"  {callee_name} → {first_line}")
        callee_block = chr(10).join(lines)

    # v5: Build write-directives block for shared-write tables
    write_block = ""
    if task.write_directives:
        w_lines = [
            "WRITE DIRECTIVES (MANDATORY — do NOT choose write modes yourself):",
            "The following tables are written by multiple procs in this pipeline.",
            "You MUST use the exact write mode specified below.",
            "Ignoring these directives WILL cause data loss.",
            "",
        ]
        for table_name, directive in task.write_directives.items():
            w_lines.append(directive)
            w_lines.append("")
        write_block = chr(10).join(w_lines)

    # v5: Build retry context (when orchestrator retries a failed chunk)
    retry_block = ""
    if task.retry_number > 0:
        retry_block = (
            f"\n⚠️ RETRY ATTEMPT {task.retry_number} for this chunk\n"
            f"A previous conversion of this exact chunk FAILED validation.\n"
            f"CRITICAL: Do NOT repeat the same patterns. Take a different approach if needed.\n"
            f"Focus especially on using EXACT column names from TABLE SCHEMA above.\n"
        )

    # v7: Module context block
    module_block = ""
    if task.is_module and task.module_description:
        module_block = (
            f"MODULE CONTEXT:\n"
            f"This chunk is part of module '{task.proc_name}' which combines "
            f"{len(task.source_procs)} source procedures: {', '.join(task.source_procs)}.\n"
            f"Purpose: {task.module_description}\n"
            f"Produce a single cohesive output — not separate functions per source proc.\n"
        )

    # v7: Source SQL section label
    chunk_label = "MODULE SOURCE" if task.is_module else "SQL CHUNK"

    user_content = f"""Convert this SQL chunk.

{module_block}CONSTRUCT MAPPINGS (source dialect → {_target_format(task.strategy)}):
Use these EXACTLY. Do not look up alternatives in your training data.
{json.dumps(task.construct_hints, indent=2) if task.construct_hints else "  (no dialect-specific functions in this chunk)"}

TABLE SCHEMA (exact Trino column names and types):
{json.dumps(task.schema_context, indent=2) if task.schema_context else "  (no tables in this chunk)"}

PRIOR CHUNK VARIABLES (already exist — do NOT re-declare):
{json.dumps(task.state_vars, indent=2) if task.state_vars else "  (first chunk — no prior variables)"}

{callee_block if callee_block else ""}
{write_block if write_block else ""}
{f"REPLAN NOTES (from human review):{chr(10)}{task.replan_notes}" if task.replan_notes else ""}

--- {chunk_label} (lines {task.start_line}–{task.end_line}) ---
{chunk_code}
--- END {chunk_label} ---
{retry_block}
{_build_test_instruction(task)}"""

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_content},
    ]


def _build_test_instruction(task) -> str:
    """
    Build chunk-position-aware test instruction for end of C2 prompt.

    Single-chunk module (chunk 1/1):
        Full module test — code is the complete unit, test covers everything.

    Intermediate chunk (chunk N of M, N < M):
        No test — the chunk is a fragment that can't run in isolation.
        Static checks (parse + remnant) in C3 are sufficient.

    Last chunk (chunk M of M, M > 1):
        Module-level integration test — the LLM has history summaries of
        all prior chunks and knows what the full module does. The test
        covers the complete assembled module, not just this chunk.
        Stored as artifact; C5 can run it against assembled code.
    """
    target = _target_format(task.strategy)
    is_single = (task.total_chunks == 1)
    is_last   = (task.chunk_index == task.total_chunks - 1)

    if is_single:
        # ── Single-chunk module: full test ────────────────────────────────
        return f"""Output TWO sections separated by the exact marker line "--- TEST ---":

SECTION 1 (PRODUCTION CODE): The converted {target} code.
--- TEST ---
SECTION 2 (TEST SCRIPT): A self-contained PySpark test that:
  - Imports SparkSession and creates a local session
  - Creates small synthetic DataFrames (3-5 rows) matching TABLE SCHEMA above
    Include edge cases: at least 1 NULL value, 1 boundary date, 1 zero amount.
  - Registers them as temp views (same names as in TABLE SCHEMA)
  - Executes the production code from SECTION 1
  - Asserts: output schema has expected columns, row count > 0, no runtime errors
  - Prints "TEST_PASS" if all assertions pass
  - Prints "TEST_FAIL: {{reason}}" on any failure
  - Wraps everything in try/except — never raises unhandled exceptions

If strategy is TRINO_SQL, the test should use spark.sql() to run the SQL statements.
The test must be fully self-contained — import everything it needs, create all data."""

    elif is_last:
        # ── Last chunk of multi-chunk module: integration test ────────────
        return f"""Output TWO sections separated by the exact marker line "--- TEST ---":

SECTION 1 (PRODUCTION CODE): The converted {target} code for THIS chunk only.
--- TEST ---
SECTION 2 (MODULE INTEGRATION TEST): A self-contained PySpark test for the
COMPLETE module — all chunks combined, not just this chunk. You have seen
summaries of all prior chunks in the conversation history. Write a test that:
  - Imports SparkSession and creates a local session
  - Creates synthetic DataFrames for ALL tables referenced across ALL chunks
    (not just this chunk's tables). Include edge cases: NULLs, boundary values.
  - Registers them as temp views
  - Executes the complete module logic end-to-end (all chunks' code in sequence)
  - Uses the variables from PRIOR CHUNK VARIABLES — create mock versions with
    realistic schemas matching what the prior chunks would have produced
  - Asserts: final output table has expected columns, row count > 0
  - Prints "TEST_PASS" if all assertions pass
  - Prints "TEST_FAIL: {{reason}}" on any failure
  - Wraps everything in try/except — never raises unhandled exceptions

This test will be run against the ASSEMBLED module (all chunks combined).
It must be fully self-contained — import everything it needs, create all data."""

    else:
        # ── Intermediate chunk: no test, code only ────────────────────────
        return f"Output {target} code only. No test section — this is an intermediate chunk."


# ---------------------------------------------------------------------------
# Code + Test splitter (v7)
# ---------------------------------------------------------------------------

_TEST_SEPARATOR = "--- TEST ---"


def split_code_and_test(llm_output: str) -> tuple[str, str]:
    """
    Split LLM output into production code and test script.

    The C2 prompt instructs the LLM to output:
      SECTION 1: production code
      --- TEST ---
      SECTION 2: test script

    Returns:
        (production_code, test_code)
        If no separator found, returns (full_output, "") — backward compatible.
    """
    if _TEST_SEPARATOR in llm_output:
        parts = llm_output.split(_TEST_SEPARATOR, 1)
        code = parts[0].strip()
        test = parts[1].strip() if len(parts) > 1 else ""
        # Strip markdown fences from each section individually
        code = _strip_fences(code)
        test = _strip_fences(test)
        return code, test

    # No separator found — LLM didn't follow the format.
    # Treat entire output as code (test will be skipped in C3).
    return _strip_fences(llm_output), ""


def _strip_fences(text: str) -> str:
    """Remove ```python / ```sql / ``` fences."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = lines[1:]  # Remove opening fence line
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)
    return text.strip()




# ---------------------------------------------------------------------------
# Prompt budget estimation (Gap 12)
# ---------------------------------------------------------------------------

def estimate_c2_prompt_tokens(
    task,
    chunk_code: str,
) -> int:
    """
    Estimate the total token count of a C2 conversion prompt BEFORE sending.

    Uses the same token counting utility as history compaction (tiktoken
    cl100k_base with fallback to len//4).

    Call this in ConversionAgent._c2_convert() before calling the LLM.
    If the estimate exceeds the model's context budget, trim schema_context
    to only tables referenced in the chunk code.
    """
    # Simple token estimate (word count / 0.75) — no external dependency
    def _count_tokens(s: str) -> int:
        return max(1, int(len(s.split()) / 0.75))
    messages = build_llm_conversion_input(task, chunk_code)
    total = sum(_count_tokens(m.get("content", "")) + 4 for m in messages)
    return total


def trim_schema_to_referenced_tables(
    schema_context: dict[str, Any],
    chunk_code: str,
) -> dict[str, Any]:
    """
    Trim schema_context to only tables actually referenced in the chunk code.

    When the C2 prompt exceeds token budget, the schema_context (which
    contains exact column names/types for ALL tables in the proc) is the
    largest compressible section. Trimming to referenced tables keeps
    the essential information while reducing token count significantly.

    A table is "referenced" if its name (case-insensitive) appears as a
    word boundary in the chunk code. This catches FROM/JOIN/INSERT INTO
    references but may miss dynamic SQL table names — those are a known
    edge case that the LLM handles via TODO markers anyway.
    """
    import re
    code_upper = chunk_code.upper()
    trimmed: dict[str, Any] = {}
    for table_name, table_info in schema_context.items():
        # Check if the source table name or Trino FQN appears in chunk code
        names_to_check = [table_name]
        if isinstance(table_info, dict) and table_info.get("trino_fqn"):
            names_to_check.append(table_info["trino_fqn"])
        for name in names_to_check:
            if re.search(r'\b' + re.escape(name.upper()) + r'\b', code_upper):
                trimmed[table_name] = table_info
                break
    return trimmed


# ---------------------------------------------------------------------------
# Callee interface model (Gap 4)
# ---------------------------------------------------------------------------

class CalleeInterface(BaseModel):
    """
    Structured interface description for a converted callee proc.

    Replaces the fragile 4-line def extraction with a machine-readable
    interface that captures everything a caller needs to know:
      - How to call it (params, return type)
      - What data it touches (tables read/written)
      - What strategy it uses (affects invocation: Python call vs SQL)

    Populated during C5 assembly, stored alongside converted_code.
    Read by Orchestrator at dispatch time for callee signature injection.
    """
    proc_name:      str
    strategy:       str                  = ""   # "TRINO_SQL" | "PYSPARK_DF" etc.
    params:         list[str]            = Field(default_factory=list)  # ["spark: SparkSession"]
    return_type:    str                  = "None"  # "DataFrame" | "None" | "dict"
    tables_read:    list[str]            = Field(default_factory=list)
    tables_written: list[str]            = Field(default_factory=list)
    signature_line: str                  = ""   # "def proc_X(spark: SparkSession) -> None:"

    def to_prompt_block(self) -> str:
        """Format for injection into a caller's C2 prompt."""
        if self.strategy.startswith("PYSPARK") or self.strategy == "":
            return (
                f"{self.signature_line}\n"
                f"    # Returns: {self.return_type}\n"
                f"    # Reads: {', '.join(self.tables_read) if self.tables_read else 'none'}\n"
                f"    # Writes: {', '.join(self.tables_written) if self.tables_written else 'none'}"
            )
        else:
            return (
                f"-- TRINO SQL proc: {self.proc_name}\n"
                f"-- Reads: {', '.join(self.tables_read) if self.tables_read else 'none'}\n"
                f"-- Writes: {', '.join(self.tables_written) if self.tables_written else 'none'}"
            )


def extract_callee_interface(
    proc_name: str,
    assembled_code: str,
    strategy: str,
    tables_read: list[str] | None = None,
    tables_written: list[str] | None = None,
) -> CalleeInterface:
    """
    Extract a structured CalleeInterface from assembled converted code.

    Called during C5 assembly. Uses regex to extract the function signature
    (more robust than reading raw lines because it handles decorators,
    multi-line params, type annotations, etc.).
    """
    import re

    # Extract signature line
    sig_line = f"def proc_{proc_name}(spark: SparkSession) -> None:"
    params = ["spark: SparkSession"]
    return_type = "None"

    # Try to extract actual signature from code
    sig_match = re.search(
        r'^(def\s+\w+\(.*?\))\s*(?:->.*?)?:', assembled_code, re.MULTILINE
    )
    if sig_match:
        sig_line = sig_match.group(0)
        # Extract params
        param_match = re.search(r'\((.*?)\)', sig_match.group(1))
        if param_match:
            raw_params = param_match.group(1)
            params = [p.strip() for p in raw_params.split(",") if p.strip()]
        # Extract return type
        ret_match = re.search(r'->\s*(\S+)', sig_line)
        if ret_match:
            return_type = ret_match.group(1).rstrip(":")

    return CalleeInterface(
        proc_name=proc_name,
        strategy=strategy,
        params=params,
        return_type=return_type,
        tables_read=tables_read or [],
        tables_written=tables_written or [],
        signature_line=sig_line,
    )


# ---------------------------------------------------------------------------
# Conversion LLM input: self-correction (C4)
# ---------------------------------------------------------------------------

def build_llm_self_correction_input(
    task:              DispatchTask,
    failed_code:       str,
    error_list:        list[str],
    attempt:           int,
    escalation_context: str = "",
) -> list[dict]:
    """
    Build LLM messages for self-correction after sandbox validation fails.
    Targeted: fix ONLY the listed errors, change nothing else.

    Args:
        task:               DispatchTask with strategy + construct_hints
        failed_code:        Code that failed validation
        error_list:         Specific errors to fix (from C3 sandbox output)
        attempt:            Current correction attempt number (1-based)
        escalation_context: Guidance from ErrorClassifier.build_llm_guidance()
                            — empty string for first attempt (standard guidance),
                            escalating message for repeated errors near limit.

    Returns:
        messages_list_for_llm_client
    """
    import json

    system_prompt = (
        "You are fixing specific errors in converted SQL code. "
        "Fix ONLY the listed errors. Change nothing else. "
        f"Output ONLY valid {_target_format(task.strategy)} code. "
        "No explanation. No markdown fences."
    )

    # Inject escalation context at top when provided so the LLM sees it first
    escalation_block = ""
    if escalation_context:
        escalation_block = escalation_context + "\n\n---\n\n"

    user_content = f"""{escalation_block}Fix ONLY these errors in the converted code (attempt {attempt}):

ERRORS TO FIX:
{chr(10).join(f"  {i+1}. {e}" for i, e in enumerate(error_list))}

CONSTRUCT MAPPINGS (re-reference for any REMNANT errors):
{json.dumps(task.construct_hints, indent=2) if task.construct_hints else "  (none)"}

FAILED CODE:
{failed_code}

Output corrected {_target_format(task.strategy)} code only:"""

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_content},
    ]


def _target_format(strategy: ConversionStrategy) -> str:
    if strategy == ConversionStrategy.TRINO_SQL:
        return "Trino SQL"
    return "PySpark Python"


# ===========================================================================
# VALIDATION AGENT
# ===========================================================================

class ColumnDiff(BaseModel):
    """Diff result for a single column."""
    column:   str
    status:   ColumnDiffStatus
    severity: str              = "LOW"   # "LOW" | "MEDIUM" | "HIGH"
    detail:   str              = ""


class ExecutionResult(BaseModel):
    """Result of running converted code in the sandbox."""
    runtime_error:   str | None     = None
    output_schema:   dict[str, str] = Field(default_factory=dict)
    row_count:       int            = 0
    sample_rows:     list[dict]     = Field(default_factory=list)
    dml_preview:     list[dict]     = Field(default_factory=list)
    duration_s:      float          = 0.0
    is_dry_run:      bool           = False


class ValidationResult(BaseModel):
    """
    Full validation result for a proc.
    Written to artifacts/05_validation/validation_{proc}.json.
    Returned to Orchestrator.
    """
    proc_name:            str
    outcome:              ValidationOutcome
    validation_level:     ValidationLevel
    execution_result:     ExecutionResult | None = None

    # Column-level diff (LEVEL_3 only)
    column_diffs:         list[ColumnDiff]   = Field(default_factory=list)

    # Row count comparison
    sandbox_row_count:    int | None         = None
    trino_row_count:      int | None         = None
    row_deviation_pct:    float | None       = None

    # UNMAPPED constructs from conversion_log
    manual_review_items:  list[TodoItem]     = Field(default_factory=list)

    # Warnings (present on PARTIAL)
    warnings:             list[str]          = Field(default_factory=list)

    # Fail reason (present on FAIL)
    fail_reason:          str                = ""

    # Provenance
    run_id:               str                = ""
    validated_at:         str                = ""

    @property
    def has_manual_items(self) -> bool:
        return len(self.manual_review_items) > 0

    @property
    def fail_columns(self) -> list[str]:
        return [d.column for d in self.column_diffs
                if d.status == ColumnDiffStatus.MISSING_IN_TRINO]


# ---------------------------------------------------------------------------
# Validation LLM input/output (semantic diff — LEVEL_3 only)
# ---------------------------------------------------------------------------

def build_llm_semantic_diff_input(
    converted_pyspark: str,
    schema_context:    dict[str, Any],
    strategy:          ConversionStrategy,
) -> list[dict]:
    """
    Build LLM messages to generate an equivalent Trino SQL query
    from PySpark code for semantic diff comparison.

    Returns:
        messages_list_for_llm_client
    """
    import json

    system_prompt = (
        "You translate PySpark DataFrame code to equivalent Trino SQL SELECT queries. "
        "Output ONLY the SQL query. No explanation. No markdown."
    )

    user_content = f"""Generate the equivalent Trino SQL SELECT for this PySpark code.
The SQL will be run against the same data to verify correctness.

Available Trino tables and columns:
{json.dumps(schema_context, indent=2)}

PySpark code:
{converted_pyspark[:3000]}

Output a single Trino SQL SELECT query only:"""

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_content},
    ]


def parse_column_diffs(
    sandbox_schema: dict[str, str],
    trino_schema:   dict[str, str],
) -> list[ColumnDiff]:
    """
    Compare sandbox execution schema with Trino query schema.
    Returns column-level diff list.
    """
    diffs: list[ColumnDiff] = []
    all_cols = set(sandbox_schema) | set(trino_schema)

    for col in sorted(all_cols):
        sandbox_type = sandbox_schema.get(col)
        trino_type   = trino_schema.get(col)

        if sandbox_type is not None and trino_type is not None:
            # Both present — check type compatibility
            if _types_compatible(sandbox_type, trino_type):
                diffs.append(ColumnDiff(
                    column=col,
                    status=ColumnDiffStatus.MATCH,
                    severity="LOW",
                ))
            elif _is_type_widening(sandbox_type, trino_type):
                diffs.append(ColumnDiff(
                    column=col,
                    status=ColumnDiffStatus.TYPE_WIDENED,
                    severity="LOW",
                    detail=f"{sandbox_type} → {trino_type}",
                ))
            else:
                diffs.append(ColumnDiff(
                    column=col,
                    status=ColumnDiffStatus.TYPE_CHANGED,
                    severity="MEDIUM",
                    detail=f"{sandbox_type} vs {trino_type}",
                ))
        elif sandbox_type is not None:
            diffs.append(ColumnDiff(
                column=col,
                status=ColumnDiffStatus.MISSING_IN_TRINO,
                severity="HIGH",
            ))
        else:
            diffs.append(ColumnDiff(
                column=col,
                status=ColumnDiffStatus.EXTRA_IN_TRINO,
                severity="LOW",
            ))

    return diffs


def _types_compatible(t1: str, t2: str) -> bool:
    t1, t2 = t1.lower(), t2.lower()
    if t1 == t2:
        return True
    # Treat varchar/string as compatible
    if set([t1, t2]) & {"varchar", "string", "text"}:
        return True
    return False


def _is_type_widening(from_type: str, to_type: str) -> bool:
    """True if to_type is a wider version of from_type (acceptable)."""
    widening_pairs = [
        ("int", "bigint"), ("int", "long"),
        ("float", "double"), ("decimal", "double"),
        ("smallint", "int"), ("tinyint", "smallint"),
    ]
    f, t = from_type.lower(), to_type.lower()
    return (f, t) in widening_pairs


# ===========================================================================
# HUMAN FEEDBACK (bundled here as it's created by Orchestrator)
# ===========================================================================

class FeedbackBundle(BaseModel):
    """
    Complete developer feedback bundle for a FROZEN proc.
    Written to artifacts/06_developer_feedback/developer_feedback_{proc}.json.
    Read by Streamlit feedback UI.
    """
    proc_name:            str
    frozen_at:            str
    replan_count:         int
    retry_count:          int
    status:               str   = "FROZEN"

    # Source context (no raw code from source files — manifest entry only)
    manifest_entry:       dict  = Field(default_factory=dict)
    complexity_entry:     dict  = Field(default_factory=dict)

    # Conversion artifacts (the converted code that failed)
    chunk_codes:          dict[str, str] = Field(default_factory=dict)  # {chunk_id: code}

    # What failed
    validation_reports:   dict[str, dict] = Field(default_factory=dict)
    error_history:        list[ErrorEntry] = Field(default_factory=list)

    # UNMAPPED constructs needing manual attention
    manual_review_items:  list[TodoItem]   = Field(default_factory=list)

    # Available resolution actions
    available_actions:    list[str]        = Field(default_factory=list)
    bundle_version:       str              = "1"


class FeedbackResolution(BaseModel):
    """
    Human's resolution for a FROZEN proc.
    Written to artifacts/06_developer_feedback/feedback_resolved_{proc}.json.
    Read by Orchestrator to continue the pipeline.
    """
    proc_name:       str
    action:          str   # One of ResolutionAction values
    notes:           str   = ""
    corrected_code:  str   = ""
    resolved_at:     str   = ""
    resolved_by:     str   = "human"