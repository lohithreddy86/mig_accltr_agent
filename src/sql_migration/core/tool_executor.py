"""
tool_executor.py
================
Tool execution framework for agentic LLM calls.

Provides:
  1. Tool definitions (JSON schema) for LiteLLM tool-calling
  2. Tool execution dispatch — routes tool calls to sandbox/MCP/local
  3. Agentic loop runner — iterates tool calls until the LLM produces
     a final result or hits a guard limit

Two tool sets:
  ANALYSIS_TOOLS  — for A1.5 Semantic Analysis (read-only exploration)
  CONVERSION_TOOLS — for C2/C4 Conversion (execute + validate + submit)

Both use the same execution infrastructure (sandbox, MCP) but with
different tool definitions and guard limits.

Security model:
  - MCP queries are read-only (blocked_keywords enforced by MCP server)
  - Sandbox execution is ephemeral (Podman container, resource-limited)
  - Tool call count is bounded by max_tool_calls per invocation
  - No write access to Trino lakehouse from any tool

Usage:
    from sql_migration.core.tool_executor import ToolExecutor, ANALYSIS_TOOLS

    executor = ToolExecutor(sandbox=sandbox, mcp=mcp, store=store)
    result = executor.run_agentic_loop(
        llm_client=llm,
        messages=messages,
        tools=ANALYSIS_TOOLS,
        max_tool_calls=30,
    )
"""

from __future__ import annotations

import json
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from sql_migration.core.logger import get_logger

log = get_logger("tool_executor")


# ===========================================================================
# Tool definitions (OpenAI function-calling format, used by LiteLLM)
# ===========================================================================

# ---------------------------------------------------------------------------
# Analysis tools (A1.5 — read-only code exploration + Trino queries)
# ---------------------------------------------------------------------------

ANALYSIS_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "read_proc",
            "description": (
                "Read the full source code of a stored procedure by name. "
                "Returns the complete proc body as a string. Use this to understand "
                "what a procedure does — its business logic, data transformations, "
                "and control flow."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "proc_name": {
                        "type": "string",
                        "description": "Exact procedure name from the manifest",
                    },
                },
                "required": ["proc_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_lines",
            "description": (
                "Read a specific line range from a source file. "
                "Use when you need to inspect a particular section without "
                "reading the entire proc."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Path to the source SQL file",
                    },
                    "start_line": {
                        "type": "integer",
                        "description": "Start line (0-indexed)",
                    },
                    "end_line": {
                        "type": "integer",
                        "description": "End line (inclusive)",
                    },
                },
                "required": ["file_path", "start_line", "end_line"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_trino",
            "description": (
                "Execute a read-only SQL query against the live Trino lakehouse. "
                "Use to inspect table schemas (DESCRIBE table), sample data "
                "(SELECT * FROM table LIMIT 5), row counts, or column values. "
                "DML statements (INSERT, UPDATE, DELETE, DROP) are blocked."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "Read-only SQL query to execute",
                    },
                },
                "required": ["sql"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "submit_semantic_map",
            "description": (
                "Submit the final Pipeline Semantic Map. Call this exactly once "
                "when you have finished analyzing all procedures and are ready "
                "to produce the structured output. This ends the analysis."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "semantic_map": {
                        "type": "object",
                        "description": "The complete Pipeline Semantic Map JSON",
                    },
                },
                "required": ["semantic_map"],
            },
        },
    },
]

# ---------------------------------------------------------------------------
# Intelligent Analysis tools (v9 — LLM-driven analysis replacing regex A1)
# The LLM uses sandbox parsers for heavy lifting, but validates and
# corrects results, discovers unknown UDFs, and builds semantic understanding.
# ---------------------------------------------------------------------------

INTELLIGENT_ANALYSIS_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "parse_file",
            "description": (
                "Parse a SQL source file for proc/function boundaries using "
                "sqlglot AST + regex fallback. Returns structured list of procs "
                "with boundaries, tables, and flags. REVIEW the parse_warnings — "
                "if coverage is low or proc sizes look wrong, use read_lines to "
                "inspect and correct boundaries manually."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Path to the SQL source file",
                    },
                },
                "required": ["file_path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "find_all_calls",
            "description": (
                "Scan a source file for ALL function-call-like patterns. "
                "Returns every identifier followed by '(' with occurrence counts. "
                "SQL keywords are pre-filtered. Use this to discover UDFs, "
                "external dependencies, and cross-proc calls that the parser missed."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Path to the SQL source file",
                    },
                },
                "required": ["file_path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_code",
            "description": (
                "Search for a regex pattern across all source files. "
                "Use to find where a function is defined, where a table is "
                "referenced, or to investigate anything the parser missed. "
                "Returns matching lines with file paths and line numbers."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Regex pattern to search for",
                    },
                },
                "required": ["pattern"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_lines",
            "description": (
                "Read a specific line range from a source file. "
                "Use to inspect proc boundaries, understand business logic, "
                "or verify parser output."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Path to the source SQL file",
                    },
                    "start_line": {
                        "type": "integer",
                        "description": "Start line (0-indexed)",
                    },
                    "end_line": {
                        "type": "integer",
                        "description": "End line (inclusive)",
                    },
                },
                "required": ["file_path", "start_line", "end_line"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_trino",
            "description": (
                "Execute a read-only SQL query against the live Trino lakehouse. "
                "Use to resolve table names (SHOW TABLES, DESCRIBE), check if "
                "tables exist, inspect schemas, or sample data."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "Read-only SQL query to execute",
                    },
                },
                "required": ["sql"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "build_dependency_graph",
            "description": (
                "Build a dependency graph from proc call relationships and "
                "compute topological conversion order. Provide the proc names "
                "and their calls. Returns edges, cycles, and sorted order."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "procs": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "calls": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                            },
                        },
                        "description": "List of {name, calls} for each proc",
                    },
                },
                "required": ["procs"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "submit_analysis",
            "description": (
                "Submit the complete analysis results. Call this exactly once "
                "when you have finished analyzing all source files. This ends "
                "the analysis and produces all artifacts for downstream agents."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "analysis": {
                        "type": "object",
                        "description": "Complete analysis output JSON",
                    },
                },
                "required": ["analysis"],
            },
        },
    },
]

# ---------------------------------------------------------------------------
# Conversion tools (C2/C4 — execute code + validate + submit)
# ---------------------------------------------------------------------------

_TOOL_RUN_PYSPARK = {
    "type": "function",
    "function": {
        "name": "run_pyspark",
        "description": (
            "Execute PySpark/Python code in an isolated sandbox. "
            "Returns stdout, stderr, and exit_code. Use this to test "
            "your converted code — create synthetic data, run the code, "
            "and verify the output. The sandbox has PySpark, pandas, "
            "and pyarrow available."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "Complete Python/PySpark code to execute",
                },
            },
            "required": ["code"],
        },
    },
}

_TOOL_VALIDATE_SQL = {
    "type": "function",
    "function": {
        "name": "validate_sql",
        "description": (
            "Validate Trino SQL syntax using sqlglot parser. "
            "Returns parse errors if any. Does NOT execute the SQL — "
            "use query_trino for that."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "Trino SQL to validate",
                },
            },
            "required": ["sql"],
        },
    },
}

_TOOL_QUERY_TRINO = {
    "type": "function",
    "function": {
        "name": "query_trino",
        "description": (
            "Execute a read-only SQL query against the live Trino lakehouse. "
            "Use to check table schemas, sample data, or verify column names. "
            "DML statements are blocked."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "Read-only SQL query to execute",
                },
            },
            "required": ["sql"],
        },
    },
}

_TOOL_SUBMIT_RESULT = {
    "type": "function",
    "function": {
        "name": "submit_result",
        "description": (
            "Submit the final converted code and optional test. "
            "Call this exactly once when your code passes validation. "
            "This ends the conversion for this chunk."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "converted_code": {
                    "type": "string",
                    "description": "The final converted PySpark/Trino SQL code",
                },
                "test_code": {
                    "type": "string",
                    "description": "Optional self-contained PySpark test script",
                    "default": "",
                },
                "state_vars": {
                    "type": "object",
                    "description": (
                        "Variables/DataFrames defined in this chunk that "
                        "the NEXT chunk should know about. Keys are variable "
                        "names, values are types (e.g. 'DataFrame', 'str', 'int'). "
                        "Example: {\"df_accounts\": \"DataFrame\", \"rate_tiers\": \"DataFrame\"}"
                    ),
                    "default": {},
                },
            },
            "required": ["converted_code"],
        },
    },
}

# Strategy-specific tool sets
TRINO_SQL_TOOLS = [_TOOL_VALIDATE_SQL, _TOOL_QUERY_TRINO, _TOOL_SUBMIT_RESULT]
PYSPARK_TOOLS   = [_TOOL_RUN_PYSPARK, _TOOL_QUERY_TRINO, _TOOL_SUBMIT_RESULT]

# Full set (backward compatibility — use get_conversion_tools instead)
CONVERSION_TOOLS = [_TOOL_RUN_PYSPARK, _TOOL_VALIDATE_SQL, _TOOL_QUERY_TRINO, _TOOL_SUBMIT_RESULT]


def get_conversion_tools(strategy: str) -> list[dict]:
    """Return the correct tool set for a conversion strategy."""
    if strategy == "TRINO_SQL":
        return TRINO_SQL_TOOLS
    return PYSPARK_TOOLS


# ===========================================================================
# Tool execution results
# ===========================================================================

# Default tool cost weights — overridden by config.agentic.tool_costs
_DEFAULT_TOOL_COSTS: dict[str, int] = {
    "run_pyspark":     3,   # Slow: PySpark startup + execution + teardown
    "validate_sql":    1,   # Fast: sqlglot parse, no subprocess
    "query_trino":     1,   # Fast: MCP read-only query
    "read_proc":       1,   # Fast: file read from cache
    "read_lines":      1,   # Fast: file read
    "parse_file":      2,   # Medium: sandbox subprocess + sqlglot
    "find_all_calls":  2,   # Medium: sandbox regex scan
    "search_code":     1,   # Fast: grep
    "build_dependency_graph": 2,  # Medium: networkx
    "submit_result":   0,   # Free: terminal
    "submit_analysis": 0,   # Free: terminal
    "submit_semantic_map": 0,  # Free: terminal
}

@dataclass
class ToolResult:
    """Result of executing a single tool call."""
    tool_call_id: str
    name: str
    content: str         # String content returned to the LLM
    success: bool = True
    is_terminal: bool = False   # True if this was a submit_* call
    terminal_data: Any = None   # Parsed data from submit calls


@dataclass
class AttemptRecord:
    """Record of one execution attempt (run_pyspark or validate_sql)."""
    attempt_number: int
    tool_name: str
    code_snippet: str = ""     # First 200 chars of submitted code
    code_lines: int = 0
    error: str = ""
    success: bool = False
    error_signature: str = ""  # Normalized for dedup


@dataclass
class AgenticLoopResult:
    """Result of running the full agentic tool-calling loop."""
    success: bool = False
    terminal_data: Any = None
    tool_calls_made: int = 0
    budget_used: int = 0
    budget_total: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    error: str = ""
    status: str = ""           # "COMPLETED" | "BUDGET_EXHAUSTED" | "MAX_TURNS" | "LLM_ERROR"
    attempts: list[AttemptRecord] = field(default_factory=list)
    history: list[dict] = field(default_factory=list)

    # Inter-chunk context (populated by loop for downstream consumption)
    completion_summary: str = ""     # Structured text for next chunk's prompt
    state_vars: dict[str, str] = field(default_factory=dict)
    confidence: float = 1.0          # 0.0-1.0 based on attempts and errors
    warnings: list[str] = field(default_factory=list)
    best_code: str = ""              # Best code produced (even if not submitted)


# ===========================================================================
# Tool Executor
# ===========================================================================

class ToolExecutor:
    """
    Executes tool calls from LLM responses and manages the agentic loop.

    Features:
      - Retry-aware history: older failed attempts summarized, latest kept raw
      - Tiered tool budgets: expensive tools cost more
      - Escalation protocol: ErrorClassifier guidance injected on repeated failures
      - Budget exhaustion: produces rich context for human feedback loop
      - Common output envelope: same structure consumed by all downstream agents
    """

    def __init__(
        self,
        sandbox,
        mcp=None,
        store=None,
        manifest: dict | None = None,
        source_files: list[str] | None = None,
        dialect_functions: list[str] | None = None,
    ) -> None:
        self.sandbox = sandbox
        self.mcp = mcp
        self.store = store
        self._manifest = manifest or {}
        self._source_files = source_files or []
        self._file_cache: dict[str, list[str]] = {}
        # Source-dialect functions for remnant detection at submit time
        self._dialect_functions = [f.upper() for f in (dialect_functions or [])]

    def update_manifest(self, manifest: dict) -> None:
        self._manifest = manifest

    # =========================================================================
    # Agentic loop — with retry awareness, budget control, and escalation
    # =========================================================================

    def run_agentic_loop(
        self,
        llm_client,
        messages: list[dict],
        tools: list[dict],
        max_tool_calls: int = 30,
        max_turns: int = 15,
        call_id: str = "",
        # Inter-chunk context from prior chunks (injected into system prompt)
        prior_chunk_summary: str = "",
        # Tool cost overrides from config (falls back to _DEFAULT_TOOL_COSTS)
        tool_costs: dict[str, int] | None = None,
        # Expected output format for submit_result format gating
        expected_strategy: str = "",
    ) -> AgenticLoopResult:
        """
        Run the agentic tool-calling loop with:
          - Retry-aware history management
          - Tiered budget tracking
          - Escalation protocol on repeated failures
          - Budget exhaustion → rich context for human feedback
          - Inter-chunk context injection

        Args:
            llm_client: LLMClient instance
            messages: Initial conversation (system + user)
            tools: Tool definitions
            max_tool_calls: Budget in cost units (not raw call count)
            max_turns: Max LLM round-trips
            call_id: Logging correlation ID
            prior_chunk_summary: Context from previous chunks

        Returns:
            AgenticLoopResult with common output envelope
        """
        from sql_migration.core.error_handling import ErrorClassifier

        result = AgenticLoopResult(budget_total=max_tool_calls)

        # Store expected strategy for format gating in submit_result
        self._expected_strategy = expected_strategy

        # Merge config tool costs over defaults
        effective_costs = {**_DEFAULT_TOOL_COSTS, **(tool_costs or {})}

        # Inject prior chunk context if available
        if prior_chunk_summary:
            # Find system message and append
            for i, m in enumerate(messages):
                if m.get("role") == "system":
                    messages[i] = {
                        "role": "system",
                        "content": m["content"] + (
                            f"\n\n## PRIOR CHUNK CONTEXT\n{prior_chunk_summary}"
                        ),
                    }
                    break

        result.history = list(messages)
        budget_remaining = max_tool_calls
        terminal_data = None

        # Track execution attempts for retry-aware history
        exec_attempts: list[AttemptRecord] = []
        # Track the last code submitted to run_pyspark/validate_sql
        last_exec_code: str = ""
        # Error signature counter for escalation
        error_sig_counts: dict[str, int] = {}

        for turn in range(max_turns):
            # ── Call LLM with tools ───────────────────────────────────────
            try:
                response = llm_client.call_with_tools(
                    messages=result.history,
                    tools=tools,
                    call_id=f"{call_id}_t{turn}",
                )
            except Exception as e:
                result.error = f"LLM call failed on turn {turn}: {e}"
                result.status = "LLM_ERROR"
                log.error("agentic_loop_llm_error",
                          turn=turn, error=str(e)[:200])
                break

            # Track tokens
            if hasattr(response, "usage") and response.usage:
                result.total_input_tokens += response.usage.prompt_tokens or 0
                result.total_output_tokens += response.usage.completion_tokens or 0

            choice = response.choices[0]
            message = choice.message

            # Append assistant message to history
            result.history.append(_message_to_dict(message))

            # ── Check for tool calls ──────────────────────────────────────
            if message.tool_calls:
                for tc in message.tool_calls:
                    tool_name = tc.function.name
                    cost = effective_costs.get(tool_name, 1)

                    # ── Budget check ──────────────────────────────────────
                    if budget_remaining < cost:
                        # Budget exhausted — give LLM one chance to submit
                        exhaust_msg = (
                            f"BUDGET_EXHAUSTED: You have used {result.budget_used}"
                            f"/{max_tool_calls} budget units. Cannot execute "
                            f"'{tool_name}' (costs {cost}). "
                            f"Call submit_result or submit_analysis NOW with "
                            f"your best output so far."
                        )
                        result.history.append({
                            "role": "tool",
                            "tool_call_id": tc.id,
                            "content": exhaust_msg,
                        })
                        result.warnings.append("Budget exhausted before completion")
                        continue

                    budget_remaining -= cost
                    result.budget_used += cost
                    result.tool_calls_made += 1

                    # ── Track code for execution tools ────────────────────
                    try:
                        args = json.loads(tc.function.arguments) if tc.function.arguments else {}
                    except json.JSONDecodeError:
                        args = {}

                    is_exec_tool = tool_name in ("run_pyspark", "validate_sql")
                    if is_exec_tool:
                        last_exec_code = args.get("code", args.get("sql", ""))

                    # ── Execute the tool ──────────────────────────────────
                    tr = self._execute_tool(
                        tc.id, tool_name, tc.function.arguments,
                    )

                    log.debug("tool_executed",
                              call_id=call_id, turn=turn,
                              tool=tool_name, cost=cost,
                              success=tr.success, terminal=tr.is_terminal,
                              budget_remaining=budget_remaining)

                    # ── Record execution attempt ──────────────────────────
                    if is_exec_tool:
                        attempt = AttemptRecord(
                            attempt_number=len(exec_attempts) + 1,
                            tool_name=tool_name,
                            code_snippet=last_exec_code[:200],
                            code_lines=last_exec_code.count('\n') + 1,
                            error="" if tr.success else tr.content[:300],
                            success=tr.success,
                        )

                        # Track error signatures for escalation
                        if not tr.success:
                            sig = ErrorClassifier.get_signature(tr.content)
                            attempt.error_signature = sig
                            error_sig_counts[sig] = error_sig_counts.get(sig, 0) + 1

                        exec_attempts.append(attempt)

                        # ── Retry-aware history management ────────────────
                        # If this is attempt 2+, replace OLDER attempt's raw
                        # code+error in history with a structured summary.
                        # Keep the LATEST attempt's full code visible.
                        if len(exec_attempts) >= 2:
                            self._compress_older_attempts(
                                result.history, exec_attempts,
                            )

                        # ── Escalation on repeated failures ───────────────
                        if not tr.success and len(exec_attempts) >= 2:
                            sig = attempt.error_signature
                            occurrence = error_sig_counts.get(sig, 1)
                            guidance = ErrorClassifier.build_llm_guidance(
                                error_msg=tr.content[:500],
                                occurrence=occurrence,
                                attempt=len(exec_attempts),
                                max_attempts=max_tool_calls // max(cost, 1),
                            )
                            # Inject escalation as a user hint after tool result
                            if occurrence >= 2:
                                tr.content += f"\n\n{guidance}"

                    # Track best code (last successful or latest)
                    if is_exec_tool:
                        if tr.success:
                            result.best_code = last_exec_code
                        elif not result.best_code:
                            result.best_code = last_exec_code

                    # ── Terminal check ────────────────────────────────────
                    if tr.is_terminal:
                        terminal_data = tr.terminal_data
                        result.history.append({
                            "role": "tool",
                            "tool_call_id": tc.id,
                            "content": tr.content,
                        })
                        break

                    # Append tool result to history
                    content_for_history = tr.content
                    if is_exec_tool:
                        content_for_history = f"[EXEC_ATTEMPT]\n{tr.content}"
                    result.history.append({
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": content_for_history,
                    })

                if terminal_data:
                    result.success = True
                    result.terminal_data = terminal_data
                    result.status = "COMPLETED"
                    break

            else:
                # LLM returned text without tool calls — nudge or accept
                text = message.content or ""
                if turn == max_turns - 1:
                    result.error = "LLM produced text without calling submit"
                    result.terminal_data = {"raw_text": text}
                    result.status = "MAX_TURNS"
                    break
                else:
                    result.history.append({
                        "role": "user",
                        "content": (
                            "You must call submit_result or submit_analysis "
                            "to provide your final output. Use the tool call."
                        ),
                    })

        # ── Finalize result ───────────────────────────────────────────────

        if not result.status:
            if budget_remaining <= 0:
                result.status = "BUDGET_EXHAUSTED"
            else:
                result.status = "MAX_TURNS"

        result.attempts = exec_attempts

        # Compute confidence based on attempts
        result.confidence = self._compute_confidence(exec_attempts, result.success)

        # Build completion summary for inter-chunk context
        result.completion_summary = self._build_completion_summary(
            exec_attempts, result.success, result.terminal_data, call_id,
        )

        # Populate state vars from terminal data (LLM reports what it defined)
        if result.terminal_data and isinstance(result.terminal_data, dict):
            submitted_vars = result.terminal_data.get("state_vars", {})
            if submitted_vars:
                result.state_vars = submitted_vars

        log.info("agentic_loop_complete",
                 call_id=call_id,
                 status=result.status,
                 success=result.success,
                 turns=turn + 1,
                 tool_calls=result.tool_calls_made,
                 budget=f"{result.budget_used}/{max_tool_calls}",
                 attempts=len(exec_attempts),
                 confidence=round(result.confidence, 2),
                 input_tokens=result.total_input_tokens,
                 output_tokens=result.total_output_tokens,
                 error=result.error[:100] if result.error else "")

        return result

    # =========================================================================
    # Retry-aware history compression
    # =========================================================================

    def _compress_older_attempts(
        self,
        history: list[dict],
        attempts: list[AttemptRecord],
    ) -> None:
        """
        Replace older execution attempts' raw code in history with summaries.
        Keep only the LATEST attempt's full code+error visible.

        Why: The LLM reproduces patterns from its own failed code when it
        sees the full code in history. Summarizing older attempts tells it
        what was tried and what went wrong WITHOUT exposing the exact code
        patterns that failed.
        """
        if len(attempts) < 2:
            return

        # Build summary of older attempts (all except the last one)
        older = attempts[:-1]
        summary_lines = ["PREVIOUS ATTEMPTS (summarized — do NOT repeat these approaches):"]
        for a in older:
            status = "PASSED" if a.success else "FAILED"
            summary_lines.append(
                f"  Attempt {a.attempt_number} ({a.tool_name}): {status}"
                f" — {a.code_lines} lines"
            )
            if a.error:
                # Keep error message but not the code
                summary_lines.append(f"    Error: {a.error[:150]}")
            if a.error_signature:
                summary_lines.append(f"    Signature: {a.error_signature[:80]}")
        summary = "\n".join(summary_lines)

        # Find and replace older tool results in history.
        # We scan backwards and replace run_pyspark/validate_sql tool
        # results that correspond to older attempts with the summary.
        # The most recent tool result is kept intact.
        exec_tool_results_seen = 0
        total_exec_results = len(attempts)
        replaced = False

        for i in range(len(history) - 1, -1, -1):
            msg = history[i]
            if msg.get("role") == "tool":
                content = msg.get("content", "")
                # Use explicit tag instead of content-matching
                if content.startswith("[EXEC_ATTEMPT]"):
                    exec_tool_results_seen += 1
                    # Keep the latest, replace older ones
                    if exec_tool_results_seen > 1 and not replaced:
                        msg["content"] = summary
                        replaced = True
                    elif replaced:
                        msg["content"] = "[earlier attempt — see summary above]"

    # =========================================================================
    # Confidence scoring
    # =========================================================================

    @staticmethod
    def _compute_confidence(
        attempts: list[AttemptRecord], success: bool,
    ) -> float:
        """
        Compute confidence score (0.0–1.0) based on execution history.

        1.0 = first attempt success, no errors
        0.8 = succeeded after 1 retry
        0.6 = succeeded after 2+ retries
        0.4 = succeeded but with repeated error signatures
        0.2 = budget exhausted, partial code produced
        0.0 = no code produced
        """
        if not attempts:
            return 0.8 if success else 0.3  # No exec tools used

        if success:
            failed_count = sum(1 for a in attempts if not a.success)
            if failed_count == 0:
                return 1.0
            elif failed_count == 1:
                return 0.8
            elif failed_count <= 3:
                return 0.6
            else:
                return 0.4
        else:
            if any(a.success for a in attempts):
                return 0.3  # Had some success but didn't submit
            return 0.1

    # =========================================================================
    # Completion summary for inter-chunk / inter-agent context
    # =========================================================================

    @staticmethod
    def _build_completion_summary(
        attempts: list[AttemptRecord],
        success: bool,
        terminal_data: Any,
        call_id: str,
    ) -> str:
        """
        Build a structured summary of what happened during this agentic loop.
        Used as inter-chunk context for the next chunk's system prompt.
        """
        lines = [f"[Chunk {call_id} — {'COMPLETED' if success else 'INCOMPLETE'}]"]

        if attempts:
            lines.append(f"  Execution attempts: {len(attempts)}")
            failed = [a for a in attempts if not a.success]
            if failed:
                lines.append(f"  Failed attempts: {len(failed)}")
                # Unique error types
                sigs = set(a.error_signature for a in failed if a.error_signature)
                if sigs:
                    lines.append(f"  Error types: {', '.join(list(sigs)[:3])}")
            passed = [a for a in attempts if a.success]
            if passed:
                lines.append(f"  Passed: {passed[-1].code_lines} lines of code")

        if success and terminal_data:
            if isinstance(terminal_data, dict):
                code = terminal_data.get("converted_code", "")
                if code:
                    # Extract key artifacts from code
                    import re
                    vars_found = re.findall(
                        r'(\w+)\s*=\s*(?:spark\.|df\.|F\.)', code,
                    )
                    fns_found = re.findall(r'^def\s+(\w+)\s*\(', code, re.MULTILINE)
                    if vars_found:
                        lines.append(
                            f"  Variables defined: {', '.join(vars_found[:10])}"
                        )
                    if fns_found:
                        lines.append(
                            f"  Functions defined: {', '.join(fns_found[:5])}"
                        )

        return "\n".join(lines)

    # =========================================================================
    # Tool dispatch
    # =========================================================================

    def _execute_tool(
        self, tool_call_id: str, name: str, arguments_json: str,
    ) -> ToolResult:
        """Route a tool call to the appropriate handler."""
        try:
            args = json.loads(arguments_json) if arguments_json else {}
        except json.JSONDecodeError as e:
            return ToolResult(
                tool_call_id=tool_call_id, name=name,
                content=f"ERROR: Invalid JSON arguments: {e}",
                success=False,
            )

        handler = self._HANDLERS.get(name)
        if not handler:
            return ToolResult(
                tool_call_id=tool_call_id, name=name,
                content=f"ERROR: Unknown tool '{name}'",
                success=False,
            )

        try:
            return handler(self, tool_call_id, args)
        except Exception as e:
            log.error("tool_execution_error",
                      tool=name, error=str(e)[:300])
            return ToolResult(
                tool_call_id=tool_call_id, name=name,
                content=f"ERROR: {type(e).__name__}: {e}",
                success=False,
            )

    # =========================================================================
    # Tool handlers
    # =========================================================================

    def _handle_read_proc(self, tool_call_id: str, args: dict) -> ToolResult:
        """Read full source code of a proc by name."""
        proc_name = args.get("proc_name", "")
        procs = self._manifest.get("procs", [])
        proc = next((p for p in procs if p["name"] == proc_name), None)

        if not proc:
            available = [p["name"] for p in procs[:20]]
            return ToolResult(
                tool_call_id=tool_call_id, name="read_proc",
                content=f"ERROR: Proc '{proc_name}' not found in manifest. "
                        f"Available: {available}",
                success=False,
            )

        source_file = proc["source_file"]
        start = proc["start_line"]
        end = proc["end_line"]

        lines = self._read_file_lines(source_file)
        if lines is None:
            return ToolResult(
                tool_call_id=tool_call_id, name="read_proc",
                content=f"ERROR: Could not read file '{source_file}'",
                success=False,
            )

        code = "\n".join(lines[start:end + 1])
        return ToolResult(
            tool_call_id=tool_call_id, name="read_proc",
            content=code,
        )

    def _handle_read_lines(self, tool_call_id: str, args: dict) -> ToolResult:
        """Read specific line range from a file."""
        file_path = args.get("file_path", "")
        start = args.get("start_line", 0)
        end = args.get("end_line", 0)

        lines = self._read_file_lines(file_path)
        if lines is None:
            return ToolResult(
                tool_call_id=tool_call_id, name="read_lines",
                content=f"ERROR: Could not read file '{file_path}'",
                success=False,
            )

        chunk = "\n".join(lines[start:end + 1])
        return ToolResult(
            tool_call_id=tool_call_id, name="read_lines",
            content=chunk,
        )

    def _handle_query_trino(self, tool_call_id: str, args: dict) -> ToolResult:
        """Execute read-only Trino query via MCP."""
        sql = args.get("sql", "")
        if not self.mcp:
            return ToolResult(
                tool_call_id=tool_call_id, name="query_trino",
                content="ERROR: MCP client not available",
                success=False,
            )

        try:
            result = self.mcp.run_query(sql)
            # Truncate large results
            result_str = json.dumps(result, default=str)
            if len(result_str) > 8000:
                result_str = result_str[:8000] + "\n... (truncated)"
            return ToolResult(
                tool_call_id=tool_call_id, name="query_trino",
                content=result_str,
            )
        except Exception as e:
            return ToolResult(
                tool_call_id=tool_call_id, name="query_trino",
                content=f"QUERY_ERROR: {e}",
                success=False,
            )

    def _handle_run_pyspark(self, tool_call_id: str, args: dict) -> ToolResult:
        """Execute PySpark code in the sandbox."""
        code = args.get("code", "")
        if not code.strip():
            return ToolResult(
                tool_call_id=tool_call_id, name="run_pyspark",
                content="ERROR: Empty code provided",
                success=False,
            )

        result = self.sandbox.run(
            script="conversion/validate_conversion.py",
            args={
                "mode": "exec_code",
                "code": code,
            },
        )

        if not result.success:
            stderr = result.stderr[:2000] if result.stderr else "unknown error"
            return ToolResult(
                tool_call_id=tool_call_id, name="run_pyspark",
                content=f"EXECUTION_ERROR:\n{stderr}",
                success=False,
            )

        try:
            data = result.stdout_json
            stdout = data.get("stdout", "")
            stderr = data.get("stderr", "")
            exit_code = data.get("exit_code", 0)
            content = f"exit_code: {exit_code}\n"
            if stdout:
                content += f"stdout:\n{stdout[:3000]}\n"
            if stderr:
                content += f"stderr:\n{stderr[:2000]}\n"
            return ToolResult(
                tool_call_id=tool_call_id, name="run_pyspark",
                content=content,
                success=exit_code == 0,
            )
        except Exception:
            return ToolResult(
                tool_call_id=tool_call_id, name="run_pyspark",
                content=result.stdout[:3000] if result.stdout else "No output",
            )

    def _handle_validate_sql(self, tool_call_id: str, args: dict) -> ToolResult:
        """Validate Trino SQL via sqlglot parse."""
        sql = args.get("sql", "")
        result = self.sandbox.run(
            script="conversion/validate_conversion.py",
            args={
                "converted_code": sql,
                "strategy": "TRINO_SQL",
                "dialect_functions": [],
                "trino_mappings": {},
            },
        )

        if not result.success:
            return ToolResult(
                tool_call_id=tool_call_id, name="validate_sql",
                content=f"VALIDATION_ERROR: {result.stderr[:500]}",
                success=False,
            )

        try:
            data = result.stdout_json
            errors = data.get("errors", [])
            if errors:
                return ToolResult(
                    tool_call_id=tool_call_id, name="validate_sql",
                    content=f"PARSE_ERRORS:\n" + "\n".join(f"  - {e}" for e in errors),
                    success=False,
                )
            return ToolResult(
                tool_call_id=tool_call_id, name="validate_sql",
                content="SQL syntax is valid (sqlglot parse passed)",
            )
        except Exception as e:
            return ToolResult(
                tool_call_id=tool_call_id, name="validate_sql",
                content=f"ERROR parsing validation output: {e}",
                success=False,
            )

    def _handle_submit_semantic_map(self, tool_call_id: str, args: dict) -> ToolResult:
        """Accept the final semantic map from A1.5."""
        semantic_map = args.get("semantic_map", {})
        return ToolResult(
            tool_call_id=tool_call_id, name="submit_semantic_map",
            content="Semantic map accepted.",
            is_terminal=True,
            terminal_data=semantic_map,
        )

    def _handle_submit_result(self, tool_call_id: str, args: dict) -> ToolResult:
        """Accept the final converted code from Conversion Agent."""
        code = args.get("converted_code", "")
        test = args.get("test_code", "")
        state_vars = args.get("state_vars", {})
        if not code.strip():
            return ToolResult(
                tool_call_id=tool_call_id, name="submit_result",
                content="ERROR: converted_code is empty. "
                        "Provide the code and call submit_result again.",
                success=False,
            )

        # Remnant detection: check for unconverted source-dialect functions.
        # If the code still contains NVL(, DECODE(, SYSDATE, etc., reject
        # the submit and tell the LLM what to fix.
        # v10-fix: Strip comments BEFORE scanning so TODO markers like
        # "# TODO: UNMAPPED — original: NVL(balance, 0)" don't trigger rejection.
        if self._dialect_functions:
            import re
            # OLD: scanned raw code including comments — caused infinite rejection
            # loop when LLM followed instructions and wrote TODO comments containing
            # the original function call.
            # remnants = []
            # code_upper = code.upper()
            # for fn in self._dialect_functions:
            #     # Match function call pattern: FUNC_NAME( but not in comments
            #     if re.search(r'\b' + re.escape(fn) + r'\s*\(', code_upper):
            #         remnants.append(fn)

            # NEW: strip comment lines and inline comments before scanning
            stripped_lines = []
            for line in code.splitlines():
                line_stripped = line.strip()
                # Skip full-line comments (Python # and SQL --)
                if line_stripped.startswith("#") or line_stripped.startswith("--"):
                    continue
                # Remove inline comments (# ... and -- ...)
                # Naive but sufficient: split at first # or -- outside quotes
                for marker in (" #", " --"):
                    idx = line_stripped.find(marker)
                    if idx >= 0:
                        line_stripped = line_stripped[:idx]
                stripped_lines.append(line_stripped)
            code_no_comments = "\n".join(stripped_lines).upper()

            remnants = []
            for fn in self._dialect_functions:
                if re.search(r'\b' + re.escape(fn) + r'\s*\(', code_no_comments):
                    remnants.append(fn)

            if remnants:
                return ToolResult(
                    tool_call_id=tool_call_id, name="submit_result",
                    content=(
                        f"REJECTED: Code contains {len(remnants)} unconverted "
                        f"source-dialect function(s): {', '.join(remnants[:10])}. "
                        f"Replace these with their target equivalents from the "
                        f"construct mappings, or wrap in a TODO comment if unmapped. "
                        f"Then call submit_result again."
                    ),
                    success=False,
                )

        # Format gate: verify code matches expected strategy output format.
        strategy = getattr(self, "_expected_strategy", "")
        if strategy:
            code_lower = code.lower()
            _PYSPARK_MARKERS = ("def ", "import ", "spark.", "dataframe", "from pyspark")
            _SQL_MARKERS = ("select ", "insert ", "create ", "with ", "merge ")
            has_python = any(m in code_lower for m in _PYSPARK_MARKERS)
            has_sql = any(m in code_lower for m in _SQL_MARKERS)

            if strategy == "TRINO_SQL" and has_python and not has_sql:
                return ToolResult(
                    tool_call_id=tool_call_id, name="submit_result",
                    content=(
                        "REJECTED: This is a TRINO_SQL conversion but you submitted "
                        "PySpark/Python code. Rewrite as pure Trino SQL and submit again."
                    ),
                    success=False,
                )
            if strategy in ("PYSPARK_DF", "PYSPARK_PIPELINE") and has_sql and not has_python:
                return ToolResult(
                    tool_call_id=tool_call_id, name="submit_result",
                    content=(
                        "REJECTED: This is a PySpark conversion but you submitted "
                        "raw SQL. Rewrite as PySpark Python code and submit again."
                    ),
                    success=False,
                )

        return ToolResult(
            tool_call_id=tool_call_id, name="submit_result",
            content="Result accepted.",
            is_terminal=True,
            terminal_data={
                "converted_code": code,
                "test_code": test,
                "state_vars": state_vars,
            },
        )

    # ── Intelligent Analysis tool handlers (v9) ──────────────────────────

    def _handle_parse_file(self, tool_call_id: str, args: dict) -> ToolResult:
        """Parse a SQL file for proc boundaries via sandbox."""
        file_path = args.get("file_path", "")
        adapter_data = None
        # Try to load adapter for hint patterns
        if self.store:
            try:
                profile = self.store.read("analysis", "dialect_profile.json")
                adapter_path = Path(profile.get("adapter_path", ""))
                if adapter_path.exists():
                    adapter_data = json.loads(adapter_path.read_text())
            except Exception:
                pass

        result = self.sandbox.run(
            script="extraction/parse_and_scan.py",
            args={
                "mode": "parse_file",
                "file_path": file_path,
                "adapter": adapter_data,
            },
        )
        if not result.success:
            return ToolResult(
                tool_call_id=tool_call_id, name="parse_file",
                content=f"PARSE_ERROR: {result.stderr[:500]}",
                success=False,
            )
        try:
            data = result.stdout_json
            return ToolResult(
                tool_call_id=tool_call_id, name="parse_file",
                content=json.dumps(data, indent=2)[:8000],
            )
        except Exception as e:
            return ToolResult(
                tool_call_id=tool_call_id, name="parse_file",
                content=f"ERROR parsing output: {e}",
                success=False,
            )

    def _handle_find_all_calls(self, tool_call_id: str, args: dict) -> ToolResult:
        """Scan a file for all function-call-like patterns."""
        file_path = args.get("file_path", "")
        result = self.sandbox.run(
            script="extraction/parse_and_scan.py",
            args={"mode": "find_all_calls", "file_path": file_path},
        )
        if not result.success:
            return ToolResult(
                tool_call_id=tool_call_id, name="find_all_calls",
                content=f"SCAN_ERROR: {result.stderr[:500]}",
                success=False,
            )
        try:
            data = result.stdout_json
            return ToolResult(
                tool_call_id=tool_call_id, name="find_all_calls",
                content=json.dumps(data, indent=2)[:8000],
            )
        except Exception as e:
            return ToolResult(
                tool_call_id=tool_call_id, name="find_all_calls",
                content=f"ERROR: {e}",
                success=False,
            )

    def _handle_search_code(self, tool_call_id: str, args: dict) -> ToolResult:
        """Search for a pattern across source files."""
        pattern = args.get("pattern", "")
        result = self.sandbox.run(
            script="extraction/parse_and_scan.py",
            args={
                "mode": "search_code",
                "pattern": pattern,
                "file_paths": self._source_files,
                "max_results": 30,
            },
        )
        if not result.success:
            return ToolResult(
                tool_call_id=tool_call_id, name="search_code",
                content=f"SEARCH_ERROR: {result.stderr[:500]}",
                success=False,
            )
        try:
            data = result.stdout_json
            return ToolResult(
                tool_call_id=tool_call_id, name="search_code",
                content=json.dumps(data, indent=2)[:6000],
            )
        except Exception as e:
            return ToolResult(
                tool_call_id=tool_call_id, name="search_code",
                content=f"ERROR: {e}",
                success=False,
            )

    def _handle_build_dependency_graph(self, tool_call_id: str, args: dict) -> ToolResult:
        """Build dependency graph from proc call relationships."""
        procs = args.get("procs", [])
        result = self.sandbox.run(
            script="extraction/build_graph.py",
            args={"procs": procs},
        )
        if not result.success:
            return ToolResult(
                tool_call_id=tool_call_id, name="build_dependency_graph",
                content=f"GRAPH_ERROR: {result.stderr[:500]}",
                success=False,
            )
        try:
            data = result.stdout_json
            return ToolResult(
                tool_call_id=tool_call_id, name="build_dependency_graph",
                content=json.dumps(data, indent=2)[:6000],
            )
        except Exception as e:
            return ToolResult(
                tool_call_id=tool_call_id, name="build_dependency_graph",
                content=f"ERROR: {e}",
                success=False,
            )

    def _handle_submit_analysis(self, tool_call_id: str, args: dict) -> ToolResult:
        """Accept the complete analysis output."""
        analysis = args.get("analysis", {})
        if not analysis:
            return ToolResult(
                tool_call_id=tool_call_id, name="submit_analysis",
                content="ERROR: analysis is empty.",
                success=False,
            )
        return ToolResult(
            tool_call_id=tool_call_id, name="submit_analysis",
            content="Analysis accepted.",
            is_terminal=True,
            terminal_data=analysis,
        )

    # Handler dispatch table
    _HANDLERS: dict[str, Callable] = {
        "read_proc":               _handle_read_proc,
        "read_lines":              _handle_read_lines,
        "query_trino":             _handle_query_trino,
        "run_pyspark":             _handle_run_pyspark,
        "validate_sql":            _handle_validate_sql,
        "submit_semantic_map":     _handle_submit_semantic_map,
        "submit_result":           _handle_submit_result,
        # v9: Intelligent analysis tools
        "parse_file":              _handle_parse_file,
        "find_all_calls":          _handle_find_all_calls,
        "search_code":             _handle_search_code,
        "build_dependency_graph":  _handle_build_dependency_graph,
        "submit_analysis":         _handle_submit_analysis,
    }

    # =========================================================================
    # File reading helpers
    # =========================================================================

    def _read_file_lines(self, file_path: str) -> list[str] | None:
        """Read and cache file lines."""
        if file_path in self._file_cache:
            return self._file_cache[file_path]

        path = Path(file_path)
        if not path.exists():
            return None

        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
            self._file_cache[file_path] = lines
            return lines
        except Exception:
            return None


# ===========================================================================
# Helper
# ===========================================================================

def _message_to_dict(message) -> dict:
    """Convert a LiteLLM message object to a dict for history."""
    d: dict[str, Any] = {"role": message.role}
    if message.content:
        d["content"] = message.content
    if message.tool_calls:
        d["tool_calls"] = [
            {
                "id": tc.id,
                "type": "function",
                "function": {
                    "name": tc.function.name,
                    "arguments": tc.function.arguments,
                },
            }
            for tc in message.tool_calls
        ]
    return d