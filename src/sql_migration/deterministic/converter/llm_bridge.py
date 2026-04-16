"""
Unified LLM interface for surgical PL/SQL → PySpark conversion assistance.

Single integration point between the converter pipeline and LLM providers.
The LLM is a fallback — rule-based path runs first; LLM only activates
when a TODO would otherwise be emitted.

All calls go through: prompt → cache check → LLM call → validate → cache store → return.
Returns None on any failure (caller falls back to original TODO).
"""

import os
import re

from .llm_cache import LLMCache
from .llm_prompts import (
    CORRELATED_UPDATE_TO_MERGE, CORRELATED_UPDATE_TO_MERGE_VERSION,
    UNKNOWN_STATEMENT, UNKNOWN_STATEMENT_VERSION,
    FOR_LOOP_FALLBACK, FOR_LOOP_FALLBACK_VERSION,
    TODO_FIX, TODO_FIX_VERSION,
)
from .llm_validator import validate_merge_sql, validate_python_code, validate_todo_fix


class LLMBridge:
    """Unified LLM interface for converter pipeline."""

    def __init__(self, provider: str = "deepseek", cache_dir: str = ".cache/llm_conversions",
                 dry_run: bool = False):
        self.provider = provider
        self.cache = LLMCache(cache_dir)
        self.dry_run = dry_run
        self._client = None
        self._stats = {
            "calls_made": 0,
            "cache_hits": 0,
            "todos_resolved": 0,
            "validation_failures": 0,
            "api_errors": 0,
        }

    def _get_client(self):
        """Lazy-initialize the LLM client."""
        if self._client is not None:
            return self._client

        if self.provider == "deepseek":
            api_key = os.environ.get("DEEPSEEK_API_KEY")
            if not api_key:
                return None
            try:
                from openai import OpenAI
                self._client = OpenAI(
                    api_key=api_key,
                    base_url="https://api.deepseek.com",
                )
            except ImportError:
                return None
        elif self.provider == "claude":
            api_key = os.environ.get("ANTHROPIC_API_KEY")
            if not api_key:
                return None
            try:
                import anthropic
                # Explicit timeout so a half-open connection doesn't block
                # the deterministic converter for Anthropic's 10-min default.
                self._client = anthropic.Anthropic(api_key=api_key, timeout=60.0)
            except ImportError:
                return None
        else:
            return None

        return self._client

    def _call_llm(self, prompt: str) -> str | None:
        """Make a single LLM call. Returns response text or None."""
        if self.dry_run:
            return None

        client = self._get_client()
        if client is None:
            return None

        try:
            if self.provider == "deepseek":
                response = client.chat.completions.create(
                    model="deepseek-chat",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0,
                    max_tokens=4096,
                )
                return response.choices[0].message.content
            elif self.provider == "claude":
                response = client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=4096,
                    temperature=0,
                    messages=[{"role": "user", "content": prompt}],
                )
                return response.content[0].text
        except Exception:
            self._stats["api_errors"] += 1
            return None

    def convert_correlated_update(self, oracle_sql: str, preprocessed_sql: str,
                                   target_table: str) -> str | None:
        """Convert a correlated UPDATE to MERGE via LLM.

        Args:
            oracle_sql: Original Oracle SQL
            preprocessed_sql: SQL after SQLGlot transpilation (NVL→COALESCE etc.)
            target_table: The target table name

        Returns:
            Valid MERGE SQL string, or None on failure.
        """
        category = "correlated_update"
        version = CORRELATED_UPDATE_TO_MERGE_VERSION

        # Check cache
        cached = self.cache.get(category, oracle_sql, version)
        if cached is not None:
            self._stats["cache_hits"] += 1
            self._stats["todos_resolved"] += 1
            return cached

        # Build prompt
        prompt = CORRELATED_UPDATE_TO_MERGE.format(
            preprocessed_sql=preprocessed_sql,
            oracle_sql=oracle_sql,
            target_table=target_table,
        )

        # Call LLM
        self._stats["calls_made"] += 1
        result = self._call_llm(prompt)
        if result is None:
            return None

        # Validate
        validated = validate_merge_sql(result, target_table)
        if validated is None:
            self._stats["validation_failures"] += 1
            return None

        # Cache and return
        self.cache.put(category, oracle_sql, version, validated)
        self._stats["todos_resolved"] += 1
        return validated

    def convert_unknown_statement(self, raw_plsql: str,
                                   procedure_context: str = "") -> str | None:
        """Convert an unclassified PL/SQL statement to PySpark via LLM.

        Returns valid Python code string, or None on failure.
        """
        category = "unknown_statement"
        version = UNKNOWN_STATEMENT_VERSION

        cached = self.cache.get(category, raw_plsql, version)
        if cached is not None:
            self._stats["cache_hits"] += 1
            self._stats["todos_resolved"] += 1
            return cached

        prompt = UNKNOWN_STATEMENT.format(
            raw_plsql=raw_plsql,
            procedure_context=procedure_context,
        )

        self._stats["calls_made"] += 1
        result = self._call_llm(prompt)
        if result is None:
            return None

        validated = validate_python_code(result)
        if validated is None:
            self._stats["validation_failures"] += 1
            return None

        self.cache.put(category, raw_plsql, version, validated)
        self._stats["todos_resolved"] += 1
        return validated

    def convert_for_loop(self, raw_plsql: str) -> str | None:
        """Convert a FOR cursor loop to PySpark via LLM.

        Returns valid Python code string, or None on failure.
        """
        category = "for_loop"
        version = FOR_LOOP_FALLBACK_VERSION

        cached = self.cache.get(category, raw_plsql, version)
        if cached is not None:
            self._stats["cache_hits"] += 1
            self._stats["todos_resolved"] += 1
            return cached

        prompt = FOR_LOOP_FALLBACK.format(raw_plsql=raw_plsql)

        self._stats["calls_made"] += 1
        result = self._call_llm(prompt)
        if result is None:
            return None

        validated = validate_python_code(result)
        if validated is None:
            self._stats["validation_failures"] += 1
            return None

        self.cache.put(category, raw_plsql, version, validated)
        self._stats["todos_resolved"] += 1
        return validated

    def fix_remaining_todos(self, generated_code: str) -> str:
        """Post-generation sweep: fix any remaining TODO comments in generated code.

        Returns the code with as many TODOs resolved as possible.
        """
        lines = generated_code.split("\n")
        result_lines = list(lines)

        i = 0
        while i < len(result_lines):
            line = result_lines[i]
            if "# TODO" not in line:
                i += 1
                continue

            # Gather surrounding context (5 lines before and after)
            start = max(0, i - 5)
            end = min(len(result_lines), i + 6)
            surrounding = "\n".join(result_lines[start:end])
            todo_line = line.strip()

            # Also grab the next line if it's a comment (often the raw SQL)
            todo_block_end = i + 1
            while todo_block_end < len(result_lines) and result_lines[todo_block_end].strip().startswith("#"):
                todo_block_end += 1

            todo_block = "\n".join(result_lines[i:todo_block_end])

            category = "todo_fix"
            version = TODO_FIX_VERSION

            cached = self.cache.get(category, todo_block, version)
            if cached is not None:
                self._stats["cache_hits"] += 1
                self._stats["todos_resolved"] += 1
                # Replace the TODO block with the fix
                indent = len(line) - len(line.lstrip())
                fixed_lines = _reindent(cached, indent).split("\n")
                result_lines[i:todo_block_end] = fixed_lines
                i += len(fixed_lines)
                continue

            prompt = TODO_FIX.format(
                surrounding_code=surrounding,
                todo_line=todo_block,
            )

            self._stats["calls_made"] += 1
            result = self._call_llm(prompt)
            if result is None:
                i = todo_block_end
                continue

            validated = validate_todo_fix(todo_block, result)
            if validated is None:
                self._stats["validation_failures"] += 1
                i = todo_block_end
                continue

            self.cache.put(category, todo_block, version, validated)
            self._stats["todos_resolved"] += 1

            indent = len(line) - len(line.lstrip())
            fixed_lines = _reindent(validated, indent).split("\n")
            result_lines[i:todo_block_end] = fixed_lines
            i += len(fixed_lines)

        return "\n".join(result_lines)

    @property
    def stats(self) -> dict:
        return {**self._stats, **{"cache_stats": self.cache.stats}}


def _reindent(code: str, target_indent: int) -> str:
    """Re-indent code to match a target indentation level."""
    lines = code.split("\n")
    if not lines:
        return code

    # Find current minimum indentation
    min_indent = float("inf")
    for line in lines:
        if line.strip():
            min_indent = min(min_indent, len(line) - len(line.lstrip()))
    if min_indent == float("inf"):
        min_indent = 0

    # Re-indent
    prefix = " " * target_indent
    result = []
    for line in lines:
        if line.strip():
            result.append(prefix + line[min_indent:])
        else:
            result.append("")
    return "\n".join(result)
