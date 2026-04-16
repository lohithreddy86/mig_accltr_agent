"""
prompt_builder.py
=================
Builds structured agent prompts from deterministic converter metadata.

Instead of asking the LLM to simultaneously classify AND convert Oracle SQL,
this builder provides pre-classified, pre-transpiled context so the LLM only
handles the residual work (TODOs, semantic understanding, complex patterns).

SCOPE GUARD: This module is used ONLY for PySpark conversion strategies.
It is NEVER used for TRINO_SQL strategy or query mode.

Two prompt modes:
  1. REPAIR mode: The converter produced code with TODOs. The prompt focuses
     the LLM on fixing specific TODO blocks while preserving surrounding code.
  2. FULL mode: The converter failed or scored too low. The prompt provides
     pre-transpiled SQL and statement classification to help the LLM.
"""

from __future__ import annotations

import json
from typing import Any

from .interchange import ConversionArtifact, ConversionRoute, TodoItem


def build_repair_prompt(artifact: ConversionArtifact,
                        schema_context: dict[str, Any] | None = None,
                        oracle_mappings: dict[str, Any] | None = None,
                        target_context: dict[str, Any] | None = None) -> str:
    """Build a focused repair prompt for DETERMINISTIC_THEN_REPAIR route.

    The LLM receives the converter's PySpark output with TODO markers and is
    asked to fix ONLY the TODO sections. Surrounding code is frozen.

    Args:
        artifact: ConversionArtifact from the deterministic converter
        schema_context: Trino table schemas (from Planning P3)
        oracle_mappings: Relevant entries from oracle_mappings.json
    """
    todo_blocks = _format_todo_blocks(artifact.todos)
    schema_block = _format_schema_context(schema_context) if schema_context else ""
    target_block = _format_target_context(target_context) if target_context else ""
    mappings_block = _format_oracle_mappings(oracle_mappings) if oracle_mappings else ""

    return f"""You are fixing TODO markers in auto-generated PySpark code that was converted from Oracle PL/SQL.

The deterministic converter has already handled most of the conversion. Your job is to fix ONLY the TODO-marked sections. Do NOT modify any code outside the TODO blocks.

## Procedure: {artifact.proc_name}
## Confidence: {artifact.confidence_score}/100 ({artifact.confidence_tier})
## TODOs to fix: {artifact.todo_count}

## TODO Blocks to Fix

{todo_blocks}

## Generated PySpark Code (fix TODOs in place)

```python
{artifact.generated_pyspark}
```

{schema_block}
{target_block}
{mappings_block}

## Rules
1. Fix ONLY the lines marked with # TODO
2. Do NOT modify any surrounding code that is already converted
3. Use spark.sql() for SQL operations
4. Do NOT use Oracle-specific functions (NVL, DECODE, SYSDATE, ROWNUM)
5. Preserve the function signature and decorator
6. Output the COMPLETE fixed function (not just the TODO sections)
"""


def build_full_prompt(artifact: ConversionArtifact,
                      schema_context: dict[str, Any] | None = None,
                      oracle_mappings: dict[str, Any] | None = None,
                      chunk_code: str = "",
                      target_context: dict[str, Any] | None = None) -> str:
    """Build a structured prompt for AGENTIC_FULL route.

    The converter failed or scored too low, but we still provide pre-transpiled
    SQL and statement classification to help the LLM produce better output.

    Args:
        artifact: ConversionArtifact (may have empty generated_pyspark)
        schema_context: Trino table schemas
        oracle_mappings: Relevant oracle_mappings.json entries
        chunk_code: Original Oracle SQL chunk (raw, for context)
    """
    # Use pre-transpiled SQL if available, otherwise original
    source_sql = artifact.preprocessed_sql or chunk_code or artifact.original_plsql

    classifications_block = _format_classifications(artifact.statement_classifications)
    schema_block = _format_schema_context(schema_context) if schema_context else ""
    target_block = _format_target_context(target_context) if target_context else ""
    mappings_block = _format_oracle_mappings(oracle_mappings) if oracle_mappings else ""
    warnings_block = _format_warnings(artifact.warnings)

    return f"""Convert this Oracle PL/SQL procedure to PySpark Python.

## Procedure: {artifact.proc_name}
## Type: {artifact.unit_type}

## Pre-Processed Source SQL
The following SQL has been partially pre-transpiled from Oracle to Spark syntax.
Oracle functions like NVL, DECODE, SYSDATE have already been converted.
DB link references (@dblink) have been stripped.
Optimizer hints have been removed.

```sql
{source_sql}
```

## Statement Classification
Each statement has been pre-classified by type. Use these classifications
to select the correct conversion pattern.

{classifications_block}

{schema_block}
{target_block}
{mappings_block}
{warnings_block}

## Conversion Rules
1. Use spark.sql() for all SQL operations
2. All Oracle functions have been pre-converted — do NOT re-apply Oracle syntax
3. COMMIT/ROLLBACK → comment (Spark writes are atomic)
4. EXCEPTION WHEN OTHERS → try/except Exception
5. EXECUTE IMMEDIATE 'truncate table X' → spark.sql("TRUNCATE TABLE X")
6. DBMS_STATS.Gather_Table_Stats → spark.sql("ANALYZE TABLE ... COMPUTE STATISTICS")
7. Correlated UPDATE → MERGE INTO (if not already converted)
8. FOR cursor_var IN (SELECT ...) LOOP → df = spark.sql(); for row in df.collect():
9. **Write-target routing (CRITICAL when TARGET TABLE LOCATIONS block is present):**
   Every INSERT INTO / UPDATE / DELETE FROM / MERGE INTO / CREATE TABLE AS / saveAsTable / insertInto / writeTo target MUST use the exact FQN listed under TARGET TABLE LOCATIONS. Do NOT shorten, substitute, or use bare table names for writes. Read statements keep using the TABLE SCHEMA block's FQNs.

## Output
Produce a complete PySpark function. Include all imports at the top.
"""


# ---------------------------------------------------------------------------
# Internal formatters
# ---------------------------------------------------------------------------

def _format_todo_blocks(todos: list[TodoItem]) -> str:
    """Format TODO items for the repair prompt."""
    if not todos:
        return "(No TODOs — code is complete)"

    lines = []
    for i, todo in enumerate(todos, 1):
        lines.append(f"### TODO {i} (line {todo.line})")
        lines.append(f"**Type:** {todo.construct_type}")
        lines.append(f"**Comment:** {todo.comment}")
        if todo.original_sql:
            lines.append(f"**Original Oracle SQL:**")
            lines.append(f"```sql\n{todo.original_sql}\n```")
        lines.append("")
    return "\n".join(lines)


def _format_classifications(classifications: list) -> str:
    """Format statement classifications as a table."""
    if not classifications:
        return "(No classifications available)"

    lines = ["| # | Type | Target Table | Oracle Constructs | DB Link |"]
    lines.append("|---|------|-------------|-------------------|---------|")
    for i, c in enumerate(classifications, 1):
        constructs = ", ".join(c.oracle_constructs[:5]) if c.oracle_constructs else "-"
        lines.append(
            f"| {i} | {c.statement_type} | {c.target_table or '-'} "
            f"| {constructs} | {'Yes' if c.has_db_link else '-'} |"
        )
    return "\n".join(lines)


def _format_schema_context(schema: dict[str, Any]) -> str:
    """Format Trino table schemas for the prompt."""
    if not schema:
        return ""

    lines = ["\n## Table Schemas (from Trino)"]
    for table_name, columns in schema.items():
        lines.append(f"\n**{table_name}:**")
        if isinstance(columns, list):
            for col in columns[:30]:  # Limit columns shown
                if isinstance(col, dict):
                    lines.append(f"  - {col.get('name', '?')}: {col.get('type', '?')}")
                else:
                    lines.append(f"  - {col}")
    return "\n".join(lines)


def _format_target_context(target: dict[str, Any]) -> str:
    """Format the TARGET TABLE LOCATIONS allow-list for the prompt.

    Expected shape (populated by Planning P3):
        {source_name: {"target_fqn": "...", "columns": [...], "types": {...},
                        "role": "target|both", "target_status": "EXISTS|NEEDS_CREATE"}}

    Emits a block the Conversion LLM must obey: every write-verb (INSERT,
    UPDATE, DELETE, MERGE, CTAS, saveAsTable, insertInto, writeTo) MUST use
    one of the listed FQNs.
    """
    if not target:
        return ""
    lines = ["\n## TARGET TABLE LOCATIONS — writes MUST use these FQNs"]
    for source_name, spec in target.items():
        if not isinstance(spec, dict):
            continue
        tgt = spec.get("target_fqn", "")
        if not tgt:
            continue
        role = spec.get("role", "target")
        status = spec.get("target_status", "")
        lines.append(f"\n**{source_name}** → `{tgt}`  (role={role}, status={status})")
        cols = spec.get("columns") or []
        types = spec.get("types") or {}
        if cols:
            preview = ", ".join(
                f"{c} {types.get(c, '')}".strip() for c in cols[:12]
            )
            lines.append(f"  columns: {preview}")
    lines.append(
        "\nEvery INSERT INTO / UPDATE / DELETE FROM / MERGE INTO / CREATE TABLE AS / "
        "saveAsTable / insertInto / writeTo target in the converted code MUST be one "
        "of the FQNs above — not the source FQN, not a bare name, not @dblink-suffixed."
    )
    return "\n".join(lines)


def _format_oracle_mappings(mappings: dict[str, Any]) -> str:
    """Format relevant Oracle→Spark mappings."""
    if not mappings:
        return ""

    lines = ["\n## Oracle → Spark Function Reference"]
    for func_name, details in mappings.items():
        if isinstance(details, dict):
            spark_eq = details.get("spark_sql", details.get("trino", ""))
            notes = details.get("notes", "")
            lines.append(f"- **{func_name}** → `{spark_eq}`{f' ({notes})' if notes else ''}")
        else:
            lines.append(f"- **{func_name}** → `{details}`")
    return "\n".join(lines)


def _format_warnings(warnings: list[str]) -> str:
    """Format converter warnings for the prompt."""
    if not warnings:
        return ""

    lines = ["\n## Warnings from Deterministic Analysis"]
    for w in warnings:
        lines.append(f"- {w}")
    return "\n".join(lines)
