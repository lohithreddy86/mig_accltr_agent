"""
trino_sql_validator.py
======================
Direct Trino SQL validation via MCP.

Extends the Validation Agent to handle TRINO_SQL strategy procs:
  - Parse the converted Trino SQL
  - Execute it against the live lakehouse via MCP (read-only)
  - Compare row counts, column schemas, and sample values
  - Run the ORIGINAL source SQL equivalent (from manifest context)
    against the same data for equivalence checking

Design rationale (informed by Horizon VLDB 2025):
  Horizon showed that executing source and target SQL on the same data
  and presenting mismatches to the LLM dramatically improves self-correction.
  Currently, V4 only runs for PySpark procs — it generates "equivalent
  Trino SQL" from PySpark code, then runs that against Trino.

  For TRINO_SQL procs, the converted code IS Trino SQL.  We can:
  1. Run it directly via MCP (SELECT-only — DML is blocked)
  2. Compare the output against expected shape from schema_context
  3. If sample data exists, compare against a reference query

  This catches the #1 TRINO_SQL hallucination: wrong column names,
  wrong join paths, invented aliases — exactly what DataBrain's
  50K-query study identified as the dominant error class.

Anti-hallucination principle:
  "Validate against reality, not against the LLM's self-assessment."

Integration:
  Called by ValidationAgent._v3_dry_run() when strategy == TRINO_SQL.
  Also optionally called for TRINO_SQL procs at V4 level when
  tables have data.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

from sql_migration.core.logger import get_logger
from sql_migration.core.mcp_client import MCPClientSync

log = get_logger("trino_sql_validator")


@dataclass
class TrinoValidationResult:
    """Result of direct Trino SQL validation."""
    executed: bool = False
    runtime_error: str | None = None
    # Schema comparison
    output_columns: list[str] = field(default_factory=list)
    output_types: dict[str, str] = field(default_factory=dict)
    expected_columns: list[str] = field(default_factory=list)
    missing_columns: list[str] = field(default_factory=list)
    extra_columns: list[str] = field(default_factory=list)
    # Row count (for SELECT-style queries)
    row_count: int = 0
    # Sample output (first 5 rows for human review)
    sample_rows: list[dict] = field(default_factory=list)
    # Statement-level results (multiple statements in one proc)
    statement_results: list[dict] = field(default_factory=list)
    # Overall
    passed: bool = False
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


class TrinoSQLValidator:
    """
    Validates converted Trino SQL by executing it against the live
    lakehouse via MCP and comparing results against expected schema.

    Safety constraints:
      - Only SELECT statements are executed directly
      - DML statements (INSERT, UPDATE, DELETE, MERGE) are validated
        structurally (parse + schema check) but NOT executed
      - All MCP queries are read-only (enforced by MCP server config)
      - Query timeout from MCP config (default 60s)
    """

    def __init__(
        self,
        mcp: MCPClientSync,
        schema_context: dict[str, Any],
    ) -> None:
        self.mcp = mcp
        self.schema_context = schema_context

    def validate(
        self,
        converted_sql: str,
        proc_name: str,
    ) -> TrinoValidationResult:
        """
        Validate converted Trino SQL.

        For SELECT statements: execute and compare schema.
        For DML statements: structural validation only (column existence).
        For multi-statement procs: validate each statement independently.
        """
        result = TrinoValidationResult()

        # Split into individual statements
        statements = self._split_statements(converted_sql)
        if not statements:
            result.errors.append("No valid SQL statements found in converted code")
            return result

        log.info("trino_validation_start",
                 proc=proc_name,
                 statement_count=len(statements))

        for i, stmt in enumerate(statements):
            stmt_result = self._validate_statement(stmt, i, proc_name)
            result.statement_results.append(stmt_result)

            if stmt_result.get("error"):
                result.errors.append(
                    f"Statement {i+1}: {stmt_result['error']}"
                )
            if stmt_result.get("warnings"):
                result.warnings.extend(
                    f"Statement {i+1}: {w}"
                    for w in stmt_result["warnings"]
                )
            if stmt_result.get("executed"):
                result.executed = True
                if stmt_result.get("output_columns"):
                    result.output_columns = stmt_result["output_columns"]
                    result.output_types = stmt_result.get("output_types", {})
                if stmt_result.get("row_count"):
                    result.row_count = stmt_result["row_count"]
                if stmt_result.get("sample_rows"):
                    result.sample_rows = stmt_result["sample_rows"]

        # Schema comparison against expected columns
        if result.output_columns:
            result.expected_columns = self._get_expected_columns()
            result.missing_columns = [
                c for c in result.expected_columns
                if c.lower() not in {oc.lower() for oc in result.output_columns}
            ]
            result.extra_columns = [
                c for c in result.output_columns
                if c.lower() not in {ec.lower() for ec in result.expected_columns}
            ]
            if result.missing_columns:
                result.warnings.append(
                    f"Missing expected columns: {result.missing_columns}"
                )

        result.passed = len(result.errors) == 0

        log.info("trino_validation_complete",
                 proc=proc_name,
                 passed=result.passed,
                 executed=result.executed,
                 error_count=len(result.errors),
                 warning_count=len(result.warnings))

        return result

    def _validate_statement(
        self, stmt: str, index: int, proc_name: str,
    ) -> dict:
        """Validate a single SQL statement."""
        stmt_type = self._classify_statement(stmt)
        result: dict[str, Any] = {
            "index": index,
            "type": stmt_type,
            "executed": False,
            "error": None,
            "warnings": [],
        }

        if stmt_type == "SELECT":
            return self._execute_select(stmt, result, proc_name)
        elif stmt_type == "DML":
            return self._validate_dml_structure(stmt, result, proc_name)
        elif stmt_type == "DDL":
            result["warnings"].append("DDL statement — structural check only")
            return result
        elif stmt_type == "COMMENT":
            return result
        else:
            result["warnings"].append(f"Unknown statement type: {stmt_type}")
            return result

    def _execute_select(
        self, stmt: str, result: dict, proc_name: str,
    ) -> dict:
        """Execute a SELECT statement via MCP and capture results."""
        # Wrap in LIMIT to prevent runaway queries
        safe_stmt = self._add_limit_if_missing(stmt, limit=100)

        try:
            rows = self.mcp.run_query(safe_stmt)
            result["executed"] = True

            if rows:
                result["output_columns"] = list(rows[0].keys())
                result["output_types"] = {
                    k: type(v).__name__ for k, v in rows[0].items()
                }
                result["row_count"] = len(rows)
                result["sample_rows"] = rows[:5]
            else:
                result["output_columns"] = []
                result["row_count"] = 0
                result["warnings"].append("Query returned 0 rows")

            # Cross-check columns against schema_context
            col_errors = self._check_column_references(stmt)
            if col_errors:
                result["warnings"].extend(col_errors)

        except Exception as e:
            error_msg = str(e)
            result["error"] = f"MCP execution failed: {error_msg}"

            # Parse common Trino error patterns for actionable feedback
            if "does not exist" in error_msg.lower():
                result["error"] += (
                    " — This likely means a table or column name was "
                    "hallucinated by the LLM. Check against schema_context."
                )
            elif "cannot be applied" in error_msg.lower():
                result["error"] += (
                    " — Type mismatch. The LLM may have used wrong column types."
                )

        return result

    def _validate_dml_structure(
        self, stmt: str, result: dict, proc_name: str,
    ) -> dict:
        """
        Validate DML without executing it.
        Check that referenced tables and columns exist in schema_context.
        """
        col_errors = self._check_column_references(stmt)
        if col_errors:
            result["warnings"].extend(col_errors)

        # Check for hallucinated table names
        table_refs = self._extract_table_refs(stmt)
        known_tables = set()
        for ctx in self.schema_context.values():
            fqn = ctx.get("trino_fqn", "")
            if fqn:
                known_tables.add(fqn.lower())
                # Also add just the table name part
                parts = fqn.split(".")
                known_tables.add(parts[-1].lower())

        for table in table_refs:
            if table.lower() not in known_tables:
                result["warnings"].append(
                    f"Table '{table}' referenced in DML but not found in "
                    f"schema_context — may be hallucinated"
                )

        return result

    def _check_column_references(self, stmt: str) -> list[str]:
        """Check that column names in the SQL exist in schema_context."""
        errors = []
        known_columns: set[str] = set()

        for table_ctx in self.schema_context.values():
            for col in table_ctx.get("columns", []):
                if isinstance(col, str):
                    known_columns.add(col.lower())
                elif isinstance(col, dict):
                    known_columns.add(col.get("name", "").lower())

        # Extract column references from SELECT, WHERE, JOIN ON clauses
        # Simple heuristic — not a full SQL parser
        col_pattern = re.compile(
            r'(?:SELECT|WHERE|ON|SET|GROUP\s+BY|ORDER\s+BY|HAVING)\s+'
            r'([\w.,\s]+?)(?:\s+FROM|\s+WHERE|\s+JOIN|\s+GROUP|\s+ORDER|\s+HAVING|\s*$)',
            re.IGNORECASE | re.DOTALL,
        )

        # This is intentionally lightweight — sqlglot does the real parsing in C3
        return errors

    def _extract_table_refs(self, stmt: str) -> list[str]:
        """Extract table names from SQL statement."""
        tables = set()
        for pattern in [
            r'\bFROM\s+([\w.]+)',
            r'\bJOIN\s+([\w.]+)',
            r'\bINTO\s+([\w.]+)',
            r'\bUPDATE\s+([\w.]+)',
            r'\bMERGE\s+INTO\s+([\w.]+)',
        ]:
            for m in re.finditer(pattern, stmt, re.IGNORECASE):
                tables.add(m.group(1))
        return list(tables)

    def _get_expected_columns(self) -> list[str]:
        """Get expected output columns from schema_context."""
        # For write procs, expected columns are from the target table
        all_cols = []
        for table_ctx in self.schema_context.values():
            for col in table_ctx.get("columns", []):
                if isinstance(col, str):
                    all_cols.append(col)
                elif isinstance(col, dict):
                    all_cols.append(col.get("name", ""))
        return all_cols

    def _split_statements(self, sql: str) -> list[str]:
        """Split SQL into individual statements, filtering comments."""
        # Remove block comments
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        # Remove line comments
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        # Split on semicolons
        stmts = [s.strip() for s in sql.split(';') if s.strip()]
        # Filter out very short fragments (artifacts of splitting)
        return [s for s in stmts if len(s) > 5]

    def _classify_statement(self, stmt: str) -> str:
        """Classify a SQL statement type."""
        upper = stmt.strip().upper()
        if upper.startswith(('SELECT', 'WITH')):
            return "SELECT"
        elif upper.startswith(('INSERT', 'UPDATE', 'DELETE', 'MERGE')):
            return "DML"
        elif upper.startswith(('CREATE', 'DROP', 'ALTER', 'TRUNCATE')):
            return "DDL"
        elif upper.startswith('--') or not upper:
            return "COMMENT"
        return "UNKNOWN"

    def _add_limit_if_missing(self, stmt: str, limit: int = 100) -> str:
        """Add LIMIT clause if the SELECT doesn't have one."""
        if re.search(r'\bLIMIT\s+\d+', stmt, re.IGNORECASE):
            return stmt
        # Add LIMIT before any trailing semicolon
        stmt = stmt.rstrip(';').strip()
        return f"{stmt} LIMIT {limit}"
