"""
preprocessor.py
===============
Deterministic Oracle SQL preprocessing for PySpark conversion.

Wraps the converter pipeline's transpilation engine to pre-process Oracle SQL
BEFORE the Conversion Agent's LLM sees it. This eliminates ~70% of mechanical
translation work (NVL→COALESCE, DECODE→CASE, SYSDATE→CURRENT_TIMESTAMP, etc.)
from the LLM's workload, producing higher-fidelity conversion.

SCOPE GUARD: This module is activated ONLY for PySpark conversion strategies
(PYSPARK_DF, PYSPARK_PIPELINE). It is NEVER called for TRINO_SQL strategy
or query mode. The guard is enforced by the Conversion Agent, not here.

Usage:
    from sql_migration.deterministic.preprocessor import OraclePreprocessor

    preprocessor = OraclePreprocessor()
    result = preprocessor.preprocess(oracle_sql)
    # result.transpiled_sql — pre-transpiled SQL with Oracle constructs converted
    # result.transformations_applied — list of what was changed
    # result.constructs_remaining — Oracle constructs still present

    artifact = preprocessor.run_full_conversion(proc_source)
    # artifact.generated_pyspark — full PySpark code
    # artifact.confidence_score — 0-100
    # artifact.route — DETERMINISTIC_ACCEPT / DETERMINISTIC_THEN_REPAIR / AGENTIC_FULL
"""

from __future__ import annotations

import os
import re
from typing import Any

from .interchange import (
    ConversionArtifact,
    ConversionRoute,
    PreprocessedSQL,
    StatementClassification,
    TodoItem,
    compute_route,
)

# ---------------------------------------------------------------------------
# Import converter modules — the converter is vendored as a subpackage at
# sql_migration.deterministic.converter. No external path dependency.
# ---------------------------------------------------------------------------

_converter_available = False
try:
    from .converter.transpiler import (
        transpile_sql,
        transpile_correlated_update,
        transpile_execute_immediate,
        TranspileResult,
    )
    from .converter.sql_extractor import (
        extract_statements,
        get_statement_summary,
        set_status_table_pattern,
        StatementType,
    )
    from .converter.plsql_splitter import (
        split_package,
        extract_procedure_body,
        ProcedureInfo,
    )
    from .converter.router import analyze_complexity, select_path, ConversionPath
    from .converter.regex_to_ir import statements_to_ir
    from .converter.ir_code_generator import generate_procedure_from_ir
    from .converter.confidence_scorer import score_procedure, ConfidenceReport
    from .converter.main import (
        _detect_package_name,
        _detect_status_table,
        _detect_orchestrator_proc,
    )
    _converter_available = True
except ImportError as e:
    import warnings
    warnings.warn(
        f"Deterministic converter not available: {e}. "
        f"PySpark conversion will use agentic-only path.",
        stacklevel=2,
    )


def is_converter_available() -> bool:
    """Check if the deterministic converter is importable."""
    return _converter_available


class OraclePreprocessor:
    """
    Deterministic Oracle SQL preprocessor for PySpark conversion.

    Applies the converter's 17 ordered transpilation rules to Oracle SQL
    before the LLM sees it. Also provides full converter pipeline execution
    for first-pass code generation.
    """

    def __init__(self) -> None:
        if not _converter_available:
            raise RuntimeError(
                "Deterministic converter is not available. "
                "Ensure APP/converter/ is present and importable."
            )

    # ------------------------------------------------------------------
    # Lightweight preprocessing (transpilation rules only)
    # ------------------------------------------------------------------

    def preprocess(self, oracle_sql: str) -> PreprocessedSQL:
        """Apply deterministic transpilation rules to Oracle SQL.

        This converts mechanical Oracle constructs (NVL, DECODE, SYSDATE,
        ROWNUM, @dblink, hints, etc.) to Spark SQL equivalents. The output
        is partially-transpiled SQL that the LLM can work with more accurately.

        Args:
            oracle_sql: Raw Oracle SQL (a single statement or block)

        Returns:
            PreprocessedSQL with transpiled SQL and metadata
        """
        result = transpile_sql(oracle_sql)
        return PreprocessedSQL(
            original_sql=oracle_sql,
            transpiled_sql=result.transpiled,
            transformations_applied=[
                f"Method: {result.method}",
            ] + [f"Warning: {w}" for w in result.warnings],
            warnings=result.warnings,
            constructs_remaining=self._detect_remaining_constructs(result.transpiled),
        )

    def preprocess_correlated_update(self, oracle_sql: str) -> PreprocessedSQL:
        """Pre-process a correlated UPDATE → MERGE conversion."""
        # First apply standard preprocessing
        pre = self.preprocess(oracle_sql)
        # Then attempt MERGE conversion
        merge_sql = transpile_correlated_update(pre.transpiled_sql)
        has_todo = "TODO" in merge_sql
        return PreprocessedSQL(
            original_sql=oracle_sql,
            transpiled_sql=merge_sql,
            transformations_applied=pre.transformations_applied + [
                "Correlated UPDATE → MERGE" if not has_todo else "Correlated UPDATE → MERGE (partial)"
            ],
            warnings=pre.warnings + (["MERGE conversion incomplete — TODO remains"] if has_todo else []),
            constructs_remaining=self._detect_remaining_constructs(merge_sql),
        )

    # ------------------------------------------------------------------
    # Full converter pipeline (first-pass PySpark generation)
    # ------------------------------------------------------------------

    def run_full_conversion(self, proc_source: str,
                            proc_name: str = "",
                            status_table: str | None = None) -> ConversionArtifact:
        """Run the full deterministic converter on a single procedure.

        This produces complete PySpark code with confidence scoring and TODO
        markers for unresolved constructs.

        Args:
            proc_source: Raw PL/SQL procedure source (PROCEDURE ... END name;)
            proc_name: Procedure name (auto-detected if empty)
            status_table: Status tracking table name (auto-detected if None)

        Returns:
            ConversionArtifact with generated code, confidence, and routing decision
        """
        # Configure status table detection
        if status_table:
            set_status_table_pattern(status_table)

        # Parse the procedure
        try:
            procedures = split_package(proc_source)
        except Exception:
            return self._make_failure_artifact(proc_name or "unknown", proc_source,
                                                "parse_failure")

        # When the Conversion Agent passes a chunk that is *just the body* of a
        # package-internal procedure (no leading `Procedure name Is`), the
        # splitter finds zero procedures. Wrap the body in a synthetic
        # `Procedure <name> Is ... End <name>;` and retry. This is a generic
        # robustness step — it lets the deterministic path produce real code
        # for partial chunks instead of silently routing them to the full LLM
        # loop.
        if not procedures:
            wrapper_name = re.sub(r"\W", "_", proc_name or "temp_proc") or "temp_proc"
            wrapped = self._wrap_body_as_procedure(proc_source, wrapper_name)
            if wrapped is not None:
                try:
                    procedures = split_package(wrapped)
                except Exception:
                    procedures = []
                if procedures:
                    proc_source = wrapped   # use the wrapped form downstream

        if not procedures:
            return self._make_failure_artifact(proc_name or "unknown", proc_source,
                                                "no_procedures_found")

        proc = procedures[0]
        if proc_name:
            # Find the named procedure if multiple
            for p in procedures:
                if p.name.lower() == proc_name.lower():
                    proc = p
                    break

        # Extract body and classify statements
        body = extract_procedure_body(proc)
        statements = extract_statements(body)
        summary = get_statement_summary(statements)

        # Route complexity
        sig = analyze_complexity(proc.raw_text)
        selected_path = select_path(sig)

        # Determine status name
        status_name = ""
        for stmt in statements:
            if stmt.type == StatementType.STATUS_INSERT:
                match = re.search(r"'(\w+)'", stmt.sql)
                if match:
                    status_name = match.group(1)
                    break
        if not status_name:
            status_name = proc.name.upper()

        # Build IR and generate code
        try:
            if selected_path == ConversionPath.ANTLR:
                try:
                    from .converter.antlr_bridge import parse_procedure_to_ir
                    proc_ir = parse_procedure_to_ir(proc, status_name)
                except Exception:
                    proc_ir = statements_to_ir(proc, statements, status_name)
                    proc_ir.conversion_path = "antlr-fallback"
            else:
                proc_ir = statements_to_ir(proc, statements, status_name)

            pyspark_code = generate_procedure_from_ir(proc_ir)
            conversion_path = proc_ir.conversion_path
        except Exception as e:
            return self._make_failure_artifact(proc.name, proc_source,
                                                f"codegen_failure: {e}")

        # Score confidence
        confidence = score_procedure(proc.name, statements)

        # Extract TODOs from generated code
        todos = self._extract_todos(pyspark_code)

        # Build statement classifications
        classifications = [
            StatementClassification(
                statement_type=s.type.value,
                sql=s.sql[:500],  # Truncate long SQL for artifact size
                target_table=s.target_table or "",
                source_tables=s.source_tables or [],
                oracle_constructs=s.oracle_constructs,
                has_db_link=s.has_db_link,
                is_correlated_update=(s.type == StatementType.UPDATE_CORRELATED),
            )
            for s in statements
        ]

        # Compute route
        route = compute_route(
            confidence_score=confidence.score,
            confidence_tier=confidence.tier,
            todo_count=len(todos),
            parse_failed=False,
        )

        # Pre-process the full body for AGENTIC_FULL fallback
        preprocessed = self.preprocess(body) if route == ConversionRoute.AGENTIC_FULL else None

        # Clean up status table pattern
        set_status_table_pattern(None)

        return ConversionArtifact(
            proc_name=proc.name,
            unit_type=proc.unit_type,
            original_plsql=proc_source,
            generated_pyspark=pyspark_code,
            confidence_score=confidence.score,
            confidence_tier=confidence.tier,
            route=route,
            todos=todos,
            oracle_constructs_remaining=[
                c for c in summary.get("construct_counts", {}).keys()
                if c in ("ROWID", "REMOTE_PROC", "CURSOR_LOOP")
            ],
            conversion_path=conversion_path,
            warnings=[f"Risk: {r}" for r in confidence.risk_factors],
            tables_read=summary.get("tables_read", []),
            tables_written=summary.get("tables_written", []),
            db_links_stripped=self._extract_db_links(proc_source),
            statement_classifications=classifications,
            preprocessed_sql=preprocessed.transpiled_sql if preprocessed else "",
        )

    # ------------------------------------------------------------------
    # Classify statements for structured agent prompts
    # ------------------------------------------------------------------

    def classify_statements(self, procedure_body: str) -> list[StatementClassification]:
        """Classify SQL statements in a procedure body.

        Returns structured metadata the agent can use for context-aware conversion.
        """
        statements = extract_statements(procedure_body)
        return [
            StatementClassification(
                statement_type=s.type.value,
                sql=s.sql,
                target_table=s.target_table or "",
                source_tables=s.source_tables or [],
                oracle_constructs=s.oracle_constructs,
                has_db_link=s.has_db_link,
                is_correlated_update=(s.type == StatementType.UPDATE_CORRELATED),
            )
            for s in statements
        ]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _detect_remaining_constructs(self, sql: str) -> list[str]:
        """Detect Oracle constructs that survived preprocessing."""
        remaining = []
        oracle_patterns = {
            "NVL": r"\bNVL\s*\(",
            "NVL2": r"\bNVL2\s*\(",
            "DECODE": r"\bDECODE\s*\(",
            "SYSDATE": r"\bSYSDATE\b",
            "ROWNUM": r"\bROWNUM\b",
            "ROWID": r"\bROWID\b",
            "DB_LINK": r"@\w+",
            "OUTER_JOIN_PLUS": r"\(\+\)",
        }
        for name, pattern in oracle_patterns.items():
            if re.search(pattern, sql, re.IGNORECASE):
                remaining.append(name)
        return remaining

    def _extract_todos(self, code: str) -> list[TodoItem]:
        """Extract TODO markers from generated PySpark code."""
        todos = []
        lines = code.split("\n")
        for i, line in enumerate(lines):
            if "# TODO" in line:
                # Gather context: next line often has the original SQL
                original_sql = ""
                if i + 1 < len(lines) and lines[i + 1].strip().startswith("#"):
                    original_sql = lines[i + 1].strip().lstrip("# ")
                todos.append(TodoItem(
                    line=i + 1,
                    comment=line.strip(),
                    original_sql=original_sql,
                    construct_type=self._classify_todo(line),
                ))
        return todos

    def _classify_todo(self, todo_line: str) -> str:
        """Classify a TODO by its content."""
        lower = todo_line.lower()
        if "correlated update" in lower or "merge" in lower:
            return "CORRELATED_UPDATE"
        if "remote procedure" in lower:
            return "REMOTE_CALL"
        if "dynamic sql" in lower or "execute immediate" in lower:
            return "DYNAMIC_SQL"
        if "for loop" in lower or "cursor" in lower:
            return "CURSOR_LOOP"
        if "scheduler" in lower or "dbms_scheduler" in lower:
            return "SCHEDULER"
        if "unclassified" in lower:
            return "UNCLASSIFIED"
        return "UNMAPPED_CONSTRUCT"

    def _extract_db_links(self, sql: str) -> list[str]:
        """Extract unique DB link names from SQL."""
        links = re.findall(r"@(\w+)", sql, re.IGNORECASE)
        return sorted(set(links))

    @staticmethod
    def _wrap_body_as_procedure(source: str, proc_name: str) -> str | None:
        """Wrap a raw PL/SQL body in a synthetic `Procedure name Is ... End name;`
        shell when the Conversion Agent hands us a chunk that's missing the
        declaration line.

        Heuristics:
          * If `source` already starts with `Procedure` or `Create or Replace
            Procedure`, no wrap is needed (returns None).
          * If `source` already contains a `Begin ... End;` structure, wrap as
            `Procedure <name> Is\n<source>\nEnd <name>;`.
          * Otherwise inject `Begin` and `End` as well.

        Safe to call on any Oracle source; keeps behaviour unchanged when the
        input is already a well-formed procedure.
        """
        if not source or not source.strip():
            return None
        stripped = source.lstrip()
        first_token_match = re.match(
            r"(?:create\s+or\s+replace\s+)?(?:procedure|function)\b",
            stripped,
            re.IGNORECASE,
        )
        if first_token_match:
            return None  # already a procedure declaration
        # Does the body have its own Begin/End block?
        has_begin = re.search(r"\bbegin\b", source, re.IGNORECASE)
        has_end = re.search(r"\bend\s*;", source, re.IGNORECASE)
        name = proc_name or "temp_proc"
        if has_begin and has_end:
            return f"Procedure {name} Is\n{source}\nEnd {name};\n"
        return f"Procedure {name} Is\nBegin\n{source}\nEnd {name};\n"

    def _make_failure_artifact(self, proc_name: str, source: str,
                                reason: str) -> ConversionArtifact:
        """Create an artifact for a converter failure (triggers AGENTIC_FULL)."""
        # Still preprocess the SQL so the agent gets a head start
        try:
            preprocessed = self.preprocess(source)
            preprocessed_sql = preprocessed.transpiled_sql
        except Exception:
            preprocessed_sql = source

        return ConversionArtifact(
            proc_name=proc_name,
            original_plsql=source,
            generated_pyspark="",
            confidence_score=0,
            confidence_tier="blocked",
            route=ConversionRoute.AGENTIC_FULL,
            conversion_path=reason,
            warnings=[f"Deterministic conversion failed: {reason}"],
            preprocessed_sql=preprocessed_sql,
        )
