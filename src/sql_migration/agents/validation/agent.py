"""
agent.py  (Validation Agent)
=============================
Implements Validation Agent steps V1 through V5:

  V1  MCP      — Data availability check (row counts per table)
                  Determines validation level (STATIC_ONLY / SCHEMA_PLUS_EXEC /
                  FULL_RECONCILIATION)
  V2  MCP+SBX  — Sample data extraction from Trino → parquet in sandbox
                  + synthetic edge-case rows when real data is missing
  V3  MCP/SBX  — Execution of converted code:
                  - TRINO_SQL: direct execution against live Trino via
                    TrinoSQLValidator (catches real column hallucinations)
                  - PySpark: sandbox dry-run against local Spark with samples
  V4  LLM+MCP  — Semantic diff (LEVEL_3, PySpark procs only):
                  - If a declared output_table exists in Trino, compares its
                    schema directly against sandbox output (no LLM needed)
                  - Otherwise: LLM generates equivalent Trino SQL, runs it on
                    real data, compares column schemas + row counts
  V5  SCORING  — Reconciliation report assembly → ValidationResult
                  (PASS / PARTIAL / FAIL based on configurable thresholds)

Completely dialect-agnostic — only ever sees converted Trino/PySpark code.
Never writes to any Trino table (MCP is read-only; sandbox has no outbound network).
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

from sql_migration.core.artifact_store import ArtifactStore
from sql_migration.core.config_loader import get_config
from sql_migration.core.llm_client import LLMClient
from sql_migration.core.logger import get_logger
from sql_migration.core.mcp_client import MCPClientSync
from sql_migration.core.sandbox import Sandbox
from sql_migration.models import (
    DispatchTask,
    ValidationResult,
)
from sql_migration.models.common import (
    ConversionStrategy,
    TableStatus,
    ValidationLevel,
    ValidationOutcome,
    TodoItem,
)
from sql_migration.models.pipeline import (
    ColumnDiff,
    ExecutionResult,
    build_llm_semantic_diff_input,
    parse_column_diffs,
)

log = get_logger("validation")


class ValidationAgent:
    """
    Validation Agent — verifies converted code against live Trino data.

    Validation depth scales with data availability:
      STATIC_ONLY           Tables missing → no execution, scoring only
      SCHEMA_PLUS_EXEC      Some tables have data → sandbox/Trino execution
      FULL_RECONCILIATION   All tables have sufficient data → execution +
                            semantic diff + row-count comparison

    TRINO_SQL procs are validated by executing directly against live Trino
    (not the sandbox), catching real schema mismatches.
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
        self.llm     = LLMClient(agent="validation")
        self.cfg     = get_config().validation
        self.global_cfg = get_config()

    # =========================================================================
    # Step progress (for Streamlit UI live sub-step display)
    # =========================================================================

    def _write_step_progress(self, proc: str, step: str,
                              detail: str = "", status: str = "RUNNING",
                              **extra) -> None:
        """Write step progress with history so the UI can show a timeline."""
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        try:
            filename = f"validation_progress_{proc}.json"
            try:
                existing = self.store.read("validation", filename)
                steps = existing.get("steps", [])
            except Exception:
                steps = []

            # Mark previous RUNNING step as DONE
            for s in steps:
                if s.get("status") == "RUNNING":
                    s["status"] = "DONE"
                    s["ended_at"] = now

            entry = {"step": step, "detail": detail, "status": status,
                     "started_at": now, **extra}
            steps.append(entry)

            self.store.write("validation", filename, {
                "proc": proc, "step": step, "detail": detail,
                "steps": steps,
                "updated_at": now,
            })
        except Exception:
            pass  # Progress is advisory — never block validation

    # =========================================================================
    # Public entrypoint
    # =========================================================================

    def run(self, proc_name: str, converted_code_path: str) -> ValidationResult:
        """
        Run full validation for a proc.
        Returns ValidationResult (PASS | PARTIAL | FAIL).
        """
        start = time.monotonic()
        log.info("validation_start", proc=proc_name)

        # Load proc context from artifacts
        plan_entry    = self._load_plan_entry(proc_name)
        strategy      = ConversionStrategy(plan_entry.get("strategy", "TRINO_SQL"))
        table_registry = self._load_table_registry()

        # Load output catalog/schema from dialect_profile for V4 FQN construction
        try:
            dp = self.store.read("analysis", "dialect_profile.json")
            output_catalog = dp.get("output_catalog", "")
            output_schemas = dp.get("output_schema", [])
        except Exception:
            output_catalog, output_schemas = "", []
        # v7: Collect tables from ALL chunks (important for multi-proc modules
        # where different procs reference different tables in different chunks)
        proc_tables = sorted(set(
            t for chunk in plan_entry.get("chunks", [])
            for t in chunk.get("tables", [])
        ))

        # v9: Declared output table from README proc_io_tables
        output_table = plan_entry.get("output_table", "")

        # Load converted code
        converted_code = self._load_converted_code(proc_name, strategy)
        if not converted_code:
            return self._fail_result(proc_name, "No converted code found",
                                     ValidationLevel.STATIC_ONLY)

        # Load TODO items from conversion log
        todo_items = self._load_todo_items(proc_name)

        # ── V1: Data availability ─────────────────────────────────────────────
        # TRINO_SQL: skip V1 (COUNT queries) — V3 runs against live Trino regardless.
        # PySpark: V1 determines level for V2 sample extraction + V3 gating.
        availability = {}
        val_level = ValidationLevel.STATIC_ONLY

        if strategy != ConversionStrategy.TRINO_SQL:
            self._write_step_progress(proc_name, "V1", "Checking data availability...")
            availability, val_level = self._v1_check_availability(
                proc_tables, table_registry
            )
            log.info("v1_complete",
                     proc=proc_name,
                     level=val_level.value,
                     tables=list(availability.keys()))

        # ── V2: Sample data extraction ────────────────────────────────────────
        sample_dir = None
        if strategy != ConversionStrategy.TRINO_SQL and val_level >= ValidationLevel.SCHEMA_PLUS_EXEC:
            self._write_step_progress(proc_name, "V2",
                                       f"Extracting samples (level {val_level.value})...")
            sample_dir = self._v2_extract_samples(
                proc_name, proc_tables, availability, table_registry, val_level
            )
            log.info("v2_complete", proc=proc_name, sample_dir=sample_dir)

        # ── V3: Dry-run execution ─────────────────────────────────────────────
        exec_result = None
        # TRINO_SQL: always run V3 — it executes against live Trino, not sandbox.
        # Validates column names, types, and syntax even on empty tables.
        # PySpark: needs sample data from V2, so respect the level gate.
        run_v3 = (
            strategy == ConversionStrategy.TRINO_SQL
            or (val_level >= ValidationLevel.SCHEMA_PLUS_EXEC and sample_dir)
        )
        if run_v3:
            step_msg = (
                "Executing against live Trino..."
                if strategy == ConversionStrategy.TRINO_SQL
                else "Dry-run in local PySpark..."
            )
            self._write_step_progress(proc_name, "V3", step_msg)
            exec_result = self._v3_dry_run(proc_name, converted_code,
                                            strategy, sample_dir)
            log.info("v3_complete",
                     proc=proc_name,
                     error=exec_result.runtime_error,
                     rows=exec_result.row_count)

        # ── V4: Semantic diff (LEVEL_3 SELECT procs only) ─────────────────────
        column_diffs: list[ColumnDiff] = []
        trino_row_count: int | None     = None

        if (
            val_level == ValidationLevel.FULL_RECONCILIATION
            and exec_result
            and not exec_result.runtime_error
            and strategy in {ConversionStrategy.PYSPARK_DF,
                             ConversionStrategy.PYSPARK_PIPELINE}
        ):
            self._write_step_progress(proc_name, "V4",
                                       "Semantic diff — LLM generating Trino SQL...")
            column_diffs, trino_row_count = self._v4_semantic_diff(
                proc_name, converted_code, proc_tables, table_registry,
                exec_result, output_catalog=output_catalog,
                output_schemas=output_schemas,
                output_table=output_table,
            )
            log.info("v4_complete",
                     proc=proc_name,
                     diff_cols=len(column_diffs),
                     trino_rows=trino_row_count)

        # ── V5: Assemble final report ─────────────────────────────────────────
        self._write_step_progress(proc_name, "V5", "Scoring final result...")
        result = self._v5_score(
            proc_name=proc_name,
            val_level=val_level,
            exec_result=exec_result,
            column_diffs=column_diffs,
            trino_row_count=trino_row_count,
            todo_items=todo_items,
        )

        # Write validation artifacts
        self.store.write("validation", f"validation_{proc_name}.json",
                         result.model_dump())
        if column_diffs:
            self.store.write(
                "validation", f"diff_{proc_name}.json",
                [d.model_dump() for d in column_diffs]
            )
        if todo_items:
            self.store.write(
                "validation", f"manual_review_{proc_name}.json",
                [t.model_dump() for t in todo_items]
            )

        self._write_step_progress(proc_name, "V5",
            f"Scored: {result.outcome.value}", status="DONE")
        log.info("validation_complete",
                 proc=proc_name,
                 outcome=result.outcome.value,
                 level=val_level.value,
                 duration_s=round(time.monotonic() - start, 2))
        return result

    # =========================================================================
    # V1 — Data availability
    # =========================================================================

    def _v1_check_availability(
        self,
        proc_tables:    list[str],
        table_registry: dict,
    ) -> tuple[dict[str, dict], ValidationLevel]:
        """
        Check row counts for each table and determine validation level.
        LEVEL_3 triggers when all tables have sufficient data in Trino —
        no CSV upload required.
        Returns (availability_map, validation_level).
        """
        availability: dict[str, dict] = {}
        has_sufficient_data = True

        for table in proc_tables:
            entry = table_registry.get(table, {})
            status = entry.get("status", "MISSING")

            if status in ("MISSING", "TEMP_TABLE"):
                availability[table] = {"row_count": 0, "status": status}
                has_sufficient_data = False
                continue

            try:
                trino_fqn = entry.get("trino_fqn", table)
                count = self.mcp.table_row_count(trino_fqn)
                availability[table] = {"row_count": count, "status": status, "trino_fqn": trino_fqn}
                if count < self.cfg.min_rows_for_level_3:
                    has_sufficient_data = False
            except Exception as e:
                log.warning("v1_count_failed", table=table, error=str(e))
                availability[table] = {"row_count": 0, "status": "ERROR"}
                has_sufficient_data = False

        # Determine level
        any_real_data = any(
            v.get("row_count", 0) > 0 for v in availability.values()
        )

        if not availability or not any_real_data:
            level = ValidationLevel.STATIC_ONLY
        elif has_sufficient_data:
            level = ValidationLevel.FULL_RECONCILIATION
        else:
            level = ValidationLevel.SCHEMA_PLUS_EXEC

        return availability, level

    # =========================================================================
    # V2 — Sample data extraction + synthetic rows
    # =========================================================================

    def _v2_extract_samples(
        self,
        proc_name:      str,
        proc_tables:    list[str],
        availability:   dict[str, dict],
        table_registry: dict,
        val_level:      ValidationLevel,
    ) -> str | None:
        """
        Pull sample rows from Trino, write as parquet, set up local Spark views.
        Returns path to sample directory (inside artifact store).
        """
        sample_dir = str(self.store.path("samples", proc_name))
        Path(sample_dir).mkdir(parents=True, exist_ok=True)
        limit = self.cfg.sample_row_count

        for table in proc_tables:
            info = availability.get(table, {})
            if info.get("row_count", 0) == 0:
                # Generate synthetic data from schema
                entry = table_registry.get(table, {})
                if entry.get("columns"):
                    self._write_synthetic_parquet(
                        sample_dir=sample_dir,
                        table_name=table,
                        columns=entry["columns"],
                        add_nulls=self.cfg.synthetic_data.add_null_row,
                    )
                continue

            trino_fqn = info.get("trino_fqn", table)
            try:
                rows = self.mcp.sample_rows(trino_fqn, limit=limit)
                if rows:
                    self._write_parquet_via_sandbox(
                        sample_dir=sample_dir,
                        table_name=table,
                        rows=rows,
                    )
            except Exception as e:
                log.warning("v2_sample_failed", table=table, error=str(e))

        return sample_dir

    def _write_parquet_via_sandbox(
        self,
        sample_dir: str,
        table_name: str,
        rows: list[dict],
    ) -> None:
        """Write sample rows to parquet via a sandbox script call."""
        result = self.sandbox.run(
            script="validation/run_validation.py",
            args={
                "mode":        "write_parquet",
                "sample_dir":  sample_dir,
                "table_name":  table_name,
                "rows":        rows,
            },
        )
        if not result.success:
            log.warning("write_parquet_failed",
                        table=table_name,
                        stderr=result.stderr[:200])
        else:
            # Write parquet directly (pandas, available in validation environment)
            self._write_parquet_direct(sample_dir, table_name, rows)

    def _write_parquet_direct(
        self,
        sample_dir: str,
        table_name: str,
        rows: list[dict],
    ) -> None:
        """Write rows as parquet using pandas (fallback, host-side)."""
        try:
            import pandas as pd
            df = pd.DataFrame(rows)
            out_path = Path(sample_dir) / f"{table_name}.parquet"
            df.to_parquet(str(out_path), index=False)
            log.debug("parquet_written",
                      table=table_name,
                      rows=len(rows),
                      path=str(out_path))
        except Exception as e:
            log.warning("parquet_write_failed", table=table_name, error=str(e))

    def _write_synthetic_parquet(
        self,
        sample_dir:   str,
        table_name:   str,
        columns:      list[dict],
        add_nulls:    bool = True,
    ) -> None:
        """Generate synthetic rows from column schema and write as parquet."""
        try:
            import pandas as pd

            def synthetic_value(col_type: str) -> object:
                t = col_type.lower()
                if "int" in t or "long" in t:  return 1
                if "float" in t or "double" in t or "decimal" in t: return 1.0
                if "bool" in t:  return True
                if "date" in t:  return "2024-01-01"
                if "timestamp" in t: return "2024-01-01 00:00:00"
                return "sample_value"

            row = {c.get("name", c.get("Column", f"col_{i}")):
                   synthetic_value(c.get("type", c.get("Type", "string")))
                   for i, c in enumerate(columns)}

            rows = [row]
            if add_nulls:
                null_row = {k: None for k in row}
                rows.append(null_row)

            df = pd.DataFrame(rows)
            out_path = Path(sample_dir) / f"{table_name}.parquet"
            df.to_parquet(str(out_path), index=False)
        except Exception as e:
            log.warning("synthetic_parquet_failed",
                        table=table_name, error=str(e))

    # =========================================================================
    # V3 — Dry-run execution
    # =========================================================================

    def _v3_dry_run(
        self,
        proc_name:      str,
        converted_code: str,
        strategy:       ConversionStrategy,
        sample_dir:     str,
    ) -> ExecutionResult:
        """
        Execute converted code in sandbox against local Spark.
        v5: TRINO_SQL procs validated via direct MCP execution against live Trino.
        Returns ExecutionResult with schema, row count, and any runtime errors.
        """
        # ── v5: TRINO_SQL direct validation via MCP ───────────────────────────
        # Executes SELECT statements against the live lakehouse — catches column
        # hallucinations against reality, not against LLM self-assessment.
        # Ref: Horizon VLDB 2025 — execution-based validation is the only
        # reliable schema hallucination detector.
        if strategy == ConversionStrategy.TRINO_SQL:
            try:
                from sql_migration.core.trino_sql_validator import TrinoSQLValidator

                # Build schema_context from plan chunks
                plan_entry = self._load_plan_entry(proc_name)
                schema_ctx = {}
                for chunk in plan_entry.get("chunks", []):
                    schema_ctx.update(chunk.get("schema_context", {}))

                validator = TrinoSQLValidator(self.mcp, schema_ctx)
                trino_result = validator.validate(converted_code, proc_name)

                return ExecutionResult(
                    runtime_error=trino_result.runtime_error,
                    output_schema=trino_result.output_types,
                    row_count=trino_result.row_count,
                    sample_rows=trino_result.sample_rows[:20],
                    duration_s=0.0,
                    is_dry_run=False,  # This is REAL execution against Trino
                )
            except Exception as e:
                log.warning("trino_sql_validation_fallback",
                            proc=proc_name, error=str(e),
                            msg="Falling back to sandbox validation")
                # Fall through to sandbox validation below

        # ── PySpark / fallback: sandbox dry-run ───────────────────────────────
        mode = (
            "exec_pyspark"
            if strategy in {ConversionStrategy.PYSPARK_DF,
                            ConversionStrategy.PYSPARK_PIPELINE}
            else "exec_trino_sql"
        )

        result = self.sandbox.run(
            script="validation/run_validation.py",
            args={
                "mode":           mode,
                "converted_code": converted_code,
                "proc_name":      proc_name,
                "sample_dir":     sample_dir,
                "strategy":       strategy.value,
            },
            timeout=self.global_cfg.sandbox.timeout_seconds,
        )

        if not result.success:
            return ExecutionResult(
                runtime_error=f"Sandbox execution failed: {result.stderr[:500]}",
                is_dry_run=True,
            )

        try:
            data = result.stdout_json
            return ExecutionResult(
                runtime_error=data.get("runtime_error"),
                output_schema=data.get("output_schema", {}),
                row_count=data.get("row_count", 0),
                sample_rows=data.get("sample_rows", [])[:20],
                dml_preview=data.get("dml_preview", []),
                duration_s=data.get("duration_s", 0.0),
                is_dry_run=True,
            )
        except Exception as e:
            return ExecutionResult(
                runtime_error=f"Could not parse execution result: {e}",
                is_dry_run=True,
            )

    # =========================================================================
    # V4 — Semantic diff (LEVEL_3 only)
    # =========================================================================

    def _v4_semantic_diff(
        self,
        proc_name:      str,
        converted_code: str,
        proc_tables:    list[str],
        table_registry: dict,
        exec_result:    ExecutionResult,
        output_catalog: str = "",
        output_schemas: list[str] | None = None,
        output_table:   str = "",
    ) -> tuple[list[ColumnDiff], int | None]:
        """
        Generate equivalent Trino SQL via LLM, run on real Trino,
        compare column schemas.
        Uses output_catalog/output_schemas to resolve output table FQNs.
        If output_table is declared, uses it as the primary comparison target.
        Returns (column_diffs, trino_row_count).
        """
        out_schemas = output_schemas or []

        # Build schema context for the LLM call
        schema_context: dict = {}
        for table in proc_tables:
            entry = table_registry.get(table, {})
            if entry.get("status") == "EXISTS_IN_TRINO":
                schema_context[table] = {
                    "trino_fqn": entry.get("trino_fqn", table),
                    "columns": [c.get("name", "") for c in entry.get("columns", [])],
                    "types": {c.get("name",""): c.get("type","") for c in entry.get("columns",[])},
                }
            elif entry.get("status") == "MISSING" and output_catalog and out_schemas:
                # Try resolving as output table using output_catalog/output_schemas
                bare_name = table.split(".")[-1]
                for s in out_schemas:
                    candidate = f"{output_catalog}.{s}.{bare_name}"
                    try:
                        cols_raw = self.mcp.desc_table(table=candidate)
                        schema_context[table] = {
                            "trino_fqn": candidate,
                            "columns": [c.get("name", "") for c in cols_raw],
                            "types": {c.get("name",""): c.get("type","") for c in cols_raw},
                        }
                        log.debug("v4_output_table_resolved",
                                  source=table, fqn=candidate)
                        break
                    except Exception:
                        continue

        # If a declared output_table exists in Trino, compare its schema directly
        # against the sandbox output — more reliable than LLM-generated SQL.
        if output_table:
            output_fqn = self._resolve_output_fqn(
                output_table, output_catalog, out_schemas
            )
            if output_fqn:
                try:
                    cols_raw = self.mcp.desc_table(table=output_fqn)
                    trino_schema = {
                        c.get("name", ""): c.get("type", "")
                        for c in cols_raw
                    }
                    sandbox_schema = exec_result.output_schema
                    column_diffs = parse_column_diffs(sandbox_schema, trino_schema)
                    # Also get row count for the output table
                    try:
                        count = self.mcp.table_row_count(output_fqn)
                    except Exception:
                        count = None
                    log.info("v4_output_table_direct_compare",
                             proc=proc_name, fqn=output_fqn,
                             diff_cols=len(column_diffs))
                    return column_diffs, count
                except Exception as e:
                    log.debug("v4_output_table_describe_failed",
                              fqn=output_fqn, error=str(e))
                    # Fall through to LLM path

        messages = build_llm_semantic_diff_input(
            converted_pyspark=converted_code,
            schema_context=schema_context,
            strategy=ConversionStrategy.PYSPARK_DF,
        )

        try:
            trino_sql = self.llm.call(messages=messages, expect_json=False)
            trino_sql = trino_sql.strip().rstrip(";")
        except Exception as e:
            log.warning("v4_llm_failed", proc=proc_name, error=str(e))
            return [], None

        # Run on real Trino
        try:
            limited_sql = f"SELECT * FROM ({trino_sql}) AS _diff_q LIMIT 200"
            trino_rows = self.mcp.run_query(limited_sql)
            trino_row_count = len(trino_rows)
            trino_schema = (
                {k: type(v).__name__ for k, v in trino_rows[0].items()}
                if trino_rows else {}
            )
        except Exception as e:
            log.warning("v4_trino_query_failed", proc=proc_name, error=str(e))
            return [], None

        # Compare schemas
        sandbox_schema = exec_result.output_schema
        column_diffs = parse_column_diffs(sandbox_schema, trino_schema)
        return column_diffs, trino_row_count

    def _resolve_output_fqn(
        self,
        output_table: str,
        output_catalog: str,
        output_schemas: list[str],
    ) -> str | None:
        """
        Resolve a declared output table name to a Trino FQN.
        Tries: already-qualified → catalog.schema.table for each schema → None.
        """
        parts = output_table.split(".")
        if len(parts) >= 3:
            return output_table  # Already fully qualified

        bare = parts[-1]
        if len(parts) == 2 and output_catalog:
            return f"{output_catalog}.{parts[0]}.{parts[1]}"

        # Bare name — try each output schema
        if output_catalog and output_schemas:
            for s in output_schemas:
                candidate = f"{output_catalog}.{s}.{bare}"
                try:
                    self.mcp.desc_table(table=candidate)
                    return candidate
                except Exception:
                    continue

        return None

    # =========================================================================
    # V5 — Reconciliation report
    # =========================================================================

    def _v5_score(
        self,
        proc_name:       str,
        val_level:       ValidationLevel,
        exec_result:     ExecutionResult | None,
        column_diffs:    list[ColumnDiff],
        trino_row_count: int | None,
        todo_items:      list[TodoItem],
    ) -> ValidationResult:
        """
        Apply scoring rules to produce PASS | PARTIAL | FAIL.
        """
        cfg_score = self.cfg.scoring
        cfg_diff  = self.cfg.semantic_diff

        # ── FAIL conditions ───────────────────────────────────────────────────
        if exec_result and exec_result.runtime_error:
            return ValidationResult(
                proc_name=proc_name,
                outcome=ValidationOutcome.FAIL,
                validation_level=val_level,
                execution_result=exec_result,
                column_diffs=column_diffs,
                manual_review_items=todo_items,
                fail_reason=f"Runtime error: {exec_result.runtime_error}",
                validated_at=datetime.now(timezone.utc).isoformat(),
            )

        fail_cols = [
            d for d in column_diffs
            if d.status.value in cfg_diff.column_status_fail
        ]
        if fail_cols:
            return ValidationResult(
                proc_name=proc_name,
                outcome=ValidationOutcome.FAIL,
                validation_level=val_level,
                execution_result=exec_result,
                column_diffs=column_diffs,
                manual_review_items=todo_items,
                fail_reason=f"Missing columns in Trino: {[d.column for d in fail_cols]}",
                validated_at=datetime.now(timezone.utc).isoformat(),
            )

        # Row count deviation (LEVEL_3 only)
        row_deviation: float | None = None
        if exec_result and trino_row_count and trino_row_count > 0:
            row_deviation = abs(exec_result.row_count - trino_row_count) / trino_row_count
            if row_deviation > cfg_score.row_deviation_fail:
                return ValidationResult(
                    proc_name=proc_name,
                    outcome=ValidationOutcome.FAIL,
                    validation_level=val_level,
                    execution_result=exec_result,
                    column_diffs=column_diffs,
                    sandbox_row_count=exec_result.row_count if exec_result else None,
                    trino_row_count=trino_row_count,
                    row_deviation_pct=row_deviation,
                    manual_review_items=todo_items,
                    fail_reason=f"Row count deviation {row_deviation:.0%} > {cfg_score.row_deviation_fail:.0%}",
                    validated_at=datetime.now(timezone.utc).isoformat(),
                )

        # ── PARTIAL conditions ────────────────────────────────────────────────
        warnings: list[str] = []

        # When V3 executed successfully against live Trino, note that
        # target-side validation passed but source-side is still pending.
        trino_validated = (
            exec_result
            and not exec_result.runtime_error
            and not exec_result.is_dry_run
        )

        if todo_items and len(todo_items) > cfg_score.max_todos_for_pass:
            warnings.append(f"{len(todo_items)} UNMAPPED construct(s) require manual review")

        if trino_validated:
            warnings.append("Validated on Trino (source-side validation pending)")
        elif val_level < ValidationLevel.FULL_RECONCILIATION:
            warnings.append(f"Partial validation (Level {val_level.value}) — "
                            f"{'no sample data provided' if val_level == ValidationLevel.STATIC_ONLY else 'limited data coverage'}")

        warn_cols = [d for d in column_diffs if d.status.value in cfg_diff.column_status_warn]
        if warn_cols:
            warnings.append(f"Column warnings: {[d.column for d in warn_cols]}")

        if row_deviation and row_deviation > cfg_score.row_deviation_warn:
            warnings.append(f"Row count deviation {row_deviation:.0%}")

        if warnings:
            return ValidationResult(
                proc_name=proc_name,
                outcome=ValidationOutcome.PARTIAL,
                validation_level=val_level,
                execution_result=exec_result,
                column_diffs=column_diffs,
                sandbox_row_count=exec_result.row_count if exec_result else None,
                trino_row_count=trino_row_count,
                row_deviation_pct=row_deviation,
                manual_review_items=todo_items,
                warnings=warnings,
                validated_at=datetime.now(timezone.utc).isoformat(),
            )

        # ── PASS ──────────────────────────────────────────────────────────────
        return ValidationResult(
            proc_name=proc_name,
            outcome=ValidationOutcome.PASS,
            validation_level=val_level,
            execution_result=exec_result,
            column_diffs=column_diffs,
            sandbox_row_count=exec_result.row_count if exec_result else None,
            trino_row_count=trino_row_count,
            row_deviation_pct=row_deviation,
            manual_review_items=todo_items,
            validated_at=datetime.now(timezone.utc).isoformat(),
        )

    # =========================================================================
    # Helpers
    # =========================================================================

    def _fail_result(
        self, proc_name: str, reason: str, level: ValidationLevel
    ) -> ValidationResult:
        return ValidationResult(
            proc_name=proc_name,
            outcome=ValidationOutcome.FAIL,
            validation_level=level,
            fail_reason=reason,
            validated_at=datetime.now(timezone.utc).isoformat(),
        )

    def _load_plan_entry(self, proc_name: str) -> dict:
        try:
            plan = self.store.read("planning", "plan.json")
            return plan.get("procs", {}).get(proc_name, {})
        except Exception:
            return {}

    def _load_table_registry(self) -> dict:
        try:
            data = self.store.read("analysis", "table_registry.json")
            return data.get("entries", {})
        except Exception:
            return {}

    def _load_converted_code(
        self, proc_name: str, strategy: ConversionStrategy
    ) -> str:
        ext = ".sql" if strategy == ConversionStrategy.TRINO_SQL else ".py"
        for e in (ext, ".py", ".sql"):
            path = self.store.path("conversion", f"proc_{proc_name}{e}")
            if path.exists():
                return path.read_text(encoding="utf-8")
        return ""

    def _load_todo_items(self, proc_name: str) -> list[TodoItem]:
        try:
            log_data = self.store.read("conversion", f"conversion_log_{proc_name}.json")
            return [TodoItem(**t) if isinstance(t, dict) else t
                    for t in log_data.get("todos", [])]
        except Exception:
            return []