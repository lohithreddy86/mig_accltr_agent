#!/usr/bin/env python3
"""
run_validation.py
=================
Sandbox script: execute converted code against local PySpark using sample
data pulled from Trino, and capture output schema + row samples.

Handles two modes:
  MODE "exec_pyspark":   Run a converted .py proc against local SparkSession
                          with parquet sample data registered as temp views.
  MODE "exec_trino_sql": Run converted Trino SQL statement-by-statement
                          against local Spark (SELECT statements only).

Both modes use DRY_RUN — write calls are patched to capture output instead
of actually writing anywhere.

Called by: Validation Agent steps V2/V3
Input  (--args JSON): {
    "mode":          "exec_pyspark" | "exec_trino_sql",
    "converted_code": "...",
    "proc_name":      "sp_calc_interest",
    "sample_dir":     "/artifacts/07_samples/sp_calc_interest",
    "strategy":       "PYSPARK_DF | TRINO_SQL"
}
Output (stdout JSON): {
    "runtime_error":   null | "error message",
    "output_schema":   {"col_name": "type", ...},
    "row_count":       42,
    "sample_rows":     [{...}, ...],
    "dml_preview":     [{...}, ...],
    "duration_s":      1.23,
    "is_dry_run":      true
}
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import sys
import time
import tempfile
from pathlib import Path


# ---------------------------------------------------------------------------
# DRY_RUN patcher for PySpark write calls
# ---------------------------------------------------------------------------

_DRY_RUN_PREAMBLE = '''
# ── DRY_RUN mode injected by validation sandbox ──────────────────────────
import os as _os
_captured_outputs = {}
_captured_errors  = []

class _DryRunWriter:
    def __init__(self, df, mode_str="overwrite"):
        self._df       = df
        self._mode_str = mode_str
    def mode(self, m):
        self._mode_str = m
        return self
    def saveAsTable(self, name, **kwargs):
        _captured_outputs[name] = self._df
        return None
    def parquet(self, path, **kwargs):
        _captured_outputs[path] = self._df
        return None
    def csv(self, path, **kwargs):
        _captured_outputs[path] = self._df
        return None
    def format(self, fmt):
        return self
    def option(self, k, v):
        return self
    def options(self, **kwargs):
        return self

def _patch_df_write(df):
    """Patch a DataFrame's write property to use DryRunWriter."""
    class _PatchedDF:
        def __getattr__(self, name):
            return getattr(df, name)
        @property
        def write(self):
            return _DryRunWriter(df)
    return _PatchedDF()

# Monkey-patch SparkSession to intercept createDataFrame / read results
from pyspark.sql import DataFrame as _DataFrame
_orig_write = property(lambda self: _DryRunWriter(self))
# Cannot easily monkey-patch property on final class — use exec wrapper instead
# ────────────────────────────────────────────────────────────────────────────
'''

_DRY_RUN_POSTAMBLE = '''
# ── Capture results for validation ───────────────────────────────────────
import json as _json_out
_result_rows  = []
_result_schema = {}
_row_count     = 0

for _name, _df in _captured_outputs.items():
    try:
        _rows = _df.limit(20).collect()
        _row_count = _df.count()
        if _rows:
            _result_rows = [r.asDict() for r in _rows]
            _result_schema = {
                field.name: str(field.dataType)
                for field in _df.schema.fields
            }
    except Exception as _e:
        _captured_errors.append(str(_e))

# Write results to temp file for parent process to read
_out_path = _os.environ.get("VALIDATION_OUTPUT_PATH", "/tmp/_validation_result.json")
with open(_out_path, "w") as _fh:
    _json_out.dump({
        "output_schema":  _result_schema,
        "row_count":      _row_count,
        "sample_rows":    _result_rows[:20],
        "errors":         _captured_errors,
    }, _fh)
'''


def patch_pyspark_writes(code: str) -> str:
    """
    Patch spark write calls in PySpark code for dry-run mode.
    Replaces .write.mode(...).saveAsTable(...) patterns with capture calls.
    Simple text replacement — not AST-based to keep it robust.
    """
    import re
    # Replace chained write calls
    patched = re.sub(
        r'\.write\b',
        '._dry_write',
        code
    )
    # Add _dry_write property shim at top
    shim = (
        "# DRY_RUN: _dry_write shim\n"
        "class _WriteShim:\n"
        "    def __init__(self, df):\n"
        "        self._df = df\n"
        "        self._mode_val = 'overwrite'\n"
        "    def mode(self, m): self._mode_val=m; return self\n"
        "    def format(self, f): return self\n"
        "    def option(self, k, v): return self\n"
        "    def options(self, **kw): return self\n"
        "    def saveAsTable(self, n, **kw): _captured_outputs[n]=self._df\n"
        "    def parquet(self, p, **kw): _captured_outputs[p]=self._df\n"
        "    def csv(self, p, **kw): _captured_outputs[p]=self._df\n"
        "    def insertInto(self, n, **kw): _captured_outputs[n]=self._df\n"
        "from pyspark.sql import DataFrame as _DF\n"
        "_DF._dry_write = property(lambda self: _WriteShim(self))\n\n"
    )
    return shim + patched


# ---------------------------------------------------------------------------
# SparkSession builder for local validation
# ---------------------------------------------------------------------------

def build_local_spark(app_name: str = "validation"):
    """Build a minimal local SparkSession for validation."""
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def register_sample_data(spark, sample_dir: str) -> list[str]:
    """
    Load all parquet files from sample_dir and register as Spark temp views.
    Returns list of registered table names.
    """
    registered = []
    sample_path = Path(sample_dir)
    if not sample_path.exists():
        return registered

    for parquet_file in sample_path.glob("*.parquet"):
        table_name = parquet_file.stem  # filename without extension = table name
        try:
            df = spark.read.parquet(str(parquet_file))
            df.createOrReplaceTempView(table_name)
            registered.append(table_name)
        except Exception as e:
            print(f"[WARN] Could not register {table_name}: {e}", file=sys.stderr)

    return registered


# ---------------------------------------------------------------------------
# Execute PySpark
# ---------------------------------------------------------------------------

def exec_pyspark(converted_code: str, sample_dir: str) -> dict:
    """Execute converted PySpark code in dry-run mode."""
    start = time.monotonic()

    try:
        spark = build_local_spark("pyspark_validation")
        registered = register_sample_data(spark, sample_dir)
    except Exception as e:
        return {
            "runtime_error": f"SparkSession init failed: {e}",
            "output_schema": {}, "row_count": 0,
            "sample_rows": [], "dml_preview": [],
            "duration_s": time.monotonic() - start,
            "is_dry_run": True,
        }

    # Prepare output file for results
    result_path = tempfile.mktemp(suffix=".json")
    os.environ["VALIDATION_OUTPUT_PATH"] = result_path

    # Patch write calls and inject preamble/postamble
    patched_code = (
        _DRY_RUN_PREAMBLE
        + "\n"
        + patch_pyspark_writes(converted_code)
        + "\n"
        + _DRY_RUN_POSTAMBLE
    )

    # Build execution namespace
    ns = {"spark": spark, "__name__": "__validation__"}

    try:
        exec(patched_code, ns)
        runtime_error = None
    except Exception as e:
        runtime_error = f"{type(e).__name__}: {e}"

    # Read results
    result_data: dict = {}
    if Path(result_path).exists():
        try:
            result_data = json.loads(Path(result_path).read_text())
        except Exception:
            pass

    # Merge execution errors
    exec_errors = result_data.get("errors", [])
    if runtime_error:
        exec_errors.insert(0, runtime_error)

    return {
        "runtime_error": exec_errors[0] if exec_errors else None,
        "output_schema": result_data.get("output_schema", {}),
        "row_count":     result_data.get("row_count", 0),
        "sample_rows":   result_data.get("sample_rows", []),
        "dml_preview":   [],
        "duration_s":    round(time.monotonic() - start, 2),
        "is_dry_run":    True,
    }


# ---------------------------------------------------------------------------
# Execute Trino SQL (using local Spark SQL)
# ---------------------------------------------------------------------------

def exec_trino_sql(converted_code: str, sample_dir: str) -> dict:
    """
    Execute converted Trino SQL against local Spark.
    Only SELECT statements are actually run.
    DML (INSERT/UPDATE) is parsed but not executed — preview only.
    """
    import re
    start = time.monotonic()

    try:
        spark = build_local_spark("trino_sql_validation")
        register_sample_data(spark, sample_dir)
    except Exception as e:
        return {
            "runtime_error": f"SparkSession init failed: {e}",
            "output_schema": {}, "row_count": 0,
            "sample_rows": [], "dml_preview": [],
            "duration_s": time.monotonic() - start,
            "is_dry_run": True,
        }

    # Strip SQL comments, split into statements
    stripped = re.sub(r'--.*', '', converted_code)
    statements = [s.strip() for s in stripped.split(';') if len(s.strip()) > 5]

    output_schema: dict = {}
    row_count = 0
    sample_rows: list = []
    dml_preview: list = []
    runtime_error = None

    for stmt in statements:
        upper = stmt.upper().strip()
        is_select = upper.startswith("SELECT") or upper.startswith("WITH")
        is_dml    = any(upper.startswith(k) for k in ("INSERT", "UPDATE", "DELETE", "MERGE"))

        if is_select:
            try:
                # Limit rows for validation
                limited = f"SELECT * FROM ({stmt}) AS _val_subq LIMIT 20"
                df = spark.sql(limited)
                rows = df.collect()
                sample_rows = [r.asDict() for r in rows]
                output_schema = {f.name: str(f.dataType) for f in df.schema.fields}
                row_count = len(rows)  # approximate for LIMIT 20
            except Exception as e:
                runtime_error = f"SQL execution error: {e}"
                break

        elif is_dml:
            # Preview DML by running the SELECT subquery inside it
            select_match = re.search(r'SELECT\s+.+', stmt, re.IGNORECASE | re.DOTALL)
            if select_match:
                try:
                    preview_sql = select_match.group(0)
                    df = spark.sql(f"SELECT * FROM ({preview_sql}) AS _dml_preview LIMIT 5")
                    dml_preview = [r.asDict() for r in df.collect()]
                except Exception:
                    dml_preview = [{"_preview": "Could not extract DML preview"}]

    return {
        "runtime_error": runtime_error,
        "output_schema": output_schema,
        "row_count":     row_count,
        "sample_rows":   sample_rows,
        "dml_preview":   dml_preview,
        "duration_s":    round(time.monotonic() - start, 2),
        "is_dry_run":    True,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--args", required=True)
    args_ns = parser.parse_args()
    params  = json.loads(args_ns.args)

    mode          = params.get("mode", "exec_pyspark")
    converted_code = params.get("converted_code", "")
    sample_dir    = params.get("sample_dir", "")

    if not converted_code.strip():
        print(json.dumps({
            "runtime_error": "Empty converted code",
            "output_schema": {}, "row_count": 0,
            "sample_rows": [], "dml_preview": [],
            "duration_s": 0, "is_dry_run": True,
        }))
        return

    if mode == "exec_pyspark":
        result = exec_pyspark(converted_code, sample_dir)
    elif mode == "exec_trino_sql":
        result = exec_trino_sql(converted_code, sample_dir)
    else:
        result = {"runtime_error": f"Unknown mode: {mode}",
                  "output_schema": {}, "row_count": 0,
                  "sample_rows": [], "dml_preview": [],
                  "duration_s": 0, "is_dry_run": True}

    print(json.dumps(result, default=str))


if __name__ == "__main__":
    main()
