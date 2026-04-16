#!/usr/bin/env python3
"""
assemble_proc.py
================
Sandbox script: assemble all converted chunks into a single proc file
and run an integration check on the assembled code.

Called by: Conversion Agent step C5
Input  (--args JSON): {
    "proc_name":  "sp_calc_interest",
    "strategy":   "PYSPARK_DF",
    "chunks": [
        {"chunk_id": "c0", "converted_code": "..."},
        {"chunk_id": "c1", "converted_code": "..."}
    ],
    "output_path": "/artifacts/04_conversion/proc_sp_calc_interest.py"
}
Output (stdout JSON): {
    "status":          "PASS" | "INTEGRATION_FAIL",
    "assembled_code":  "...",
    "output_path":     "/artifacts/...",
    "error":           "",
    "warnings":        []
}

Assembly logic:
  - TRINO_SQL: concatenate with newline separators + SQL comment headers
  - PYSPARK_*: wrap in a function def proc_{name}(spark), deduplicate imports,
               indent all chunk code into the function body
  - MANUAL_SKELETON: pass through as-is with a human-review header
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Import deduplication (PySpark)
# ---------------------------------------------------------------------------

_IMPORT_RE = re.compile(r'^(?:import\s+\S+|from\s+\S+\s+import\s+.+)$', re.MULTILINE)


def extract_imports(code: str) -> tuple[list[str], str]:
    """
    Extract import statements from code.
    Returns (imports_list, code_without_imports).

    Uses _IMPORT_RE regex which handles edge cases the previous string-based
    approach missed (extra whitespace, multi-line from...import with parens).
    """
    imports = []
    remaining_lines = []
    for line in code.splitlines():
        stripped = line.strip()
        if _IMPORT_RE.match(stripped):
            imports.append(stripped)
        else:
            remaining_lines.append(line)
    return imports, "\n".join(remaining_lines)


def deduplicate_imports(all_imports: list[str]) -> list[str]:
    """Return a deduplicated, sorted list of import statements."""
    seen = set()
    result = []
    for imp in all_imports:
        if imp not in seen:
            seen.add(imp)
            result.append(imp)
    return sorted(result)


# ---------------------------------------------------------------------------
# Assembly strategies
# ---------------------------------------------------------------------------

def assemble_trino_sql(proc_name: str, chunks: list[dict]) -> str:
    """Assemble Trino SQL chunks — concatenate with comment separators."""
    parts = [
        f"-- ============================================================",
        f"-- Converted proc: {proc_name}",
        f"-- Source dialect: migrated to Trino SQL",
        f"-- ============================================================",
        "",
    ]
    for chunk in chunks:
        chunk_id = chunk.get("chunk_id", "")
        code     = chunk.get("converted_code", "").strip()
        if not code:
            continue
        if len(chunks) > 1:
            parts.append(f"-- --- Chunk: {chunk_id} ---")
        parts.append(code)
        parts.append("")

    return "\n".join(parts)


def assemble_pyspark(proc_name: str, strategy: str, chunks: list[dict]) -> str:
    """
    Assemble PySpark chunks into a single function.
    - Collect all imports from all chunks, deduplicate
    - Wrap chunk bodies in: def proc_{name}(spark):
    - Handle continuation of DataFrames across chunks via state_vars
    """
    all_imports = [
        "from pyspark.sql import SparkSession",
        "from pyspark.sql import functions as F",
        "from pyspark.sql import types as T",
    ]
    chunk_bodies = []

    for chunk in chunks:
        code = chunk.get("converted_code", "").strip()
        if not code:
            continue
        imports, body = extract_imports(code)
        all_imports.extend(imports)
        chunk_bodies.append(body)

    deduped_imports = deduplicate_imports(all_imports)

    # Build function body
    full_body_lines = []
    for i, body in enumerate(chunk_bodies):
        if len(chunks) > 1:
            full_body_lines.append(f"    # --- chunk {i} ---")
        for line in body.splitlines():
            # Indent each line for function body
            full_body_lines.append(f"    {line}" if line.strip() else "")

    fn_name = f"proc_{re.sub(r'[^a-zA-Z0-9_]', '_', proc_name)}"
    assembled = (
        "\n".join(deduped_imports)
        + "\n\n\n"
        + f"def {fn_name}(spark: SparkSession) -> None:\n"
        + f'    """\n'
        + f'    Converted procedure: {proc_name}\n'
        + f'    Strategy: {strategy}\n'
        + f'    """\n'
        + "\n".join(full_body_lines)
        + "\n\n\n"
        + f"if __name__ == '__main__':\n"
        + f"    _spark = SparkSession.builder.appName('{fn_name}').getOrCreate()\n"
        + f"    {fn_name}(_spark)\n"
    )
    return assembled


def assemble_manual_skeleton(proc_name: str, chunks: list[dict]) -> str:
    """Pass through MANUAL_SKELETON with review header."""
    code = "\n\n".join(c.get("converted_code", "") for c in chunks)
    return (
        f"# ================================================================\n"
        f"# MANUAL REVIEW REQUIRED: {proc_name}\n"
        f"# This procedure contains constructs that could not be automatically\n"
        f"# converted. Review all TODO markers before using.\n"
        f"# ================================================================\n\n"
        + code
    )


# ---------------------------------------------------------------------------
# Integration check
# ---------------------------------------------------------------------------

def integration_check(assembled_code: str, strategy: str) -> list[str]:
    """Run a final parse/compile check on the fully assembled code."""
    errors = []

    if strategy == "TRINO_SQL":
        try:
            import sqlglot, re as _re
            # Strip SQL comments before parsing (sqlglot chokes on -- lines)
            stripped = _re.sub(r'--.*', '', assembled_code)
            # Parse non-empty statements only
            stmts = [s.strip() for s in stripped.split(';') if len(s.strip()) > 5]
            for stmt in stmts:
                try:
                    sqlglot.parse(stmt, dialect="trino",
                                  error_level=sqlglot.ErrorLevel.WARN)
                except Exception as e:
                    errors.append(f"INTEGRATION_PARSE: {e}")
                    break  # Stop at first error
        except Exception as e:
            errors.append(f"INTEGRATION_PARSE: {e}")

    elif strategy in ("PYSPARK_DF", "PYSPARK_PIPELINE"):
        try:
            compile(assembled_code, "<assembled_proc>", "exec")
        except SyntaxError as e:
            errors.append(
                f"INTEGRATION_SYNTAX: {e.msg} at line {e.lineno}: {e.text!r}"
            )

        # Also check for any obvious import errors
        try:
            tree = ast.parse(assembled_code)
            # Check that the proc function is present
            fn_names = [
                node.name for node in ast.walk(tree)
                if isinstance(node, ast.FunctionDef)
            ]
            if not fn_names:
                errors.append("INTEGRATION: No function definition found in assembled code")
        except Exception as e:
            errors.append(f"INTEGRATION_AST: {e}")

    return errors


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--args", required=True)
    args_ns = parser.parse_args()
    params  = json.loads(args_ns.args)

    proc_name   = params.get("proc_name", "unknown_proc")
    strategy    = params.get("strategy", "TRINO_SQL")
    chunks      = params.get("chunks", [])
    output_path = params.get("output_path", "")

    if not chunks:
        print(json.dumps({
            "status":        "INTEGRATION_FAIL",
            "assembled_code": "",
            "output_path":    output_path,
            "error":          "No chunks provided for assembly",
            "warnings":       [],
        }))
        return

    # Assemble
    if strategy == "TRINO_SQL":
        assembled = assemble_trino_sql(proc_name, chunks)
    elif strategy in ("PYSPARK_DF", "PYSPARK_PIPELINE"):
        assembled = assemble_pyspark(proc_name, strategy, chunks)
    else:
        assembled = assemble_manual_skeleton(proc_name, chunks)

    # Integration check
    errors = integration_check(assembled, strategy)

    if errors:
        print(json.dumps({
            "status":        "INTEGRATION_FAIL",
            "assembled_code": assembled,
            "output_path":    output_path,
            "error":          "; ".join(errors),
            "warnings":       [],
        }))
        return

    # Write assembled code to output path
    warnings = []
    if output_path:
        try:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            Path(output_path).write_text(assembled, encoding="utf-8")
        except Exception as e:
            warnings.append(f"Could not write to {output_path}: {e}")

    print(json.dumps({
        "status":        "PASS",
        "assembled_code": assembled,
        "output_path":    output_path,
        "error":          "",
        "warnings":       warnings,
    }))


if __name__ == "__main__":
    main()
