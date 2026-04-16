#!/usr/bin/env python3
"""
validate_conversion.py
======================
Sandbox script: validate a converted chunk of code before accepting it.
Runs inside the Podman container.

Three checks in sequence — all must pass:
  1. PARSE CHECK    — sqlglot.parse(code, dialect='trino') for TRINO_SQL
                      compile(code, 'exec') for PySpark (PYSPARK_DF / PYSPARK_PIPELINE)
  2. REMNANT CHECK  — Search for any source-dialect functions still present
                      using adapter.dialect_functions with word-boundary regex
  3. UNMAPPED CHECK — Count TODO markers (warnings, not errors)

Called by: Conversion Agent steps C3 and C4
Input  (--args JSON): {
    "converted_code": "...",
    "strategy":       "TRINO_SQL|PYSPARK_DF|PYSPARK_PIPELINE|MANUAL_SKELETON",
    "dialect_functions": ["NVL", "DECODE", ...],
    "trino_mappings":    {"NVL": "COALESCE", ...},
    "unresolved_deps":   ["calc_risk_score", "external_udf"]  // optional
}
Output (stdout JSON): {
    "pass":        true/false,
    "errors":      ["error1", ...],
    "todo_count":  3,
    "todo_items":  [{"line": 14, "comment": "# TODO: UNMAPPED ..."}],
    "warnings":    ["warning1", ...]
}
"""

from __future__ import annotations

import argparse
import json
import re
import sys


# ---------------------------------------------------------------------------
# Check 1: Parse / syntax check
# ---------------------------------------------------------------------------

def check_parse(code: str, strategy: str) -> list[str]:
    """Return list of parse errors (empty = pass)."""
    errors = []

    if strategy == "TRINO_SQL":
        try:
            import sqlglot
            stmts = sqlglot.parse(code, dialect="trino",
                                  error_level=sqlglot.ErrorLevel.RAISE)
            if not stmts:
                errors.append("PARSE: No valid statements found in converted code")
        except Exception as e:
            errors.append(f"PARSE: {e}")

    elif strategy in ("PYSPARK_DF", "PYSPARK_PIPELINE", "MANUAL_SKELETON"):
        try:
            compile(code, "<converted_chunk>", "exec")
        except SyntaxError as e:
            errors.append(f"SYNTAX: {e.msg} at line {e.lineno}: {e.text!r}")
        except Exception as e:
            errors.append(f"SYNTAX: {str(e)}")

    return errors


# ---------------------------------------------------------------------------
# Check 2: Dialect remnant detection
# ---------------------------------------------------------------------------

def check_remnants(
    code: str,
    dialect_functions: list[str],
    trino_mappings: dict[str, str],
    unresolved_deps: list[str] | None = None,
) -> list[str]:
    """
    Search for source-dialect functions still present in converted code.
    Uses word-boundary regex to avoid false positives.
    Returns list of remnant error strings.

    Also checks for raw calls to unresolved_deps (unknown UDFs / external
    procs not present in any source file).  A raw call that is NOT wrapped in
    a TODO comment is an error — the LLM should have emitted:
      # TODO: UNMAPPED — original: calc_risk_score(...)
    but instead left the literal call in the output.
    """
    errors = []
    code_upper = code.upper()

    for fn in dialect_functions:
        fn_upper = fn.upper()
        # Word-boundary match — avoids flagging 'DECODE' inside 'DECODED'
        if re.search(r'\b' + re.escape(fn_upper) + r'\b', code_upper):
            mapped_to = trino_mappings.get(fn, trino_mappings.get(fn_upper))
            if mapped_to:
                errors.append(
                    f"REMNANT: '{fn}' not translated → should be '{mapped_to}'"
                )
            else:
                # UNMAPPED function — should have a TODO comment, not the raw call
                # Only flag if it's an actual function call (followed by '(')
                if re.search(
                    r'\b' + re.escape(fn_upper) + r'\s*\(',
                    code_upper
                ):
                    errors.append(
                        f"REMNANT: '{fn}' still present as a call "
                        f"— should be replaced with TODO comment"
                    )

    # ── Check: raw calls to unknown UDFs (unresolved_deps) ─────────────────────
    # These are functions/procs that appeared in call-detection but were not
    # found in any source file in the manifest.  If the LLM left a raw call
    # to one of them instead of a TODO comment, flag it as an error.
    for fn in (unresolved_deps or []):
        fn_upper = fn.upper()
        # Check for a raw function call (fn followed by optional whitespace then '(')
        if re.search(r'\b' + re.escape(fn_upper) + r'\s*\(', code_upper):
            # Only flag if there is NO TODO comment mentioning this function on the same line
            for line in code.splitlines():
                if re.search(r'\b' + re.escape(fn_upper) + r'\s*\(', line.upper()):
                    is_commented_out = line.strip().startswith(('#', '--'))
                    has_todo = bool(re.search(r'TODO.*UNMAPPED', line, re.IGNORECASE))
                    if not is_commented_out and not has_todo:
                        errors.append(
                            f"UNRESOLVED_DEP: '{fn}' is called but was not found in any "
                            f"source file — replace with "
                            f"'# TODO: UNMAPPED — original: {fn}(...) — UDF definition unknown'"
                        )
                        break  # One error per unresolved dep is enough

    return errors


# ---------------------------------------------------------------------------
# Check 3: TODO / unmapped construct extraction
# ---------------------------------------------------------------------------

def extract_todos(code: str) -> tuple[int, list[dict]]:
    """
    Count and list TODO markers from UNMAPPED constructs.
    These are warnings (not errors) — they require human review but
    do not block the conversion from proceeding.
    """
    todos = []
    pattern = re.compile(
        r'(?:#|--)\s*TODO\s*:?\s*UNMAPPED.*',
        re.IGNORECASE
    )
    for i, line in enumerate(code.splitlines(), start=1):
        m = pattern.search(line)
        if m:
            # Try to extract the original function name
            fn_match = re.search(r'original:\s*(\w+)', line, re.IGNORECASE)
            todos.append({
                "line":         i,
                "comment":      line.strip(),
                "original_fn":  fn_match.group(1) if fn_match else "",
                "construct_type": "UNMAPPED_CONSTRUCT",
            })
    return len(todos), todos


# ---------------------------------------------------------------------------
# Additional quality checks
# ---------------------------------------------------------------------------

def check_quality_warnings(code: str, strategy: str) -> list[str]:
    """
    Non-blocking quality warnings.
    """
    warnings = []

    # Check for common Oracle-specific patterns that might have been missed
    oracle_patterns = [
        (r'\bROWNUM\b', "ROWNUM found — should be ROW_NUMBER() OVER () or LIMIT"),
        (r'\bSYSDATE\b', "SYSDATE found — should be CURRENT_TIMESTAMP"),
        (r'\bNVL\s*\(', "NVL( found — should be COALESCE("),
        (r'\bDECODE\s*\(', "DECODE( found — should be CASE WHEN"),
        (r'\bTO_DATE\s*\(', "TO_DATE( found — should be DATE_PARSE"),
        (r'\bDBMS_OUTPUT\b', "DBMS_OUTPUT found — should be removed or logged"),
    ]

    if strategy == "TRINO_SQL":
        for pattern, msg in oracle_patterns:
            if re.search(pattern, code, re.IGNORECASE):
                warnings.append(f"QUALITY: {msg}")

    # Check for empty output
    if len(code.strip()) < 10:
        warnings.append("QUALITY: Converted code is very short — may be incomplete")

    # Check for obvious placeholder text
    if "YOUR CODE HERE" in code.upper() or "PLACEHOLDER" in code.upper():
        warnings.append("QUALITY: Placeholder text found in converted code")

    return warnings


# ---------------------------------------------------------------------------
# v7: Test execution mode (C3.5)
# ---------------------------------------------------------------------------

def exec_test(converted_code: str, test_code: str, strategy: str) -> dict:
    """
    Execute the LLM-generated test script against the production code
    in a local PySpark sandbox.

    The test is self-contained but may reference the production code.
    We combine them into a single script:
      1. Production code (defines functions / SQL statements)
      2. Test code (creates synthetic data, runs production code, asserts)

    Returns:
        {"test_result": "TEST_PASS" | "TEST_FAIL: ...", "runtime_error": null | "..."}
    """
    import subprocess
    import tempfile
    import os

    if not test_code.strip():
        return {
            "test_result": "TEST_FAIL: No test code provided",
            "runtime_error": None,
        }

    # Build combined script: production code as importable block + test
    # For PySpark: define the production code, then run the test
    # For TRINO_SQL: the test uses spark.sql() to run SQL statements
    if strategy == "TRINO_SQL":
        # SQL strategy: test should use spark.sql() directly
        # Provide the production SQL as a string variable
        escaped_sql = converted_code.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')
        combined = f'''# === PRODUCTION SQL (available as _PRODUCTION_SQL variable) ===
_PRODUCTION_SQL = """{escaped_sql}"""

# === TEST SCRIPT ===
{test_code}
'''
    else:
        # PySpark strategy: production code defines functions/transforms,
        # test code calls them
        combined = f'''# === PRODUCTION CODE ===
{converted_code}

# === TEST SCRIPT ===
{test_code}
'''

    # Write to temp file and execute in subprocess with timeout
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".py", delete=False, dir="/tmp"
    ) as f:
        f.write(combined)
        script_path = f.name

    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            timeout=120,  # 2 minute timeout for PySpark startup + test
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        )

        stdout = result.stdout.strip()
        stderr = result.stderr.strip()

        # Look for TEST_PASS / TEST_FAIL in stdout
        test_result = ""
        for line in stdout.splitlines():
            if "TEST_PASS" in line or "TEST_FAIL" in line:
                test_result = line.strip()
                break

        runtime_error = None
        if result.returncode != 0:
            # Script crashed — extract the error
            error_lines = stderr.splitlines()
            # Get the last meaningful error line (skip PySpark noise)
            for line in reversed(error_lines):
                if line.strip() and not line.startswith("WARNING") \
                   and "WARN" not in line[:10]:
                    runtime_error = line.strip()
                    break
            if not runtime_error:
                runtime_error = f"Exit code {result.returncode}: {stderr[-300:]}"

        if not test_result and not runtime_error:
            test_result = f"TEST_FAIL: Test completed but did not print TEST_PASS or TEST_FAIL. stdout: {stdout[-200:]}"

        return {
            "test_result":   test_result,
            "runtime_error": runtime_error,
        }

    except subprocess.TimeoutExpired:
        return {
            "test_result":   "TEST_FAIL: Test execution timed out (120s)",
            "runtime_error": "TimeoutExpired",
        }
    except Exception as e:
        return {
            "test_result":   f"TEST_FAIL: {e}",
            "runtime_error": str(e),
        }
    finally:
        try:
            os.unlink(script_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--args", required=True)
    args_ns = parser.parse_args()
    params  = json.loads(args_ns.args)

    mode = params.get("mode", "static")

    # ── v7: Test execution mode ───────────────────────────────────────────
    if mode == "exec_test":
        converted_code = params.get("converted_code", "")
        test_code      = params.get("test_code", "")
        strategy       = params.get("strategy", "TRINO_SQL")
        result = exec_test(converted_code, test_code, strategy)
        print(json.dumps(result, default=str))
        return

    # ── v8: Arbitrary code execution mode (for agentic tool-calling) ──────
    if mode == "exec_code":
        code = params.get("code", "")
        if not code.strip():
            print(json.dumps({"stdout": "", "stderr": "Empty code", "exit_code": 1}))
            return

        import subprocess
        import tempfile
        import os

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", delete=False, dir="/tmp"
        ) as f:
            f.write(code)
            script_path = f.name

        try:
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True, text=True, timeout=120,
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            )
            print(json.dumps({
                "stdout": result.stdout[:5000],
                "stderr": result.stderr[:3000],
                "exit_code": result.returncode,
            }))
        except subprocess.TimeoutExpired:
            print(json.dumps({
                "stdout": "", "stderr": "Execution timed out (120s)", "exit_code": 124,
            }))
        except Exception as e:
            print(json.dumps({
                "stdout": "", "stderr": str(e)[:1000], "exit_code": 1,
            }))
        finally:
            try:
                os.unlink(script_path)
            except OSError:
                pass
        return

    # ── Static validation mode (default) ──────────────────────────────────
    converted_code     = params.get("converted_code", "")
    strategy           = params.get("strategy", "TRINO_SQL")
    dialect_functions  = params.get("dialect_functions", [])
    trino_mappings     = params.get("trino_mappings", {})

    if not converted_code.strip():
        print(json.dumps({
            "pass":       False,
            "errors":     ["EMPTY: Converted code is empty"],
            "todo_count": 0,
            "todo_items": [],
            "warnings":   [],
        }))
        return

    # Run all checks
    parse_errors   = check_parse(converted_code, strategy)
    unresolved_deps = params.get("unresolved_deps", [])
    remnant_errors = check_remnants(converted_code, dialect_functions, trino_mappings, unresolved_deps)
    todo_count, todo_items = extract_todos(converted_code)
    warnings       = check_quality_warnings(converted_code, strategy)

    all_errors = parse_errors + remnant_errors
    passed     = len(all_errors) == 0

    print(json.dumps({
        "pass":       passed,
        "errors":     all_errors,
        "todo_count": todo_count,
        "todo_items": todo_items,
        "warnings":   warnings,
    }))


if __name__ == "__main__":
    main()
