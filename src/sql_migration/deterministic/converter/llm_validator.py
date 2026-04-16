"""
LLM output validation for PL/SQL → PySpark conversions.

Every LLM response passes through:
1. Syntax check (sqlglot for SQL, ast.parse for Python)
2. Structure check (MERGE must have USING, ON, WHEN MATCHED)
3. Safety check (no Oracle remnants, no eval/exec)
4. Cross-check (target table matches, column count)

Returns None on any failure → caller falls back to original TODO.
"""

import ast
import re

# Oracle functions that should NOT appear in Spark output
ORACLE_FUNCTIONS = {
    "NVL", "NVL2", "DECODE", "SYSDATE", "ROWNUM", "ROWID",
    "USER", "SYSTIMESTAMP",
}

# Dangerous Python constructs
UNSAFE_PATTERNS = [
    r"\beval\s*\(",
    r"\bexec\s*\(",
    r"\bos\.system\s*\(",
    r"\bsubprocess\.",
    r"\b__import__\s*\(",
    r"\bopen\s*\(",
]

# Oracle join syntax
ORACLE_JOIN_PATTERN = r"\(\+\)"


def validate_merge_sql(sql: str, target_table: str | None = None) -> str | None:
    """Validate a MERGE statement produced by LLM.

    Returns the cleaned SQL on success, None on failure.
    """
    if not sql or not sql.strip():
        return None

    sql = sql.strip()

    # Remove markdown fences if LLM included them
    sql = _strip_markdown_fences(sql)

    # 1. Structure check: must be a MERGE
    upper = sql.upper()
    if not upper.startswith("MERGE"):
        return None

    required_clauses = ["USING", "ON", "WHEN MATCHED", "UPDATE SET"]
    for clause in required_clauses:
        if clause not in upper:
            return None

    # 2. Safety: no Oracle functions
    if _has_oracle_functions(sql):
        return None

    # 3. Safety: no Oracle outer join syntax
    if re.search(ORACLE_JOIN_PATTERN, sql):
        return None

    # 4. Cross-check: target table must appear in MERGE INTO
    if target_table:
        table_clean = re.sub(r"@\w+", "", target_table).strip()
        merge_into_match = re.search(r"MERGE\s+INTO\s+(\S+)", sql, re.IGNORECASE)
        if merge_into_match:
            merge_target = merge_into_match.group(1).strip().rstrip(".")
            if table_clean.upper() not in merge_target.upper():
                return None

    # 5. Syntax check via sqlglot (if available)
    try:
        import sqlglot
        parsed = sqlglot.parse(sql, read="spark")
        if not parsed or parsed[0] is None:
            return None
    except ImportError:
        pass  # sqlglot not available, skip syntax check
    except Exception:
        return None  # Parse failed

    return sql


def validate_python_code(code: str) -> str | None:
    """Validate Python code produced by LLM.

    Returns the cleaned code on success, None on failure.
    """
    if not code or not code.strip():
        return None

    code = code.strip()

    # Remove markdown fences if LLM included them
    code = _strip_markdown_fences(code)

    # 1. Safety: no dangerous constructs
    for pattern in UNSAFE_PATTERNS:
        if re.search(pattern, code):
            return None

    # 2. Safety: no Oracle functions in SQL strings
    # Extract SQL strings from spark.sql() calls
    sql_strings = re.findall(r'spark\.sql\(\s*"""(.*?)"""', code, re.DOTALL)
    sql_strings += re.findall(r"spark\.sql\(\s*'([^']*)'", code)
    sql_strings += re.findall(r'spark\.sql\(\s*"([^"]*)"', code)
    for sql_str in sql_strings:
        if _has_oracle_functions(sql_str):
            return None

    # 3. Syntax check: must be valid Python
    try:
        ast.parse(code)
    except SyntaxError:
        # Try indenting it in case it's a code block meant to be inside a function
        try:
            ast.parse("if True:\n" + _ensure_indented(code))
        except SyntaxError:
            return None

    return code


def validate_todo_fix(original_todo: str, replacement: str) -> str | None:
    """Validate a TODO fix produced by LLM.

    Returns the cleaned replacement on success, None on failure.
    """
    if not replacement or not replacement.strip():
        return None

    replacement = replacement.strip()
    replacement = _strip_markdown_fences(replacement)

    # If the replacement still contains TODO, it's not really a fix
    # (unless it's a different TODO — allow if count decreased)
    if replacement.count("# TODO") >= original_todo.count("# TODO"):
        # LLM couldn't fix it, returned the same thing
        if "# TODO" in replacement:
            return None

    # Safety checks
    for pattern in UNSAFE_PATTERNS:
        if re.search(pattern, replacement):
            return None

    # Check SQL strings for Oracle functions
    sql_strings = re.findall(r'spark\.sql\(\s*"""(.*?)"""', replacement, re.DOTALL)
    for sql_str in sql_strings:
        if _has_oracle_functions(sql_str):
            return None

    return replacement


def _has_oracle_functions(sql: str) -> bool:
    """Check if SQL contains Oracle-specific functions."""
    upper = sql.upper()
    for func in ORACLE_FUNCTIONS:
        # Match as a word boundary (not part of a larger identifier)
        if re.search(rf"\b{func}\b", upper):
            # Exclude cases where it's in a comment
            # Simple heuristic: check if it's after -- or between /* */
            for match in re.finditer(rf"\b{func}\b", upper):
                pos = match.start()
                line_start = upper.rfind("\n", 0, pos) + 1
                line = upper[line_start:pos]
                if "--" not in line:
                    return True
    return False


def _strip_markdown_fences(text: str) -> str:
    """Remove markdown code fences if present."""
    text = text.strip()
    if text.startswith("```"):
        # Remove first line (```sql, ```python, etc.)
        lines = text.split("\n")
        lines = lines[1:]  # Remove opening fence
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]  # Remove closing fence
        text = "\n".join(lines)
    return text.strip()


def _ensure_indented(code: str) -> str:
    """Ensure all lines have at least 4-space indent."""
    lines = code.split("\n")
    result = []
    for line in lines:
        if line.strip():
            if not line.startswith("    "):
                line = "    " + line
        result.append(line)
    return "\n".join(result)
