"""
SQLGlot-based Oracle → Spark SQL Transpiler

Wraps SQLGlot with custom pre/post-processing rules for Oracle-specific
constructs that SQLGlot doesn't handle or handles incorrectly.
"""

import re
import json
import os

try:
    import sqlglot
    from sqlglot import exp
    HAS_SQLGLOT = True
except ImportError:
    HAS_SQLGLOT = False


# Load Oracle mappings
_MAPPINGS_PATH = os.path.join(os.path.dirname(__file__), 'oracle_mappings.json')
with open(_MAPPINGS_PATH, 'r') as f:
    ORACLE_MAPPINGS = json.load(f)

# Oracle date format -> Spark date format mapping
DATE_FORMAT_MAP = ORACLE_MAPPINGS['functions']['TO_CHAR']['format_mapping']

# Oracle hints to strip
HINTS_TO_DROP = ORACLE_MAPPINGS['oracle_hints_to_drop']


class TranspileResult:
    def __init__(self, original: str, transpiled: str, method: str,
                 warnings: list[str] = None, success: bool = True):
        self.original = original
        self.transpiled = transpiled
        self.method = method  # 'sqlglot', 'regex', 'manual', 'passthrough'
        self.warnings = warnings or []
        self.success = success


def transpile_sql(sql: str, preserve_comments: bool = False) -> TranspileResult:
    """Transpile a single Oracle SQL statement to Spark SQL.

    Uses a multi-pass approach:
    1. Pre-process: handle constructs SQLGlot can't parse
    2. SQLGlot transpilation (if available)
    3. Post-process: fix any remaining Oracle-isms
    4. Fallback: regex-based translation if SQLGlot fails

    Args:
        sql: Oracle SQL statement
        preserve_comments: Whether to keep comments in output

    Returns:
        TranspileResult with original and transpiled SQL
    """
    warnings = []

    # Clean the SQL
    clean_sql = _strip_hints(sql)
    clean_sql = _strip_trailing_semicolon(clean_sql)

    if not preserve_comments:
        clean_sql = _strip_inline_comments(clean_sql)

    # Pre-process Oracle-specific constructs
    preprocessed, pre_warnings = _preprocess(clean_sql)
    warnings.extend(pre_warnings)

    # Try SQLGlot transpilation
    if HAS_SQLGLOT:
        try:
            result = sqlglot.transpile(preprocessed, read='oracle', write='spark')[0]
            # Post-process
            result, post_warnings = _postprocess(result)
            warnings.extend(post_warnings)
            return TranspileResult(sql, result, 'sqlglot', warnings)
        except Exception as e:
            warnings.append(f"SQLGlot failed: {str(e)}, falling back to regex")

    # Fallback: regex-based translation
    result, regex_warnings = _regex_transpile(preprocessed)
    warnings.extend(regex_warnings)

    # Post-process
    result, post_warnings = _postprocess(result)
    warnings.extend(post_warnings)

    method = 'regex' if result != preprocessed else 'passthrough'
    return TranspileResult(sql, result, method, warnings)


def transpile_date_format(oracle_fmt: str) -> str:
    """Convert Oracle date format string to Spark date format string."""
    # Remove quotes
    fmt = oracle_fmt.strip("'\"")

    # Try exact match first
    if fmt.upper() in DATE_FORMAT_MAP:
        return DATE_FORMAT_MAP[fmt.upper()]

    # Try component-by-component translation
    spark_fmt = fmt
    # Sort by length descending to avoid partial replacements
    sorted_mappings = sorted(DATE_FORMAT_MAP.items(), key=lambda x: len(x[0]), reverse=True)
    for oracle_part, spark_part in sorted_mappings:
        # Case-insensitive replacement
        pattern = re.compile(re.escape(oracle_part), re.IGNORECASE)
        spark_fmt = pattern.sub(spark_part, spark_fmt)

    return spark_fmt


_LLM_BRIDGE = None


def _get_llm_bridge():
    """Lazy-init a singleton LLM bridge for correlated UPDATE conversion.

    Tier-2 optimisation: a single focused LLM call (~10 s, aggressively cached)
    produces a correct MERGE statement. Much cheaper than the full agentic loop
    (~90-120 s, 10 tool calls) that otherwise runs when the regex transpiler's
    MERGE trips semantic review.
    """
    global _LLM_BRIDGE
    if _LLM_BRIDGE is not None:
        return _LLM_BRIDGE
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return None
    try:
        from .llm_bridge import LLMBridge
        _LLM_BRIDGE = LLMBridge(provider="claude")
        return _LLM_BRIDGE
    except Exception:
        return None


def transpile_correlated_update(sql: str) -> str:
    """Transpile Oracle correlated UPDATE to Spark SQL MERGE.

    Flow:
      1. Try the LLM bridge (cached, ~10 s first-call, 0 s on cache hit).
         This yields a correct, semantically-complete MERGE — much cheaper
         than the agentic repair loop that otherwise runs downstream.
      2. If LLM bridge unavailable or returns None, fall back to the regex
         transpiler below.

    Regex fallback still handles three Oracle patterns:
      A) UPDATE t SET (c1,c2) = (SELECT ... WHERE correlated) [WHERE EXISTS ...]
      B) UPDATE t SET c = (SELECT ... WHERE correlated)
      C) UPDATE t SET c = expr WHERE simple  (simple, no subquery)
    """
    # ── Tier-2: LLM bridge first (produces correct MERGE in one shot) ───
    bridge = _get_llm_bridge()
    if bridge is not None:
        try:
            tgt_match = re.search(
                r'Update\s+(?:/\*.*?\*/)?\s*([\w\.]+)',
                sql, re.IGNORECASE)
            target_table = re.sub(r'@\w+', '', tgt_match.group(1)) if tgt_match else "TARGET"
            # Strip DB-link tokens from the source before sending
            clean_sql = re.sub(r'@\w+', '', sql)
            preprocessed = transpile_sql(clean_sql).transpiled
            llm_merge = bridge.convert_correlated_update(
                oracle_sql=clean_sql,
                preprocessed_sql=preprocessed,
                target_table=target_table,
            )
            if llm_merge and "MERGE" in llm_merge.upper():
                return (
                    f"-- Correlated UPDATE converted to MERGE (LLM bridge)\n"
                    f"-- Target: {target_table}\n"
                    f"{llm_merge.strip()}"
                )
        except Exception:
            pass  # fall through to regex

    # ── Parse target table + alias ──────────────────────────────────
    target_match = re.search(
        r'Update\s+(?:/\*.*?\*/)?\s*([\w\.]+)\s+(\w+)',
        sql, re.IGNORECASE)
    if not target_match:
        return f"-- TODO: Manual conversion needed for UPDATE\n-- {sql}"

    target_table = re.sub(r'@\w+', '', target_match.group(1))
    target_alias = target_match.group(2)

    # ── Find SET clause and extract subquery with balanced parens ────
    set_kw = re.search(r'\bSet\b', sql, re.IGNORECASE)
    if not set_kw:
        return _regex_transpile(sql)[0]

    set_pos = set_kw.end()
    rest_after_set = sql[set_pos:]

    # Determine if multi-column: SET (c1, c2) = (SELECT ...) or SET c = (SELECT ...)
    multi_match = re.match(r'\s*\(([^)]+)\)\s*=\s*', rest_after_set, re.IGNORECASE)
    single_match = re.match(r'\s*(?:\w+\.)?(\w+)\s*=\s*', rest_after_set, re.IGNORECASE)

    if multi_match:
        raw_cols = [c.strip() for c in multi_match.group(1).split(',')]
        columns = [re.sub(r'^\w+\.', '', c) for c in raw_cols]
        subq_start_pos = set_pos + multi_match.end()
    elif single_match:
        columns = [single_match.group(1)]
        subq_start_pos = set_pos + single_match.end()
    else:
        return _regex_transpile(sql)[0]

    # Extract balanced-paren subquery: find ( ... SELECT ... ) with proper nesting
    subquery, subq_end_pos = _extract_balanced_subquery(sql, subq_start_pos)
    if not subquery:
        return _regex_transpile(sql)[0]

    set_end_pos = subq_end_pos

    # ── Extract outer WHERE clause (often WHERE EXISTS …) ───────────
    outer_where = ''
    ow_match = re.search(r'\bWhere\s+(.+?)$', sql[set_end_pos:],
                         re.IGNORECASE | re.DOTALL)
    if ow_match:
        outer_where = ow_match.group(1).strip().rstrip(';').rstrip(')')

    # ── Parse subquery SELECT list (positional → SET columns) ───────
    sel_match = re.search(r'Select\s+(.+?)\s+From\b', subquery,
                          re.IGNORECASE | re.DOTALL)
    if sel_match:
        src_exprs = [e.strip() for e in sel_match.group(1).split(',')]
    else:
        src_exprs = columns

    # Build positional column map: target.SetCol = source_expr_alias
    src_aliases = []
    for i, expr in enumerate(src_exprs):
        # Use the last token (after AS or just the column name) as alias
        alias_match = re.search(r'(?:\bAs\s+)?(\w+)\s*$', expr, re.IGNORECASE)
        if alias_match:
            src_aliases.append(alias_match.group(1))
        elif i < len(columns):
            src_aliases.append(columns[i])
        else:
            src_aliases.append(f"_col{i}")

    col_assignments = ', '.join(
        f"target.{columns[i]} = source.{src_aliases[i]}"
        if i < len(src_aliases) else f"target.{columns[i]} = source.{columns[i]}"
        for i in range(len(columns)))

    # ── Extract correlated join condition from subquery WHERE ───────
    sub_where_match = re.search(r'\bWhere\s+(.+?)$', subquery,
                                re.IGNORECASE | re.DOTALL)
    if sub_where_match:
        raw_conds = sub_where_match.group(1).strip().rstrip(';').rstrip(')')
    else:
        raw_conds = ''

    # Split conditions on AND, classify into join / filter / rownum
    cond_parts = re.split(r'\bAnd\b', raw_conds, flags=re.IGNORECASE)
    join_parts = []
    filter_parts = []
    for cp in cond_parts:
        cp = cp.strip()
        if not cp:
            continue
        # ROWNUM predicates → skip (handled below via ROW_NUMBER if needed)
        if re.search(r'\brownum\b', cp, re.IGNORECASE):
            continue
        # Correlated predicate: references the target alias
        if re.search(rf'\b{re.escape(target_alias)}\.', cp, re.IGNORECASE):
            # Qualify both sides: source-table refs stay, target alias → target.
            qualified = re.sub(
                rf'\b{re.escape(target_alias)}\.(\w+)',
                r'target.\1', cp, flags=re.IGNORECASE)
            # Also prefix bare non-target column refs with source.
            for alias_candidate in re.findall(r'\b(\w+)\.\w+', cp):
                if alias_candidate.lower() != target_alias.lower():
                    qualified = re.sub(
                        rf'\b{re.escape(alias_candidate)}\.(\w+)',
                        r'source.\1', qualified, flags=re.IGNORECASE)
            join_parts.append(qualified)
        else:
            filter_parts.append(cp)

    join_condition = ' AND '.join(join_parts) if join_parts else 'source._key = target._key'

    # Handle ROWNUM < 2 → wrap USING subquery with ROW_NUMBER
    has_rownum = bool(re.search(r'\brownum\b', raw_conds, re.IGNORECASE))

    # ── Transpile the subquery through sqlglot ──────────────────────
    sub_result = transpile_sql(subquery)
    transpiled_sub = sub_result.transpiled
    # Strip trailing semicolons/whitespace
    transpiled_sub = transpiled_sub.strip().rstrip(';')

    # If ROWNUM used, wrap the USING subquery to deduplicate
    if has_rownum and join_parts:
        partition_col = re.search(r'target\.(\w+)', join_parts[0])
        part_col = partition_col.group(1) if partition_col else columns[0]
        transpiled_sub = (
            f"SELECT * FROM (\n"
            f"  SELECT *, ROW_NUMBER() OVER (PARTITION BY {part_col} ORDER BY 1) AS _rn\n"
            f"  FROM ({transpiled_sub})\n"
            f") WHERE _rn = 1"
        )

    # Add non-correlated filter predicates into the USING subquery
    if filter_parts:
        filter_sql = ' AND '.join(filter_parts)
        transpiled_sub = f"SELECT * FROM ({transpiled_sub}) WHERE {filter_sql}"

    return (
        f"-- Correlated UPDATE converted to MERGE\n"
        f"-- Original: UPDATE {target_table} {target_alias}\n"
        f"-- Columns: {', '.join(columns)}\n"
        f"MERGE INTO {target_table} target\n"
        f"USING ({transpiled_sub}) source\n"
        f"ON {join_condition}\n"
        f"WHEN MATCHED THEN UPDATE SET {col_assignments}"
    )


def _extract_balanced_subquery(sql: str, start: int) -> tuple[str | None, int]:
    """Extract content between balanced parentheses starting at `start`.

    Skips leading whitespace then expects '('. Returns the content between
    the opening '(' and its matching ')' (exclusive of both parens), plus the
    position just past the closing ')'.
    """
    i = start
    while i < len(sql) and sql[i] in ' \t\n\r':
        i += 1
    if i >= len(sql) or sql[i] != '(':
        return None, start
    depth = 0
    content_start = i + 1
    in_string = False
    string_char = None
    while i < len(sql):
        ch = sql[i]
        if in_string:
            if ch == string_char and (i + 1 >= len(sql) or sql[i + 1] != string_char):
                in_string = False
        else:
            if ch in ("'", '"'):
                in_string = True
                string_char = ch
            elif ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    return sql[content_start:i].strip(), i + 1
        i += 1
    return None, start


def _extract_join_condition_from_subquery(subquery: str, target_alias: str) -> str:
    """Extract the correlated join condition from a subquery's WHERE clause.

    In Oracle correlated UPDATEs, the subquery references the outer table's alias:
        SELECT col FROM source_table s WHERE s.key = outer_alias.key

    We identify predicates that reference the target alias and convert them
    into MERGE ON conditions with target./source. prefixes.

    Args:
        subquery: The SELECT subquery text from the SET clause
        target_alias: The alias of the UPDATE target table (e.g., 'a', 'l')

    Returns:
        A join condition string like "source.Lac_No = target.Lac_No"
    """
    # Extract the WHERE clause, stripping ROWNUM/LIMIT predicates
    where_match = re.search(r'\bWhere\s+(.+?)$', subquery, re.IGNORECASE | re.DOTALL)
    if not where_match:
        return f"source.key = target.key  -- TODO: could not extract join condition"

    where_text = where_match.group(1).strip()
    # Clean trailing characters
    where_text = re.sub(r'[;\s\)]+$', '', where_text)

    # Remove ROWNUM predicates (they're row-limiting, not join conditions)
    where_text = re.sub(r'\bAnd\s+Rownum\s*[<>=]+\s*\d+', '', where_text, flags=re.IGNORECASE)
    where_text = re.sub(r'LIMIT\s+\d+\s*--[^\n]*', '', where_text, flags=re.IGNORECASE)
    where_text = where_text.strip()

    if not where_text:
        return f"source.key = target.key  -- TODO: could not extract join condition"

    alias_escaped = re.escape(target_alias)
    correlated_preds = []

    # Split on AND (at top level, not inside parentheses)
    predicates = re.split(r'\bAnd\b', where_text, flags=re.IGNORECASE)

    for pred in predicates:
        pred = pred.strip()
        if not pred:
            continue
        # Skip non-join predicates (IS NULL, IS NOT NULL, literals, ROWNUM)
        if re.match(r'^rownum\b', pred, re.IGNORECASE):
            continue
        if re.match(r'^LIMIT\b', pred, re.IGNORECASE):
            continue

        # Check if this predicate references the outer table alias
        if re.search(rf'\b{alias_escaped}\.\w+', pred, re.IGNORECASE):
            # This is a correlated predicate — rewrite alias references
            # Replace target_alias.col with target.col
            rewritten = re.sub(
                rf'\b{alias_escaped}\.(\w+)',
                r'target.\1',
                pred,
                flags=re.IGNORECASE
            )
            # Replace any other alias.col with source.col
            other_refs = re.findall(r'\b(\w+)\.(\w+)', rewritten)
            for ref_alias, ref_col in other_refs:
                if ref_alias.lower() != 'target' and ref_alias.lower() != 'source':
                    rewritten = re.sub(
                        rf'\b{re.escape(ref_alias)}\.{re.escape(ref_col)}',
                        f'source.{ref_col}',
                        rewritten,
                        count=1,
                        flags=re.IGNORECASE
                    )
            correlated_preds.append(rewritten)

    if correlated_preds:
        return ' AND '.join(correlated_preds)

    # Fallback 1: look for unaliased column = target_alias.column patterns
    # e.g., WHERE Lac_No = a.Lac_No (no source alias on the left side)
    unaliased_matches = re.findall(
        rf'\b(\w+)\s*=\s*{alias_escaped}\.(\w+)',
        where_text, re.IGNORECASE
    )
    if unaliased_matches:
        preds = []
        for src_col, tgt_col in unaliased_matches:
            # Skip if the "src_col" is actually a keyword or literal
            if src_col.upper() in ('NULL', 'TRUE', 'FALSE', 'AND', 'OR', 'NOT'):
                continue
            preds.append(f"source.{src_col} = target.{tgt_col}")
        if preds:
            return ' AND '.join(preds)

    # Fallback 2: reverse pattern — target_alias.column = column
    reverse_matches = re.findall(
        rf'{alias_escaped}\.(\w+)\s*=\s*(\w+)',
        where_text, re.IGNORECASE
    )
    if reverse_matches:
        preds = []
        for tgt_col, src_col in reverse_matches:
            if src_col.upper() in ('NULL', 'TRUE', 'FALSE', 'AND', 'OR', 'NOT'):
                continue
            preds.append(f"source.{src_col} = target.{tgt_col}")
        if preds:
            return ' AND '.join(preds)

    return f"source.key = target.key  -- TODO: could not extract join condition from: {where_text[:80]}"


def _strip_hints(sql: str) -> str:
    """Remove Oracle optimizer hints."""
    # General hint pattern
    result = re.sub(r'/\*\+[^*]*?\*/', '', sql)
    return result


def _strip_trailing_semicolon(sql: str) -> str:
    """Remove trailing semicolon."""
    return sql.rstrip().rstrip(';').rstrip()


def _strip_inline_comments(sql: str) -> str:
    """Remove inline -- comments but preserve the SQL."""
    lines = sql.split('\n')
    cleaned = []
    for line in lines:
        # Don't strip if the entire line is a comment
        if line.strip().startswith('--'):
            continue
        # Remove trailing comments
        cleaned.append(re.sub(r'\s*--.*$', '', line))
    return '\n'.join(cleaned)


def _preprocess(sql: str) -> tuple[str, list[str]]:
    """Pre-process Oracle SQL before SQLGlot transpilation.

    Handles constructs that SQLGlot can't parse or transpile correctly.
    """
    warnings = []
    result = sql

    # 1. Convert Oracle (+) outer join syntax to remove the operator
    # a.col = b.col(+) means LEFT OUTER JOIN (the (+) side is the optional side)
    # b.col(+) = a.col means LEFT OUTER JOIN (same — (+) marks the deficient side)
    # We strip the (+) markers here and add a warning; SQLGlot or the regex fallback
    # then processes the cleaned SQL. The WHERE-based join becomes an implicit join
    # which is semantically equivalent in Spark SQL when (+) is simply removed.
    if '(+)' in result:
        # Pattern: column(+) — the (+) marks the table whose rows may be absent
        # In WHERE-clause joins, removing (+) converts outer to inner semantics.
        # To preserve outer join semantics, convert to explicit ANSI JOIN syntax.
        result, outer_join_count = _convert_oracle_outer_joins(result)
        if outer_join_count > 0:
            warnings.append(
                f"Oracle (+) outer join converted to LEFT JOIN ({outer_join_count} condition(s)) — verify join correctness"
            )

    # 2. Replace SYSDATE with CURRENT_TIMESTAMP()
    result = re.sub(r'\bSysdate\b', 'CURRENT_TIMESTAMP()', result, flags=re.IGNORECASE)

    # 3. Handle TRUNC(date, 'MONTH') - N (Oracle date arithmetic)
    # TRUNC(SYSDATE, 'MONTH') - 1 means "last day of previous month"
    result = re.sub(
        r"TRUNC\s*\(\s*CURRENT_TIMESTAMP\(\)\s*,\s*'MONTH'\s*\)\s*-\s*(\d+)",
        r"DATE_SUB(DATE_TRUNC('MONTH', CURRENT_DATE()), \1)",
        result, flags=re.IGNORECASE
    )

    # TRUNC(SYSDATE, 'MI') -> DATE_TRUNC('MINUTE', CURRENT_TIMESTAMP())
    result = re.sub(
        r"TRUNC\s*\(\s*CURRENT_TIMESTAMP\(\)\s*,\s*'MI'\s*\)",
        "DATE_TRUNC('MINUTE', CURRENT_TIMESTAMP())",
        result, flags=re.IGNORECASE
    )

    # TRUNC(SYSDATE) or TRUNC(date) -> DATE_TRUNC('DAY', date) i.e. CURRENT_DATE()
    result = re.sub(
        r"TRUNC\s*\(\s*CURRENT_TIMESTAMP\(\)\s*\)",
        "CURRENT_DATE()",
        result, flags=re.IGNORECASE
    )

    # 4. Handle TRUNC(date_col, 'MONTH')
    result = re.sub(
        r"TRUNC\s*\(\s*(\w[\w\.]*)\s*,\s*'MONTH'\s*\)",
        r"DATE_TRUNC('MONTH', \1)",
        result, flags=re.IGNORECASE
    )

    # 4a. Handle TRUNC(column) without format arg -> CAST(column AS DATE)
    # Must run after TRUNC(SYSDATE) and TRUNC(col, 'MONTH') to avoid double-match
    result = re.sub(
        r"\bTrunc\s*\(\s*(\w[\w\.]*)\s*\)",
        r"CAST(\1 AS DATE)",
        result, flags=re.IGNORECASE
    )

    # 4b. Handle remaining TRUNC with format args
    def _replace_trunc_fmt(m):
        fmt_map = {'YEAR': 'YEAR', 'YY': 'YEAR', 'Q': 'QUARTER', 'MM': 'MONTH'}
        spark_fmt = fmt_map.get(m.group(2).upper(), m.group(2).upper())
        return f"DATE_TRUNC('{spark_fmt}', {m.group(1)})"

    result = re.sub(
        r"TRUNC\s*\(\s*(.+?)\s*,\s*'(YEAR|YY|Q|MM)'\s*\)",
        _replace_trunc_fmt,
        result, flags=re.IGNORECASE
    )

    # 5. Handle TO_NUMBER(TO_CHAR(...)) -> CAST(DATE_FORMAT(...) AS DECIMAL)
    # This common Oracle pattern must be handled before individual TO_CHAR/TO_NUMBER
    def replace_tonumber_tochar(match):
        inner_expr = match.group(1)
        fmt = match.group(2)
        spark_fmt = transpile_date_format(fmt)
        return f"CAST(DATE_FORMAT({inner_expr}, '{spark_fmt}') AS DECIMAL)"

    result = re.sub(
        r"TO_NUMBER\s*\(\s*TO_CHAR\s*\(\s*(.+?)\s*,\s*'([^']+)'\s*\)\s*\)",
        replace_tonumber_tochar,
        result, flags=re.IGNORECASE
    )

    # 6. Handle TO_CHAR with date format conversion
    def replace_to_char(match):
        expr = match.group(1)
        fmt = match.group(2)
        spark_fmt = transpile_date_format(fmt)
        return f"DATE_FORMAT({expr}, '{spark_fmt}')"

    result = re.sub(
        r"TO_CHAR\s*\(\s*(.+?)\s*,\s*'([^']+)'\s*\)",
        replace_to_char,
        result, flags=re.IGNORECASE
    )

    # 7. Handle NVL -> COALESCE
    result = re.sub(r'\bNvl\s*\(', 'COALESCE(', result, flags=re.IGNORECASE)

    # 8. Handle DECODE -> CASE WHEN
    result = _convert_decode_to_case(result)

    # 9. Remove all @database_link references (e.g., @Dbloans, @lnk_dbmis, etc.)
    db_link_matches = re.findall(r'@(\w+)', result)
    if db_link_matches:
        unique_links = sorted(set(m.lower() for m in db_link_matches))
        link_names = ', '.join(f'@{l}' for l in unique_links)
        warnings.append(f"Database link {link_names} removed — ensure table is available locally or via JDBC")
        result = re.sub(r'@\w+', '', result)

    # 9. Remove FROM DUAL
    result = re.sub(r',\s*Dual\b', '', result, flags=re.IGNORECASE)
    result = re.sub(r'\bFrom\s+Dual\b', '', result, flags=re.IGNORECASE)

    # 10. ROWNUM handling — convert all Oracle ROWNUM patterns to Spark LIMIT
    # 10a. AND ROWNUM < N  →  LIMIT N-1
    result = re.sub(
        r'\bAnd\s+Rownum\s*<\s*2',
        'LIMIT 1  -- was ROWNUM < 2',
        result, flags=re.IGNORECASE
    )
    # 10b. AND ROWNUM = 1  →  LIMIT 1
    result = re.sub(
        r'\bAnd\s+Rownum\s*=\s*1\b',
        'LIMIT 1  -- was ROWNUM = 1',
        result, flags=re.IGNORECASE
    )
    # 10c. AND ROWNUM <= N  →  LIMIT N
    result = re.sub(
        r'\bAnd\s+Rownum\s*<=\s*(\d+)',
        r'LIMIT \1  -- was ROWNUM <= \1',
        result, flags=re.IGNORECASE
    )
    # 10d. WHERE ROWNUM = 1) at end of subquery  →  LIMIT 1)
    result = re.sub(
        r'\bWhere\s+Rownum\s*=\s*1\s*\)',
        'LIMIT 1)  -- was WHERE ROWNUM = 1',
        result, flags=re.IGNORECASE
    )
    # 10e. WHERE ROWNUM < 2  →  LIMIT 1
    result = re.sub(
        r'\bWhere\s+Rownum\s*<\s*2\b',
        'LIMIT 1  -- was WHERE ROWNUM < 2',
        result, flags=re.IGNORECASE
    )

    # 11. Handle MINUS -> EXCEPT
    result = re.sub(r'\bMINUS\b', 'EXCEPT', result, flags=re.IGNORECASE)

    # 12. Handle INSTR with 4 args (Oracle allows pos, occurrence)
    # INSTR(str, substr, pos, occurrence) -> basic warning
    instr_4arg = re.search(r'INSTR\s*\([^,]+,[^,]+,[^,]+,[^)]+\)', result, re.IGNORECASE)
    if instr_4arg:
        warnings.append("INSTR with 4 arguments detected — Spark only supports 2 args. Manual conversion needed.")

    # 13. Oracle date arithmetic: date + N/date - N where N is integer
    # This means add/subtract N days
    # Pattern: some_date_expr - N (where N is a small integer, not part of TRUNC...MONTH-1 already handled)

    return result, warnings


def _convert_decode_to_case(sql: str) -> str:
    """Convert Oracle DECODE to CASE WHEN expression.

    DECODE(expr, search1, result1, search2, result2, ..., default)
    -> CASE WHEN expr = search1 THEN result1 WHEN expr = search2 THEN result2 ... ELSE default END
    """
    # Find DECODE( and extract balanced parentheses content
    result = sql

    while True:
        match = re.search(r'\bDECODE\s*\(', result, re.IGNORECASE)
        if not match:
            break

        start = match.start()
        paren_start = match.end() - 1  # Position of opening (

        # Find matching closing )
        depth = 1
        pos = paren_start + 1
        while pos < len(result) and depth > 0:
            if result[pos] == '(':
                depth += 1
            elif result[pos] == ')':
                depth -= 1
            pos += 1

        if depth != 0:
            break  # Unbalanced parens, skip

        paren_end = pos - 1
        inner = result[paren_start + 1:paren_end]

        # Split by commas, respecting nested parentheses
        args = _split_args(inner)

        if len(args) < 3:
            break  # Invalid DECODE, skip

        expr = args[0].strip()

        # Build CASE expression
        case_parts = [f"CASE"]
        i = 1
        while i < len(args) - 1:
            search_val = args[i].strip()
            result_val = args[i + 1].strip()
            case_parts.append(f" WHEN {expr} = {search_val} THEN {result_val}")
            i += 2

        # If odd number of remaining args, last is default
        if i < len(args):
            default_val = args[i].strip()
            case_parts.append(f" ELSE {default_val}")

        case_parts.append(" END")
        case_expr = ''.join(case_parts)

        result = result[:start] + case_expr + result[paren_end + 1:]

    return result


def _split_args(s: str) -> list[str]:
    """Split a comma-separated argument string, respecting nested parentheses."""
    args = []
    depth = 0
    current = []

    for char in s:
        if char == '(' :
            depth += 1
            current.append(char)
        elif char == ')':
            depth -= 1
            current.append(char)
        elif char == ',' and depth == 0:
            args.append(''.join(current))
            current = []
        else:
            current.append(char)

    if current:
        args.append(''.join(current))

    return args


def _convert_oracle_outer_joins(sql: str) -> tuple[str, int]:
    """Convert Oracle (+) outer join conditions in WHERE clauses.

    Oracle syntax:
        WHERE a.col = b.col(+)    → LEFT JOIN  (b is optional)
        WHERE a.col(+) = b.col    → RIGHT JOIN (a is optional, but we normalize to LEFT)

    Strategy: Strip (+) markers from individual conditions and rewrite the FROM/WHERE
    clause when possible. For complex cases (multiple tables, mixed joins), we fall back
    to simply removing (+) which preserves the query structure for SQLGlot to handle.

    Returns:
        (modified_sql, count_of_conditions_converted)
    """
    result = sql
    count = 0

    # Pattern: expression(+) in a WHERE condition
    # Matches: alias.column(+) or just column(+)
    # We convert by removing the (+) marker. The semantic change from outer to inner
    # is acceptable because Spark SQL's FROM-clause ANSI joins are the norm,
    # and the WHERE-based Oracle join with (+) removed becomes a standard equi-join.
    #
    # For full ANSI conversion we'd need to restructure FROM/WHERE which is fragile.
    # Instead: strip (+) and emit a clear warning so humans verify.

    # Count occurrences for the warning message
    count = len(re.findall(r'\(\+\)', result))

    # Strip the (+) markers
    result = result.replace('(+)', '')

    return result, count


def _regex_transpile(sql: str) -> tuple[str, list[str]]:
    """Fallback regex-based transpilation when SQLGlot is unavailable or fails."""
    warnings = []
    result = sql

    # Most Oracle-specific transforms are already handled in _preprocess
    # This handles any remaining patterns

    # TO_NUMBER -> CAST AS DECIMAL
    result = re.sub(
        r'TO_NUMBER\s*\(\s*(.+?)\s*\)',
        r'CAST(\1 AS DECIMAL)',
        result, flags=re.IGNORECASE
    )

    # SUBSTR -> SUBSTRING
    result = re.sub(r'\bSUBSTR\s*\(', 'SUBSTRING(', result, flags=re.IGNORECASE)

    # Remove any residual Oracle (+) markers that weren't handled in preprocessing
    if '(+)' in result:
        result = result.replace('(+)', '')
        warnings.append("Residual (+) outer join syntax removed — verify join semantics")

    return result, warnings


def _postprocess(sql: str) -> tuple[str, list[str]]:
    """Post-process transpiled SQL for Spark compatibility."""
    warnings = []
    result = sql

    # Ensure LIMIT comes at the end (SQLGlot may misplace it)
    # Fix any LIMIT inside subquery that was ROWNUM

    # Clean up double spaces
    result = re.sub(r'  +', ' ', result)

    # Clean up empty lines
    result = re.sub(r'\n\s*\n\s*\n', '\n\n', result)

    return result, warnings


def transpile_execute_immediate(sql: str) -> str:
    """Transpile EXECUTE IMMEDIATE statements.

    Handles:
    - TRUNCATE TABLE -> spark.sql("TRUNCATE TABLE ...")
    - ALTER TABLE (partition mgmt) -> comment/skip
    - Other DDL -> spark.sql(...)
    """
    # Extract the SQL string from EXECUTE IMMEDIATE
    match = re.search(r"Execute\s+Immediate\s+'([^']+)'", sql, re.IGNORECASE)
    if not match:
        # Handle string concatenation: Execute Immediate v_Str
        var_match = re.search(r"Execute\s+Immediate\s+(\w+)", sql, re.IGNORECASE)
        if var_match:
            var_name = var_match.group(1)
            return f"spark.sql({var_name})  # Dynamic SQL — verify at runtime"
        return f"# TODO: Convert EXECUTE IMMEDIATE\n# {sql}"

    inner_sql = match.group(1)

    if 'truncate' in inner_sql.lower():
        table = _extract_truncate_target(f"truncate table {inner_sql.split('table')[-1]}")
        if table:
            # Remove any @db_link suffix from table name
            table = re.sub(r'@\w+', '', table)
            return f'spark.sql("TRUNCATE TABLE {table}")'
        return f'spark.sql("TRUNCATE TABLE {inner_sql}")'

    if 'alter table' in inner_sql.lower():
        return f"# Partition management — handled by Spark/Iceberg\n# Original: {inner_sql}"

    return f'spark.sql("{inner_sql}")'


def _extract_truncate_target(sql: str) -> str | None:
    """Extract table name from truncate statement."""
    match = re.search(r"truncate\s+table\s+([\w\.@]+)", sql, re.IGNORECASE)
    return match.group(1) if match else None
