"""
PySpark Code Generator

Assembles complete PySpark Python functions from:
- SQLGlot-transpiled SQL statements
- Classified statement metadata
- Status tracking decorator
- Procedural logic (IF/ELSE, FOR loops, etc.)
"""

import re
from .sql_extractor import SQLStatement, StatementType
from .transpiler import transpile_sql, transpile_correlated_update, transpile_execute_immediate


def generate_procedure(procedure_name: str, status_name: str,
                       statements: list[SQLStatement],
                       params: list[str] = None,
                       step_tracking: bool = True) -> str:
    """Generate a complete PySpark function from classified SQL statements.

    Args:
        procedure_name: Python function name
        status_name: Status name for tracking table
        statements: Classified SQL statements from the procedure
        params: Function parameters (beyond 'spark')
        step_tracking: Whether to include step number tracking

    Returns:
        Complete Python function as a string
    """
    lines = []
    param_str = ', '.join(params) if params else ''
    func_sig = f"def {procedure_name}(spark{', ' + param_str if param_str else ''}):"

    lines.append(f'@track_status("{status_name}")')
    lines.append(func_sig)
    lines.append(f'    """Converted from Oracle PL/SQL procedure {procedure_name}."""')

    if step_tracking:
        lines.append('    step_no = 0')
    lines.append('')

    for stmt in statements:
        if stmt.is_commented:
            # Preserve commented-out code as Python comments
            lines.append(_generate_comment_block(stmt))
            continue

        generated = _generate_statement(stmt, step_tracking)
        if generated:
            lines.append(generated)

    # If no meaningful statements were generated, add a pass
    meaningful = [s for s in statements if s.type not in
                  (StatementType.COMMIT, StatementType.STATUS_INSERT,
                   StatementType.STATUS_UPDATE, StatementType.DBMS_STATS,
                   StatementType.COMMENT_BLOCK, StatementType.ROLLBACK)]
    if not meaningful:
        lines.append('    pass')

    body = '\n'.join(lines)
    # Post-pass: Oracle PL/SQL bind variables (e.g. `Lv_Yyyymm`) that appear
    # inside `spark.sql("""...""")` string literals are dead — the raw string
    # is not f-substituted, so the SQL references a column/identifier that
    # doesn't exist instead of the Python variable. Walk the body, collect
    # Python-side bind names (proc params + assignments), and convert any
    # `spark.sql("""...""")` that references them into an f-string with
    # `{var}` interpolation.
    body = _interpolate_bind_vars_in_sql_blocks(body, params or [])
    return body


def _generate_statement(stmt: SQLStatement, step_tracking: bool = True) -> str:
    """Generate Python/PySpark code for a single classified statement."""

    handlers = {
        StatementType.COMMIT: _gen_commit,
        StatementType.ROLLBACK: _gen_rollback,
        StatementType.STATUS_INSERT: _gen_status_insert,
        StatementType.STATUS_UPDATE: _gen_status_update,
        StatementType.TRUNCATE: _gen_truncate,
        StatementType.EXECUTE_IMMEDIATE_DDL: _gen_execute_immediate,
        StatementType.INSERT_SELECT: _gen_insert_select,
        StatementType.INSERT_VALUES: _gen_insert_values,
        StatementType.UPDATE_CORRELATED: _gen_update_correlated,
        StatementType.UPDATE_SIMPLE: _gen_update_simple,
        StatementType.DELETE: _gen_delete,
        StatementType.SELECT_INTO: _gen_select_into,
        StatementType.DBMS_STATS: _gen_dbms_stats,
        StatementType.DBMS_SCHEDULER: _gen_dbms_scheduler,
        StatementType.DBMS_SESSION: _gen_dbms_session,
        StatementType.REMOTE_PROC_CALL: _gen_remote_proc,
        StatementType.VARIABLE_ASSIGNMENT: _gen_variable_assignment,
        StatementType.IF_BLOCK: _gen_if_block,
        StatementType.FOR_LOOP: _gen_for_loop,
        StatementType.GOTO: _gen_goto,
        StatementType.LABEL: _gen_label,
        StatementType.BEGIN_END_BLOCK: _gen_begin_end,
        StatementType.UNKNOWN: _gen_unknown,
    }

    handler = handlers.get(stmt.type, _gen_unknown)
    return handler(stmt)


def _gen_commit(stmt: SQLStatement) -> str:
    return '    # COMMIT — write checkpoint (Spark writes are atomic per operation)'


def _gen_rollback(stmt: SQLStatement) -> str:
    return '    # ROLLBACK — handled by exception/status tracking decorator'


def _gen_status_insert(stmt: SQLStatement) -> str:
    return '    # Status tracking INSERT — handled by @track_status decorator'


def _gen_status_update(stmt: SQLStatement) -> str:
    return '    # Status tracking UPDATE — handled by @track_status decorator'


def _gen_truncate(stmt: SQLStatement) -> str:
    table = stmt.target_table or 'UNKNOWN_TABLE'
    # Remove @dbloans
    table = re.sub(r'@\w+', '', table)
    return f'    spark.sql("TRUNCATE TABLE {table}")'


def _gen_execute_immediate(stmt: SQLStatement) -> str:
    code = transpile_execute_immediate(stmt.sql)
    return _indent_code(code, 4)


def _gen_insert_select(stmt: SQLStatement) -> str:
    result = transpile_sql(stmt.sql)
    transpiled = result.transpiled

    # Format as spark.sql() call
    warnings = ''
    if result.warnings:
        warnings = '\n'.join(f'    # WARNING: {w}' for w in result.warnings) + '\n'

    # Clean up the SQL for embedding
    sql_clean = _format_sql_string(transpiled)

    target = stmt.target_table or 'UNKNOWN'
    target = re.sub(r'@\w+', '', target)

    return f'{warnings}    # INSERT INTO {target}\n    spark.sql("""\n{_indent_code(sql_clean, 8)}\n    """)'


def _gen_insert_values(stmt: SQLStatement) -> str:
    result = transpile_sql(stmt.sql)
    sql_clean = _format_sql_string(result.transpiled)
    return f'    spark.sql("""\n{_indent_code(sql_clean, 8)}\n    """)'


def _gen_update_correlated(stmt: SQLStatement) -> str:
    target = stmt.target_table or 'UNKNOWN'
    target = re.sub(r'@\w+', '', target)

    # First, apply standard transpilation (NVL, SYSDATE, @Dbloans removal, etc.)
    result = transpile_sql(stmt.sql)
    preprocessed_sql = result.transpiled

    # Now try to generate a MERGE statement from the preprocessed SQL
    merge_sql = transpile_correlated_update(preprocessed_sql)

    warnings = ''
    if result.warnings:
        warnings = '\n'.join(f'    # WARNING: {w}' for w in result.warnings) + '\n'

    if 'TODO' in merge_sql:
        # Couldn't auto-convert — provide the preprocessed SQL with guidance
        return (
            f'{warnings}    # TODO: Correlated UPDATE on {target} — convert to MERGE or DataFrame join\n'
            f'    spark.sql("""\n{_indent_code(preprocessed_sql, 8)}\n    """)'
        )

    return f'{warnings}    # Correlated UPDATE on {target}\n    spark.sql("""\n{_indent_code(merge_sql, 8)}\n    """)'


def _gen_update_simple(stmt: SQLStatement) -> str:
    result = transpile_sql(stmt.sql)
    sql_clean = _format_sql_string(result.transpiled)

    warnings = ''
    if result.warnings:
        warnings = '\n'.join(f'    # WARNING: {w}' for w in result.warnings) + '\n'

    target = stmt.target_table or 'UNKNOWN'
    target = re.sub(r'@\w+', '', target)

    return f'{warnings}    # UPDATE {target}\n    spark.sql("""\n{_indent_code(sql_clean, 8)}\n    """)'


def _gen_delete(stmt: SQLStatement) -> str:
    result = transpile_sql(stmt.sql)
    sql_clean = _format_sql_string(result.transpiled)
    return f'    # DELETE\n    spark.sql("""\n{_indent_code(sql_clean, 8)}\n    """)'


def _gen_select_into(stmt: SQLStatement) -> str:
    # SELECT ... INTO variable — convert to spark.sql().collect()
    sql = stmt.sql

    # Extract the variable name(s)
    into_match = re.search(r'\bInto\s+(\w+(?:\s*,\s*\w+)*)', sql, re.IGNORECASE)
    variables = []
    if into_match:
        variables = [v.strip() for v in into_match.group(1).split(',')]

    # Remove the INTO clause from the SQL
    clean_sql = re.sub(r'\s+Into\s+\w+(?:\s*,\s*\w+)*', '', sql, flags=re.IGNORECASE)

    result = transpile_sql(clean_sql)
    sql_clean = _format_sql_string(result.transpiled)

    if len(variables) == 1:
        return f'    # SELECT INTO {variables[0]}\n    _row = spark.sql("""\n{_indent_code(sql_clean, 8)}\n    """).collect()\n    {variables[0]} = _row[0][0] if _row else 0'
    elif len(variables) > 1:
        var_assigns = '\n'.join(f'    {v} = _row[0][{i}] if _row else None' for i, v in enumerate(variables))
        return f'    # SELECT INTO ({", ".join(variables)})\n    _row = spark.sql("""\n{_indent_code(sql_clean, 8)}\n    """).collect()\n{var_assigns}'
    else:
        return f'    spark.sql("""\n{_indent_code(sql_clean, 8)}\n    """)'


def _gen_dbms_stats(stmt: SQLStatement) -> str:
    # Extract schema and table name
    match = re.search(r"Gather_Table_Stats\s*\(\s*'(\w+)'\s*,\s*'(\w+)'", stmt.sql, re.IGNORECASE)
    if match:
        schema = match.group(1).lower()
        table = match.group(2)
        return f'    spark.sql("ANALYZE TABLE {schema}.{table} COMPUTE STATISTICS")'
    return '    # DBMS_STATS — Spark handles statistics automatically'


def _gen_dbms_scheduler(stmt: SQLStatement) -> str:
    # Extract job details
    name_match = re.search(r"Job_Name\s*=>\s*'(\w+)'", stmt.sql, re.IGNORECASE)
    action_match = re.search(r"Job_Action\s*=>\s*'(.+?)'", stmt.sql, re.IGNORECASE | re.DOTALL)

    job_name = name_match.group(1) if name_match else 'UNKNOWN_JOB'
    job_action = action_match.group(1) if action_match else 'unknown'

    # Extract procedure calls from job action
    # Generic: extract procedure names from job action PL/SQL blocks
    proc_calls = re.findall(r'(?:\w+\.)*(\w+)\s*(?:\(|;)', job_action)
    _noise_kw = {'begin', 'end', 'null', 'exception', 'commit', 'rollback', 'declare'}
    proc_calls = [p for p in proc_calls if p.lower() not in _noise_kw]

    if proc_calls:
        call_str = ', '.join(f'"{p}"' for p in proc_calls)
        return (
            f'    # DBMS_SCHEDULER job: {job_name}\n'
            f'    # Original action: {job_action.strip()}\n'
            f'    # Procedures to run: {call_str}\n'
            f'    executor.submit({proc_calls[0]}, spark)  # Parallel execution'
        )
    return f'    # TODO: DBMS_SCHEDULER job {job_name} — needs manual conversion\n    # Action: {job_action}'


def _gen_dbms_session(stmt: SQLStatement) -> str:
    match = re.search(r'Sleep\s*\(\s*(\d+)\s*\)', stmt.sql, re.IGNORECASE)
    seconds = match.group(1) if match else '10'
    return f'    time.sleep({seconds})'


def _gen_remote_proc(stmt: SQLStatement) -> str:
    return (
        f'    # TODO: Remote procedure call — requires reimplementation or pre-execution\n'
        f'    # Original: {stmt.sql.strip()}'
    )


def _gen_variable_assignment(stmt: SQLStatement) -> str:
    sql = stmt.sql.strip().rstrip(';')

    # v_Step_No := N  ->  step_no = N
    step_match = re.match(r'v_Step_No\s*:=\s*(\d+)', sql, re.IGNORECASE)
    if step_match:
        return f'    step_no = {step_match.group(1)}'

    # v_Err := Sqlerrm  ->  handled by exception
    if 'sqlerrm' in sql.lower():
        return '    # v_Err = str(e) — handled in exception block'

    # General variable assignment
    match = re.match(r'(\w+)\s*:=\s*(.+)', sql)
    if match:
        var_name = _oracle_var_to_python(match.group(1))
        value = match.group(2).strip()

        # Transpile the value if it contains Oracle functions
        result = transpile_sql(f"SELECT {value}")
        transpiled_value = result.transpiled
        if transpiled_value.upper().startswith('SELECT '):
            transpiled_value = transpiled_value[7:].strip()

        return f'    {var_name} = {transpiled_value}'

    return f'    # Variable assignment: {sql}'


def _gen_if_block(stmt: SQLStatement) -> str:
    """Convert PL/SQL IF block to Python if/elif/else."""
    lines = stmt.sql.split('\n')
    result_lines = []

    for line in lines:
        stripped = line.strip()

        # IF condition THEN
        if_match = re.match(r'^If\s+(.+?)\s+Then\s*$', stripped, re.IGNORECASE)
        if if_match:
            condition = _translate_condition(if_match.group(1))
            result_lines.append(f'    if {condition}:')
            continue

        # ELSIF condition THEN
        elsif_match = re.match(r'^Elsif\s+(.+?)\s+Then\s*$', stripped, re.IGNORECASE)
        if elsif_match:
            condition = _translate_condition(elsif_match.group(1))
            result_lines.append(f'    elif {condition}:')
            continue

        # ELSE
        if re.match(r'^Else\s*$', stripped, re.IGNORECASE):
            result_lines.append('    else:')
            continue

        # END IF
        if re.match(r'^End\s+If\s*;?\s*$', stripped, re.IGNORECASE):
            continue

        # Inner statements — recursively handle
        if stripped and not stripped.startswith('--'):
            # Procedure/function calls (generic — any identifier with optional args and semicolon)
            proc_match = re.match(r'^([A-Za-z]\w*(?:\.\w+)*)\s*(?:\((.*?)\))?\s*;', stripped, re.IGNORECASE)
            if proc_match:
                func_name = proc_match.group(1)
                args = proc_match.group(2) or ''
                # Transpile arguments
                if args:
                    result = transpile_sql(f"SELECT {args}")
                    transpiled_args = result.transpiled
                    if transpiled_args.upper().startswith('SELECT '):
                        transpiled_args = transpiled_args[7:].strip()
                    result_lines.append(f'        {func_name}(spark, {transpiled_args})')
                else:
                    result_lines.append(f'        {func_name}(spark)')
                continue

            # SELECT INTO inside IF
            if re.match(r'^Select\s+', stripped, re.IGNORECASE):
                inner_stmt = SQLStatement(
                    type=StatementType.SELECT_INTO,
                    sql=stripped,
                    line_start=0, line_end=0,
                    oracle_constructs=[]
                )
                # Collect multi-line SELECT
                select_text = _collect_until_semicolon(lines, lines.index(line))
                inner_stmt.sql = select_text
                code = _gen_select_into(inner_stmt)
                # Add extra indentation for inside-if
                result_lines.append('    ' + code.strip())
                continue

            # Begin...End blocks (DBMS_SCHEDULER)
            if re.match(r'^Begin\s*$', stripped, re.IGNORECASE):
                result_lines.append('        # Nested BEGIN...END block')
                continue
            if re.match(r'^End\s*;', stripped, re.IGNORECASE):
                continue

            # Pass through other content
            if stripped and not re.match(r'^(Begin|End)', stripped, re.IGNORECASE):
                result_lines.append(f'        # {stripped}')

    if not result_lines:
        return f'    # IF block — needs manual conversion\n    # {stmt.sql}'

    return '\n'.join(result_lines)


def _gen_for_loop(stmt: SQLStatement) -> str:
    """Convert PL/SQL FOR cursor loop to Python/PySpark."""
    # FOR rec IN (SELECT ...) LOOP ... END LOOP;
    match = re.search(r'For\s+(\w+)\s+In\s+\((.+?)\)\s*Loop\s*(.+?)\s*End\s+Loop',
                      stmt.sql, re.IGNORECASE | re.DOTALL)

    if match:
        var = match.group(1)
        select_sql = match.group(2)
        loop_body = match.group(3)

        result = transpile_sql(select_sql)
        transpiled_select = result.transpiled

        lines = [
            f'    # FOR loop converted from cursor',
            f'    _cursor_df = spark.sql("""',
            f'        {transpiled_select}',
            f'    """)',
            f'    for {var} in _cursor_df.collect():',
        ]

        # Convert loop body statements
        for body_line in loop_body.strip().split(';'):
            body_line = body_line.strip()
            if body_line:
                lines.append(f'        # {body_line}')

        return '\n'.join(lines)

    return f'    # TODO: FOR loop needs manual conversion\n    # {stmt.sql}'


def _gen_goto(stmt: SQLStatement) -> str:
    label = re.search(r'Goto\s+(\w+)', stmt.sql, re.IGNORECASE)
    label_name = label.group(1) if label else 'unknown'
    return f'    continue  # GOTO {label_name} — converted to loop continue'


def _gen_label(stmt: SQLStatement) -> str:
    label = re.search(r'<<(\w+)>>', stmt.sql)
    label_name = label.group(1) if label else 'unknown'
    return f'    # Label: {label_name} — see while loop above'


def _gen_begin_end(stmt: SQLStatement) -> str:
    """Convert nested BEGIN...END blocks."""
    # Check for simple exception swallowing pattern:
    # Begin ... Exception When Others Then Null; End;
    if re.search(r'Exception\s+When\s+Others\s+Then\s+Null', stmt.sql, re.IGNORECASE):
        # Extract the inner SQL
        inner_match = re.search(r'Begin\s+(.+?)\s+Exception', stmt.sql, re.IGNORECASE | re.DOTALL)
        if inner_match:
            inner_sql = inner_match.group(1).strip()
            return (
                f'    try:\n'
                f'        # {inner_sql}\n'
                f'        pass  # TODO: convert inner SQL\n'
                f'    except Exception:\n'
                f'        pass  # Oracle: WHEN OTHERS THEN NULL'
            )

    return f'    # Nested BEGIN...END block\n    # {stmt.sql[:200]}...'


def _gen_unknown(stmt: SQLStatement) -> str:
    return f'    # TODO: Unclassified statement\n    # {stmt.sql.strip()}'


def _generate_comment_block(stmt: SQLStatement) -> str:
    """Convert commented-out PL/SQL to Python comments."""
    lines = stmt.sql.split('\n')
    result = ['    # --- Commented-out Oracle code ---']
    for line in lines[:10]:  # Limit to 10 lines
        result.append(f'    # {line.strip()}')
    if len(lines) > 10:
        result.append(f'    # ... ({len(lines) - 10} more lines)')
    return '\n'.join(result)


def _translate_condition(oracle_cond: str) -> str:
    """Translate Oracle IF condition to Python."""
    cond = oracle_cond.strip()

    # Variable comparisons
    cond = re.sub(r'\b(\w+)\s*<>\s*(\w+)', r'\1 != \2', cond)
    cond = re.sub(r'\bIs\s+Null\b', 'is None', cond, flags=re.IGNORECASE)
    cond = re.sub(r'\bIs\s+Not\s+Null\b', 'is not None', cond, flags=re.IGNORECASE)
    cond = re.sub(r'\bAnd\b', 'and', cond, flags=re.IGNORECASE)
    cond = re.sub(r'\bOr\b', 'or', cond, flags=re.IGNORECASE)
    cond = re.sub(r'\bNot\b', 'not', cond, flags=re.IGNORECASE)

    # Oracle variable prefix
    cond = cond.replace('Lv_', 'lv_').replace('v_', '')
    cond = cond.replace('Ln_', 'ln_')

    return cond


def _oracle_var_to_python(name: str) -> str:
    """Convert Oracle variable name to Python convention."""
    # v_Step_No -> step_no, Lv_Yyyymm -> lv_yyyymm
    # Only strip `v_`/`V_` at the START so `Lv_Yyyymm` is not butchered.
    name = re.sub(r'^[vV]_', '', name)
    # Convert CamelCase to snake_case WITHOUT inserting a second underscore
    # after an existing one (so `i_Yyyymm` -> `i_yyyymm`, not `i__yyyymm`).
    result = re.sub(
        r'(?<!^)(?<!_)([A-Z])',
        lambda m: '_' + m.group(1).lower(),
        name,
    )
    result = result.lstrip('_').lower()
    return result


# Oracle bind-variable naming convention:
#   parameters:  i_Foo, I_Foo, o_Foo, O_Foo, io_Foo
#   locals:      v_Foo, V_Foo, Lv_Foo, Ln_Foo, Lb_Foo, Lc_Foo
_BIND_VAR_RE = re.compile(
    r"\b((?:[Ii]|[Oo]|[Ii][Oo]|[Ll][vnbc]|[vV])_[A-Za-z]\w*)\b"
)

# Regex to locate `spark.sql("""...""")` blocks (single- or multi-line).
_SPARK_SQL_RE = re.compile(
    r"(spark\.sql\()(\"\"\")([\s\S]*?)(\"\"\"\))",
    re.DOTALL,
)

# Python assignment at function scope (4-space indent):  "    lv_yyyymm = ..."
_PY_ASSIGN_RE = re.compile(r"^ {4}([a-z_][a-z0-9_]*)\s*=", re.MULTILINE)


def _interpolate_bind_vars_in_sql_blocks(body: str, params: list[str]) -> str:
    # Replace Oracle bind-variable references inside spark.sql literal blocks
    # with f-string interpolation {python_var}. Without this fix the
    # deterministic converter emits SQL strings that still literally contain
    # names like `Lv_Yyyymm` — Spark parses those as column identifiers and
    # the query either fails or silently returns bad data.
    # Step 1: Build the rename map — Oracle bind name → Python identifier.
    rename_map: dict[str, str] = {}

    # Parameters, e.g. `i_Yyyymm` → `i_yyyymm`
    for p in params or []:
        p = p.strip()
        if _BIND_VAR_RE.fullmatch(p):
            rename_map[p] = _oracle_var_to_python(p)

    # Local assignments — scan the generated body for names already in
    # Python form (`lv_yyyymm = ...`) and map the Oracle-cased form back.
    py_locals = set(_PY_ASSIGN_RE.findall(body))
    for py_name in py_locals:
        # Reverse-engineer the likely Oracle form(s) and add to map.
        if py_name.startswith(("lv_", "ln_", "lb_", "lc_", "i_", "o_", "io_", "v_")):
            # Re-camelcase after the prefix so `lv_yyyymm` ↔ `Lv_Yyyymm`
            prefix, _, rest = py_name.partition("_")
            if rest:
                camel = "_".join(w.capitalize() for w in rest.split("_"))
                oracle_form = f"{prefix.capitalize()}_{camel}"
                rename_map.setdefault(oracle_form, py_name)
                # Also the all-lowercase prefix variant (v_ → v_)
                rename_map.setdefault(f"{prefix}_{camel}", py_name)

    if not rename_map:
        return body

    def _rewrite(match: re.Match) -> str:
        prefix, open_q, sql_body, close_q = match.groups()
        # Substitute only for names that actually appear.
        refs_found = set()
        def _sub(m):
            name = m.group(1)
            py = rename_map.get(name)
            if py is None:
                return name
            refs_found.add(py)
            return "{" + py + "}"
        new_sql = _BIND_VAR_RE.sub(_sub, sql_body)
        if not refs_found:
            return match.group(0)  # nothing changed — keep as raw string
        # Escape bare `{` / `}` that were already in the SQL (e.g. JSON path)
        # so the f-string doesn't mis-parse them. Only escape braces that are
        # NOT ours — i.e. not immediately around one of our inserted vars.
        # Simple approach: double any `{` or `}` not followed/preceded by our
        # placeholder. Since placeholders look like `{py_name}` we can split
        # on them.
        parts = re.split(r"(\{[a-z_][a-z0-9_]*\})", new_sql)
        for i, part in enumerate(parts):
            if part.startswith("{") and part.endswith("}"):
                continue
            parts[i] = part.replace("{", "{{").replace("}", "}}")
        safe_sql = "".join(parts)
        return f'{prefix}f{open_q}{safe_sql}{close_q}'

    return _SPARK_SQL_RE.sub(_rewrite, body)


def _format_sql_string(sql: str) -> str:
    """Format SQL for embedding in a Python triple-quoted string."""
    # Ensure proper line breaks for readability
    sql = sql.strip()
    # Escape any triple quotes
    sql = sql.replace('"""', '\\"\\"\\"')
    return sql


def _indent_code(code: str, spaces: int) -> str:
    """Indent all lines of code."""
    prefix = ' ' * spaces
    return '\n'.join(prefix + line if line.strip() else '' for line in code.split('\n'))


def _collect_until_semicolon(lines: list[str], start: int) -> str:
    """Collect lines from start until a semicolon is found."""
    result = []
    for i in range(start, len(lines)):
        result.append(lines[i].strip())
        if lines[i].strip().endswith(';'):
            break
    return ' '.join(result)


def generate_orchestrator(procedures=None, proc_statements=None,
                          orchestrator_proc_name=None) -> str:
    """Generate an orchestrator function from the call graph and scheduler semantics.

    If an orchestrator procedure is detected (one that uses DBMS_SCHEDULER to
    launch other procedures), this generates a ThreadPoolExecutor-based equivalent.

    If no orchestrator is detected, generates a simple sequential runner
    that calls all procedures in order.

    Args:
        procedures: List of ProcedureInfo objects
        proc_statements: Dict of proc name → statement data
        orchestrator_proc_name: Name of the detected orchestrator procedure (or None)
    """
    # Legacy call signature support: generate_orchestrator([]) still works
    if procedures is None or not procedures:
        return _generate_sequential_runner([])

    # Collect all procedure names (excluding the orchestrator itself)
    all_proc_names = [
        p.name for p in procedures
        if p.name != orchestrator_proc_name
    ]

    # If no orchestrator detected, generate a simple sequential runner
    if not orchestrator_proc_name or orchestrator_proc_name not in (
        p.name for p in procedures
    ):
        return _generate_sequential_runner(all_proc_names)

    # Extract scheduler job info from the orchestrator procedure
    orch_data = proc_statements.get(orchestrator_proc_name, {})
    orch_stmts = orch_data.get('statements', [])

    # Find which procs are launched via DBMS_SCHEDULER (parallel candidates)
    from .sql_extractor import StatementType
    parallel_procs = []
    for stmt in orch_stmts:
        if stmt.type == StatementType.DBMS_SCHEDULER:
            # Extract procedure name from job action
            action_match = re.search(r"Job_Action\s*=>\s*'(.+?)'", stmt.sql,
                                     re.IGNORECASE | re.DOTALL)
            if action_match:
                action = action_match.group(1)
                # Generic extraction: find schema.pkg.proc or just proc patterns
                called = re.findall(r'(?:\w+\.)*(\w+)\s*(?:\(|;)', action)
                for name in called:
                    # Match against known procedure names
                    for p in all_proc_names:
                        if p.lower() == name.lower() or p.lower().endswith(name.lower()):
                            if p not in parallel_procs:
                                parallel_procs.append(p)

    # Remaining procs that are called directly (not via scheduler)
    sequential_procs = [p for p in all_proc_names if p not in parallel_procs]

    return _generate_inferred_orchestrator(
        orchestrator_proc_name, parallel_procs, sequential_procs
    )


def _generate_sequential_runner(proc_names: list[str]) -> str:
    """Generate a simple sequential orchestrator when no scheduler is detected."""
    lines = [
        'import logging',
        '',
        'logger = logging.getLogger(__name__)',
        '',
        '',
        'def run_all(spark):',
        '    """Run all converted procedures sequentially."""',
    ]
    if not proc_names:
        lines.append('    pass  # No procedures to orchestrate')
    else:
        for name in proc_names:
            lines.append(f'    logger.info("Running {name}...")')
            lines.append(f'    {name}(spark)')
    lines.append('')
    return '\n'.join(lines)


def _generate_inferred_orchestrator(orch_name: str, parallel_procs: list[str],
                                     sequential_procs: list[str]) -> str:
    """Generate a ThreadPoolExecutor orchestrator inferred from scheduler semantics."""
    func_name = _oracle_var_to_python(orch_name) if orch_name else 'run_pipeline'
    lines = [
        'import time',
        'import logging',
        'from concurrent.futures import ThreadPoolExecutor, as_completed',
        '',
        'logger = logging.getLogger(__name__)',
        '',
        '',
        f'def {func_name}(spark):',
        f'    """Orchestrator — inferred from DBMS_SCHEDULER usage in {orch_name}.',
        '',
        '    NOTE: This orchestrator was auto-generated from call graph analysis.',
        '    Review the parallel/sequential grouping and add any missing',
        '    status checks or conditional logic from the original procedure.',
        '    """',
    ]

    if parallel_procs:
        lines.append('')
        lines.append(f'    # Phase 1: Parallel execution ({len(parallel_procs)} procedures from DBMS_SCHEDULER)')
        lines.append(f'    parallel_procs = [')
        for p in parallel_procs:
            lines.append(f'        ("{p}", {p}),')
        lines.append(f'    ]')
        lines.append('')
        lines.append(f'    logger.info("Starting {len(parallel_procs)} parallel procedures")')
        lines.append(f'    with ThreadPoolExecutor(max_workers={len(parallel_procs)}) as executor:')
        lines.append(f'        futures = {{')
        lines.append(f'            executor.submit(proc_func, spark): proc_name')
        lines.append(f'            for proc_name, proc_func in parallel_procs')
        lines.append(f'        }}')
        lines.append(f'        for future in as_completed(futures):')
        lines.append(f'            proc_name = futures[future]')
        lines.append(f'            try:')
        lines.append(f'                future.result()')
        lines.append(f'                logger.info(f"  {{proc_name}} completed")')
        lines.append(f'            except Exception as e:')
        lines.append(f'                logger.error(f"  {{proc_name}} failed: {{e}}")')
        lines.append(f'                raise')

    if sequential_procs:
        lines.append('')
        lines.append(f'    # Phase 2: Sequential execution ({len(sequential_procs)} remaining procedures)')
        for p in sequential_procs:
            lines.append(f'    logger.info("Running {p}...")')
            lines.append(f'    {p}(spark)')

    lines.append('')
    lines.append(f'    logger.info("Orchestrator complete")')
    lines.append('')
    return '\n'.join(lines)
