"""
IR-based PySpark Code Generator

Walks a ProcedureIR tree and emits PySpark Python code. This replaces the
flat statement-list code_generator.py with a tree-walking approach that
handles nested control flow properly.

Both the regex path (via regex_to_ir adapter) and the future ANTLR path
produce ProcedureIR, so this single generator serves both.
"""

import re
from .ir import IRNode, IRNodeType, ProcedureIR, SQLInfo, CursorLoopInfo
from .transpiler import transpile_sql, transpile_correlated_update, transpile_execute_immediate

# Module-level LLM bridge — None by default (zero impact when disabled)
_llm_bridge = None


def set_llm_bridge(bridge):
    """Set the LLM bridge for fallback conversions. Pass None to disable."""
    global _llm_bridge
    _llm_bridge = bridge


def generate_procedure_from_ir(proc_ir: ProcedureIR) -> str:
    """Generate a complete PySpark function from a ProcedureIR tree.

    Args:
        proc_ir: The IR tree for a single procedure

    Returns:
        Complete Python function as a string
    """
    # Transform GOTO patterns to while loops before generating code
    if proc_ir.conversion_path == "antlr":
        proc_ir.body = _transform_goto_to_loops(proc_ir.body)

    lines = []

    # Decorator
    lines.append(f'@track_status("{proc_ir.status_name}")')

    # Function signature
    param_str = ', '.join(proc_ir.parameters) if proc_ir.parameters else ''
    func_sig = f"def {proc_ir.name}(spark{', ' + param_str if param_str else ''}):"
    lines.append(func_sig)
    lines.append(f'    """Converted from Oracle PL/SQL procedure {proc_ir.name}."""')

    if proc_ir.has_step_tracking:
        lines.append('    step_no = 0')
    lines.append('')

    # Generate body
    for node in proc_ir.body:
        generated = _generate_node(node, indent=4)
        if generated:
            lines.append(generated)

    # If no meaningful statements, add pass
    meaningful_types = {
        IRNodeType.SQL_STATEMENT, IRNodeType.EXECUTE_IMMEDIATE,
        IRNodeType.IF_BLOCK, IRNodeType.FOR_CURSOR_LOOP,
        IRNodeType.WHILE_LOOP, IRNodeType.BEGIN_END_BLOCK,
        IRNodeType.PROCEDURE_CALL, IRNodeType.VARIABLE_ASSIGNMENT,
        IRNodeType.DBMS_SCHEDULER, IRNodeType.UNKNOWN,
    }
    has_meaningful = any(n.node_type in meaningful_types for n in proc_ir.body)
    if not has_meaningful:
        lines.append('    pass')

    body = '\n'.join(lines)
    # Rewrite `spark.sql("""... i_Yyyymm ...""")` → f-string with {i_yyyymm}
    # so Oracle bind variables resolve to their Python values instead of
    # landing in SQL as literal column references. Without this the semantic
    # review consistently flags "parameter not bound" and kicks the chunk
    # into a 10-call repair loop.
    from .code_generator import _interpolate_bind_vars_in_sql_blocks
    body = _interpolate_bind_vars_in_sql_blocks(body, proc_ir.parameters or [])
    return body


def _generate_node(node: IRNode, indent: int = 4) -> str:
    """Generate code for a single IR node. Dispatches by node type."""
    prefix = ' ' * indent

    handlers = {
        IRNodeType.COMMIT: _gen_commit,
        IRNodeType.ROLLBACK: _gen_rollback,
        IRNodeType.STATUS_TRACKING: _gen_status_tracking,
        IRNodeType.SQL_STATEMENT: _gen_sql_statement,
        IRNodeType.EXECUTE_IMMEDIATE: _gen_execute_immediate,
        IRNodeType.VARIABLE_ASSIGNMENT: _gen_variable_assignment,
        IRNodeType.IF_BLOCK: _gen_if_block,
        IRNodeType.FOR_CURSOR_LOOP: _gen_for_loop,
        IRNodeType.GOTO: _gen_goto,
        IRNodeType.LABEL: _gen_label,
        IRNodeType.WHILE_LOOP: _gen_while_loop,
        IRNodeType.BEGIN_END_BLOCK: _gen_begin_end,
        IRNodeType.DBMS_STATS: _gen_dbms_stats,
        IRNodeType.DBMS_SCHEDULER: _gen_dbms_scheduler,
        IRNodeType.DBMS_SESSION: _gen_dbms_session,
        IRNodeType.PROCEDURE_CALL: _gen_remote_proc,
        IRNodeType.COMMENT_BLOCK: _gen_comment_block,
        IRNodeType.UNKNOWN: _gen_unknown,
    }

    handler = handlers.get(node.node_type, _gen_unknown)
    return handler(node, indent)


# --- Node handlers ---
# Each returns a string with proper indentation.


def _gen_commit(node: IRNode, indent: int) -> str:
    return ' ' * indent + '# COMMIT — write checkpoint (Spark writes are atomic per operation)'


def _gen_rollback(node: IRNode, indent: int) -> str:
    return ' ' * indent + '# ROLLBACK — handled by exception/status tracking decorator'


def _gen_status_tracking(node: IRNode, indent: int) -> str:
    si = node.sql_info
    if si and si.statement_type == 'STATUS_INSERT':
        return ' ' * indent + '# Status tracking INSERT — handled by @track_status decorator'
    return ' ' * indent + '# Status tracking UPDATE — handled by @track_status decorator'


def _gen_sql_statement(node: IRNode, indent: int) -> str:
    """Generate code for a SQL statement (INSERT, UPDATE, DELETE, SELECT INTO, TRUNCATE)."""
    si = node.sql_info
    if not si:
        return ' ' * indent + '# Empty SQL statement'

    stype = si.statement_type
    prefix = ' ' * indent

    if stype == 'TRUNCATE':
        return _gen_truncate(node, indent)
    elif stype == 'INSERT_SELECT':
        return _gen_insert_select(node, indent)
    elif stype == 'INSERT_VALUES':
        return _gen_insert_values(node, indent)
    elif stype == 'UPDATE_CORRELATED':
        return _gen_update_correlated(node, indent)
    elif stype == 'UPDATE_SIMPLE':
        return _gen_update_simple(node, indent)
    elif stype == 'DELETE':
        return _gen_delete(node, indent)
    elif stype == 'SELECT_INTO':
        return _gen_select_into(node, indent)
    else:
        return _gen_unknown(node, indent)


def _gen_truncate(node: IRNode, indent: int) -> str:
    prefix = ' ' * indent
    table = (node.sql_info.target_table or 'UNKNOWN_TABLE') if node.sql_info else 'UNKNOWN_TABLE'
    table = re.sub(r'@\w+', '', table)
    return f'{prefix}spark.sql("TRUNCATE TABLE {table}")'


def _gen_insert_select(node: IRNode, indent: int) -> str:
    prefix = ' ' * indent
    si = node.sql_info
    result = transpile_sql(si.raw_sql)
    transpiled = result.transpiled

    warnings = ''
    if result.warnings:
        warnings = '\n'.join(f'{prefix}# WARNING: {w}' for w in result.warnings) + '\n'

    sql_clean = _format_sql_string(transpiled)
    target = si.target_table or 'UNKNOWN'
    target = re.sub(r'@\w+', '', target)

    return f'{warnings}{prefix}# INSERT INTO {target}\n{prefix}spark.sql("""\n{_indent_code(sql_clean, indent + 4)}\n{prefix}""")'


def _gen_insert_values(node: IRNode, indent: int) -> str:
    prefix = ' ' * indent
    si = node.sql_info
    result = transpile_sql(si.raw_sql)
    sql_clean = _format_sql_string(result.transpiled)
    return f'{prefix}spark.sql("""\n{_indent_code(sql_clean, indent + 4)}\n{prefix}""")'


def _gen_update_correlated(node: IRNode, indent: int) -> str:
    prefix = ' ' * indent
    si = node.sql_info
    target = si.target_table or 'UNKNOWN'
    target = re.sub(r'@\w+', '', target)

    result = transpile_sql(si.raw_sql)
    preprocessed_sql = result.transpiled
    merge_sql = transpile_correlated_update(preprocessed_sql)

    warnings = ''
    if result.warnings:
        warnings = '\n'.join(f'{prefix}# WARNING: {w}' for w in result.warnings) + '\n'

    if 'TODO' in merge_sql:
        # Point B: LLM fallback for correlated UPDATE → MERGE
        if _llm_bridge is not None:
            llm_result = _llm_bridge.convert_correlated_update(
                oracle_sql=si.raw_sql,
                preprocessed_sql=preprocessed_sql,
                target_table=target,
            )
            if llm_result:
                return (
                    f'{warnings}{prefix}# Correlated UPDATE on {target} (LLM-assisted)\n'
                    f'{prefix}spark.sql("""\n{_indent_code(llm_result, indent + 4)}\n{prefix}""")'
                )
        return (
            f'{warnings}{prefix}# TODO: Correlated UPDATE on {target} — convert to MERGE or DataFrame join\n'
            f'{prefix}spark.sql("""\n{_indent_code(preprocessed_sql, indent + 4)}\n{prefix}""")'
        )

    return f'{warnings}{prefix}# Correlated UPDATE on {target}\n{prefix}spark.sql("""\n{_indent_code(merge_sql, indent + 4)}\n{prefix}""")'


def _gen_update_simple(node: IRNode, indent: int) -> str:
    prefix = ' ' * indent
    si = node.sql_info
    result = transpile_sql(si.raw_sql)
    sql_clean = _format_sql_string(result.transpiled)

    warnings = ''
    if result.warnings:
        warnings = '\n'.join(f'{prefix}# WARNING: {w}' for w in result.warnings) + '\n'

    target = si.target_table or 'UNKNOWN'
    target = re.sub(r'@\w+', '', target)

    return f'{warnings}{prefix}# UPDATE {target}\n{prefix}spark.sql("""\n{_indent_code(sql_clean, indent + 4)}\n{prefix}""")'


def _gen_delete(node: IRNode, indent: int) -> str:
    prefix = ' ' * indent
    si = node.sql_info
    result = transpile_sql(si.raw_sql)
    sql_clean = _format_sql_string(result.transpiled)
    return f'{prefix}# DELETE\n{prefix}spark.sql("""\n{_indent_code(sql_clean, indent + 4)}\n{prefix}""")'


def _gen_select_into(node: IRNode, indent: int) -> str:
    prefix = ' ' * indent
    si = node.sql_info
    sql = si.raw_sql

    # Extract variable name(s)
    into_match = re.search(r'\bInto\s+(\w+(?:\s*,\s*\w+)*)', sql, re.IGNORECASE)
    variables = []
    if into_match:
        variables = [v.strip() for v in into_match.group(1).split(',')]

    # Remove INTO clause
    clean_sql = re.sub(r'\s+Into\s+\w+(?:\s*,\s*\w+)*', '', sql, flags=re.IGNORECASE)

    result = transpile_sql(clean_sql)
    sql_clean = _format_sql_string(result.transpiled)

    if len(variables) == 1:
        return f'{prefix}# SELECT INTO {variables[0]}\n{prefix}_row = spark.sql("""\n{_indent_code(sql_clean, indent + 4)}\n{prefix}""").collect()\n{prefix}{variables[0]} = _row[0][0] if _row else 0'
    elif len(variables) > 1:
        var_assigns = '\n'.join(f'{prefix}{v} = _row[0][{i}] if _row else None' for i, v in enumerate(variables))
        return f'{prefix}# SELECT INTO ({", ".join(variables)})\n{prefix}_row = spark.sql("""\n{_indent_code(sql_clean, indent + 4)}\n{prefix}""").collect()\n{var_assigns}'
    else:
        return f'{prefix}spark.sql("""\n{_indent_code(sql_clean, indent + 4)}\n{prefix}""")'


def _gen_execute_immediate(node: IRNode, indent: int) -> str:
    si = node.sql_info
    code = transpile_execute_immediate(si.raw_sql)
    return _indent_code(code, indent)


def _gen_variable_assignment(node: IRNode, indent: int) -> str:
    prefix = ' ' * indent
    si = node.sql_info
    sql = si.raw_sql.strip().rstrip(';') if si else ''

    # v_Step_No := N -> step_no = N
    step_match = re.match(r'v_Step_No\s*:=\s*(\d+)', sql, re.IGNORECASE)
    if step_match:
        return f'{prefix}step_no = {step_match.group(1)}'

    # v_Err := Sqlerrm -> handled by exception
    if 'sqlerrm' in sql.lower():
        return f'{prefix}# v_Err = str(e) — handled in exception block'

    # General variable assignment
    match = re.match(r'(\w+)\s*:=\s*(.+)', sql, re.DOTALL)
    if match:
        var_name = _oracle_var_to_python(match.group(1))
        value = match.group(2).strip()

        # Check if this is a SQL string construction (multi-line with || concat)
        if '||' in value or ('\n' in value and "'" in value):
            # Emit as a Python string assignment with the SQL value
            safe_value = value.replace('\n', ' ').strip()
            # Replace || concatenation with Python +
            safe_value = safe_value.replace('||', '+')
            return f'{prefix}{var_name} = {safe_value}'

        # Transpile the value if it contains Oracle functions
        result = transpile_sql(f"SELECT {value}")
        transpiled_value = result.transpiled
        if transpiled_value.upper().startswith('SELECT '):
            transpiled_value = transpiled_value[7:].strip()

        # Ensure single-line result
        transpiled_value = transpiled_value.replace('\n', ' ').strip()

        return f'{prefix}{var_name} = {transpiled_value}'

    return f'{prefix}# Variable assignment: {sql}'


def _gen_if_block(node: IRNode, indent: int) -> str:
    """Convert IF block IR node to Python if/elif/else.

    If the IR has structured children (from enrichment), use them.
    Otherwise fall back to raw SQL parsing (same as old code_generator).
    """
    prefix = ' ' * indent

    # If we have structured children from _enrich_if_block
    if node.children:
        return _gen_if_from_children(node, indent)

    # Fallback: parse from raw SQL (matches old code_generator behavior)
    if node.sql_info:
        return _gen_if_from_raw_sql(node.sql_info.raw_sql, indent)

    return f'{prefix}# IF block — needs manual conversion'


def _gen_if_from_children(node: IRNode, indent: int) -> str:
    """Generate if/elif/else from structured IR children."""
    prefix = ' ' * indent
    inner_prefix = ' ' * (indent + 4)
    result_lines = []

    for i, child in enumerate(node.children):
        if child.node_type == IRNodeType.IF_BLOCK:
            condition = _translate_condition(child.condition or '')
            result_lines.append(f'{prefix}if {condition}:')
        elif child.node_type == IRNodeType.ELSIF_BRANCH:
            condition = _translate_condition(child.condition or '')
            result_lines.append(f'{prefix}elif {condition}:')
        elif child.node_type == IRNodeType.ELSE_BRANCH:
            result_lines.append(f'{prefix}else:')

        # ANTLR path: structured children
        if child.children:
            has_body = False
            for body_node in child.children:
                generated = _generate_node(body_node, indent + 4)
                if generated:
                    result_lines.append(generated)
                    has_body = True
            if not has_body:
                result_lines.append(f'{inner_prefix}pass')
        # Regex path: raw_text fallback
        elif child.raw_text:
            body_lines = child.raw_text.split('\n')
            for body_line in body_lines:
                body_line = body_line.strip()
                if not body_line or body_line.startswith('--'):
                    continue

                # Procedure calls
                proc_match = re.match(r'^([A-Za-z]\w*(?:\.\w+)*)\s*(?:\((.*?)\))?\s*;', body_line, re.IGNORECASE)
                if proc_match:
                    func_name = proc_match.group(1)
                    args = proc_match.group(2) or ''
                    if args:
                        result_obj = transpile_sql(f"SELECT {args}")
                        transpiled_args = result_obj.transpiled
                        if transpiled_args.upper().startswith('SELECT '):
                            transpiled_args = transpiled_args[7:].strip()
                        result_lines.append(f'{inner_prefix}{func_name}(spark, {transpiled_args})')
                    else:
                        result_lines.append(f'{inner_prefix}{func_name}(spark)')
                    continue

                # SELECT INTO inside IF
                if re.match(r'^Select\s+', body_line, re.IGNORECASE):
                    result_lines.append(f'{inner_prefix}# {body_line}')
                    continue

                # Begin/End
                if re.match(r'^Begin\s*$', body_line, re.IGNORECASE):
                    result_lines.append(f'{inner_prefix}# Nested BEGIN...END block')
                    continue
                if re.match(r'^End\s*;', body_line, re.IGNORECASE):
                    continue

                # Other
                if not re.match(r'^(Begin|End)', body_line, re.IGNORECASE):
                    result_lines.append(f'{inner_prefix}# {body_line}')
        else:
            result_lines.append(f'{inner_prefix}pass')

    if not result_lines:
        return f'{prefix}# IF block — needs manual conversion'

    return '\n'.join(result_lines)


def _gen_if_from_raw_sql(sql: str, indent: int) -> str:
    """Fallback: parse IF block from raw SQL text (matches old code_generator)."""
    prefix = ' ' * indent
    inner_prefix = ' ' * (indent + 4)
    lines = sql.split('\n')
    result_lines = []

    for line in lines:
        stripped = line.strip()

        # IF condition THEN
        if_match = re.match(r'^If\s+(.+?)\s+Then\s*$', stripped, re.IGNORECASE)
        if if_match:
            condition = _translate_condition(if_match.group(1))
            result_lines.append(f'{prefix}if {condition}:')
            continue

        # ELSIF condition THEN
        elsif_match = re.match(r'^Elsif\s+(.+?)\s+Then\s*$', stripped, re.IGNORECASE)
        if elsif_match:
            condition = _translate_condition(elsif_match.group(1))
            result_lines.append(f'{prefix}elif {condition}:')
            continue

        # ELSE
        if re.match(r'^Else\s*$', stripped, re.IGNORECASE):
            result_lines.append(f'{prefix}else:')
            continue

        # END IF
        if re.match(r'^End\s+If\s*;?\s*$', stripped, re.IGNORECASE):
            continue

        # Inner statements
        if stripped and not stripped.startswith('--'):
            proc_match = re.match(r'^([A-Za-z]\w*(?:\.\w+)*)\s*(?:\((.*?)\))?\s*;', stripped, re.IGNORECASE)
            if proc_match:
                func_name = proc_match.group(1)
                args = proc_match.group(2) or ''
                if args:
                    result_obj = transpile_sql(f"SELECT {args}")
                    transpiled_args = result_obj.transpiled
                    if transpiled_args.upper().startswith('SELECT '):
                        transpiled_args = transpiled_args[7:].strip()
                    result_lines.append(f'{inner_prefix}{func_name}(spark, {transpiled_args})')
                else:
                    result_lines.append(f'{inner_prefix}{func_name}(spark)')
                continue

            if re.match(r'^Select\s+', stripped, re.IGNORECASE):
                result_lines.append(f'{prefix}    # {stripped}')
                continue

            if re.match(r'^Begin\s*$', stripped, re.IGNORECASE):
                result_lines.append(f'{inner_prefix}# Nested BEGIN...END block')
                continue
            if re.match(r'^End\s*;', stripped, re.IGNORECASE):
                continue

            if stripped and not re.match(r'^(Begin|End)', stripped, re.IGNORECASE):
                result_lines.append(f'{inner_prefix}# {stripped}')

    if not result_lines:
        return f'{prefix}# IF block — needs manual conversion\n{prefix}# {sql}'

    return '\n'.join(result_lines)


def _gen_for_loop(node: IRNode, indent: int) -> str:
    """Convert FOR cursor loop to PySpark."""
    prefix = ' ' * indent

    if node.cursor_loop_info:
        cli = node.cursor_loop_info
        result_obj = transpile_sql(cli.select_sql)
        transpiled_select = result_obj.transpiled

        lines = [
            f'{prefix}# FOR loop converted from cursor',
            f'{prefix}_cursor_df = spark.sql("""',
            f'{prefix}    {transpiled_select}',
            f'{prefix}""")',
            f'{prefix}for {cli.cursor_variable} in _cursor_df.collect():',
        ]

        # ANTLR path: structured body children
        if node.children:
            has_body = False
            for child in node.children:
                generated = _generate_node(child, indent + 4)
                if generated:
                    # Replace cursor_var.field with cursor_var['field']
                    if cli.field_references:
                        for field in cli.field_references:
                            generated = re.sub(
                                rf'\b{re.escape(cli.cursor_variable)}\.{re.escape(field)}\b',
                                f"{cli.cursor_variable}['{field}']",
                                generated,
                                flags=re.IGNORECASE,
                            )
                    lines.append(generated)
                    has_body = True
            if not has_body:
                lines.append(f'{prefix}    pass')
        # Regex path: raw_text fallback
        elif node.raw_text:
            for body_line in node.raw_text.split(';'):
                body_line = body_line.strip()
                if body_line:
                    lines.append(f'{prefix}    # {body_line}')

        return '\n'.join(lines)

    # Fallback — Point A: LLM fallback for FOR loop
    raw = node.sql_info.raw_sql if node.sql_info else (node.raw_text or '')
    if _llm_bridge is not None and raw:
        llm_result = _llm_bridge.convert_for_loop(raw)
        if llm_result:
            return _indent_code(f'# FOR loop (LLM-assisted)\n{llm_result}', indent)
    return f'{prefix}# TODO: FOR loop needs manual conversion\n{prefix}# {raw}'


def _gen_goto(node: IRNode, indent: int) -> str:
    prefix = ' ' * indent
    label_name = node.label_name or 'unknown'
    return f'{prefix}continue  # GOTO {label_name} — converted to loop continue'


def _gen_label(node: IRNode, indent: int) -> str:
    prefix = ' ' * indent
    label_name = node.label_name or 'unknown'
    return f'{prefix}# Label: {label_name} — see while loop above'


def _gen_begin_end(node: IRNode, indent: int) -> str:
    """Convert nested BEGIN...END blocks."""
    prefix = ' ' * indent
    inner_prefix = ' ' * (indent + 4)

    # ANTLR path: structured children with optional EXCEPTION_HANDLER
    if node.children:
        # Separate body nodes from exception handler
        body_nodes = [c for c in node.children if c.node_type != IRNodeType.EXCEPTION_HANDLER]
        exc_nodes = [c for c in node.children if c.node_type == IRNodeType.EXCEPTION_HANDLER]

        if exc_nodes:
            # Generate try/except
            lines = [f'{prefix}try:']
            has_try_body = False
            for child in body_nodes:
                generated = _generate_node(child, indent + 4)
                if generated:
                    lines.append(generated)
                    has_try_body = True
            if not has_try_body:
                lines.append(f'{inner_prefix}pass')

            # Generate except clauses
            for exc_handler in exc_nodes:
                for when_clause in exc_handler.children:
                    exc_name = when_clause.condition or 'OTHERS'
                    if exc_name.upper() == 'OTHERS':
                        lines.append(f'{prefix}except Exception:')
                    elif exc_name.upper() == 'NO_DATA_FOUND':
                        lines.append(f'{prefix}except Exception:  # NO_DATA_FOUND')
                    else:
                        lines.append(f'{prefix}except Exception:  # {exc_name}')

                    has_exc_body = False
                    for child in when_clause.children:
                        generated = _generate_node(child, indent + 4)
                        if generated:
                            lines.append(generated)
                            has_exc_body = True
                    if not has_exc_body:
                        lines.append(f'{inner_prefix}pass  # Oracle: WHEN {exc_name} THEN NULL')

            return '\n'.join(lines)
        else:
            # No exception handler — just generate body nodes
            lines = []
            for child in body_nodes:
                generated = _generate_node(child, indent)
                if generated:
                    lines.append(generated)
            return '\n'.join(lines) if lines else f'{prefix}pass'

    # Regex path fallback: raw SQL text
    si = node.sql_info
    raw = si.raw_sql if si else ''

    # Check for simple exception swallowing pattern
    if re.search(r'Exception\s+When\s+Others\s+Then\s+Null', raw, re.IGNORECASE):
        inner_match = re.search(r'Begin\s+(.+?)\s+Exception', raw, re.IGNORECASE | re.DOTALL)
        if inner_match:
            inner_sql = inner_match.group(1).strip()
            return (
                f'{prefix}try:\n'
                f'{prefix}    # {inner_sql}\n'
                f'{prefix}    pass  # TODO: convert inner SQL\n'
                f'{prefix}except Exception:\n'
                f'{prefix}    pass  # Oracle: WHEN OTHERS THEN NULL'
            )

    return f'{prefix}# Nested BEGIN...END block\n{prefix}# {raw[:200]}...'


def _gen_dbms_stats(node: IRNode, indent: int) -> str:
    prefix = ' ' * indent
    si = node.sql_info
    raw = si.raw_sql if si else ''

    match = re.search(r"Gather_Table_Stats\s*\(\s*'(\w+)'\s*,\s*'(\w+)'", raw, re.IGNORECASE)
    if match:
        schema = match.group(1).lower()
        table = match.group(2)
        return f'{prefix}spark.sql("ANALYZE TABLE {schema}.{table} COMPUTE STATISTICS")'
    return f'{prefix}# DBMS_STATS — Spark handles statistics automatically'


def _gen_dbms_scheduler(node: IRNode, indent: int) -> str:
    prefix = ' ' * indent
    si = node.sql_info
    raw = si.raw_sql if si else ''

    name_match = re.search(r"Job_Name\s*=>\s*'(\w+)'", raw, re.IGNORECASE)
    action_match = re.search(r"Job_Action\s*=>\s*'(.+?)'", raw, re.IGNORECASE | re.DOTALL)

    job_name = name_match.group(1) if name_match else 'UNKNOWN_JOB'
    job_action = action_match.group(1) if action_match else 'unknown'

    # Generic extraction: find any schema.package.procedure or just procedure names
    # from PL/SQL block strings like 'begin schema.pkg.proc(...); end;'
    proc_calls = re.findall(r'(?:\w+\.)*(\w+)\s*(?:\(|;)', job_action)
    # Filter out PL/SQL noise keywords
    _noise = {'begin', 'end', 'null', 'exception', 'commit', 'rollback', 'declare'}
    proc_calls = [p for p in proc_calls if p.lower() not in _noise]

    if proc_calls:
        call_str = ', '.join(f'"{p}"' for p in proc_calls)
        return (
            f'{prefix}# DBMS_SCHEDULER job: {job_name}\n'
            f'{prefix}# Original action: {job_action.strip()}\n'
            f'{prefix}# Procedures to run: {call_str}\n'
            f'{prefix}executor.submit({proc_calls[0]}, spark)  # Parallel execution'
        )
    return f'{prefix}# TODO: DBMS_SCHEDULER job {job_name} — needs manual conversion\n{prefix}# Action: {job_action}'


def _gen_dbms_session(node: IRNode, indent: int) -> str:
    prefix = ' ' * indent
    si = node.sql_info
    raw = si.raw_sql if si else ''

    match = re.search(r'Sleep\s*\(\s*(\d+)\s*\)', raw, re.IGNORECASE)
    seconds = match.group(1) if match else '10'
    return f'{prefix}time.sleep({seconds})'


def _gen_remote_proc(node: IRNode, indent: int) -> str:
    prefix = ' ' * indent
    si = node.sql_info
    raw = si.raw_sql.strip() if si else (node.raw_text or '')

    # ANTLR path: check if this is a local procedure call
    if si and si.statement_type == 'PROCEDURE_CALL':
        return _gen_local_proc_call(node, indent)

    # Check for remote procedure calls (has @)
    if '@' in raw:
        return (
            f'{prefix}# TODO: Remote procedure call — requires reimplementation or pre-execution\n'
            f'{prefix}# Original: {raw}'
        )

    return _gen_local_proc_call(node, indent)


def _gen_local_proc_call(node: IRNode, indent: int) -> str:
    """Generate code for local procedure calls."""
    prefix = ' ' * indent
    raw = node.raw_text or (node.sql_info.raw_sql if node.sql_info else '')

    # Parse: name or name(args)
    match = re.match(r'^([\w\.]+)\s*(?:\((.*?)\))?\s*$', raw.strip(), re.DOTALL)
    if match:
        func_name = match.group(1)
        args = match.group(2)
        if args and args.strip():
            # Transpile arguments
            result_obj = transpile_sql(f"SELECT {args.strip()}")
            transpiled_args = result_obj.transpiled
            if transpiled_args.upper().startswith('SELECT '):
                transpiled_args = transpiled_args[7:].strip()
            return f'{prefix}{func_name}(spark, {transpiled_args})'
        else:
            return f'{prefix}{func_name}(spark)'

    return f'{prefix}# Procedure call: {raw}'


def _gen_while_loop(node: IRNode, indent: int) -> str:
    """Generate while loop from GOTO-transformed pattern or WHILE statement."""
    prefix = ' ' * indent
    inner_prefix = ' ' * (indent + 4)

    if node.condition and node.condition != "True":
        lines = [f'{prefix}while {node.condition}:  # WHILE LOOP']
    else:
        lines = [f'{prefix}while True:  # Converted from GOTO loop']

    has_body = False
    for child in node.children:
        generated = _generate_node(child, indent + 4)
        if generated:
            lines.append(generated)
            has_body = True

    if not has_body:
        lines.append(f'{inner_prefix}pass')

    return '\n'.join(lines)


def _gen_comment_block(node: IRNode, indent: int) -> str:
    prefix = ' ' * indent
    si = node.sql_info
    raw = si.raw_sql if si else ''

    lines = raw.split('\n')
    result = [f'{prefix}# --- Commented-out Oracle code ---']
    for line in lines[:10]:
        result.append(f'{prefix}# {line.strip()}')
    if len(lines) > 10:
        result.append(f'{prefix}# ... ({len(lines) - 10} more lines)')
    return '\n'.join(result)


def _gen_unknown(node: IRNode, indent: int) -> str:
    prefix = ' ' * indent
    si = node.sql_info
    raw = si.raw_sql.strip() if si else (node.raw_text or 'Unknown statement')

    # Special case: 'break' from GOTO transformation is not unknown
    if raw == 'break':
        return f'{prefix}break'

    # Point A: LLM fallback for unknown statements
    if _llm_bridge is not None and raw and raw != 'Unknown statement':
        llm_result = _llm_bridge.convert_unknown_statement(raw, procedure_context="")
        if llm_result:
            return _indent_code(f'{llm_result}  # LLM-converted', indent)

    return f'{prefix}# TODO: Unclassified statement\n{prefix}# {raw}'


# --- GOTO-to-loop transformation ---

def _transform_goto_to_loops(body: list[IRNode]) -> list[IRNode]:
    """Transform <<label>> ... GOTO label patterns into while True loops.

    Detects patterns like:
        <<label>>
        ... statements ...
        IF condition THEN
            GOTO label
        END IF

    And wraps the region in a WHILE_LOOP with a break condition.
    """
    # Find all labels and their corresponding GOTOs
    label_indices = {}
    goto_indices = {}

    for i, node in enumerate(body):
        if node.node_type == IRNodeType.LABEL and node.label_name:
            label_indices[node.label_name.upper()] = i
        if node.node_type == IRNodeType.GOTO and node.label_name:
            goto_indices[node.label_name.upper()] = i
        # Also check for GOTOs nested inside IF blocks
        if node.node_type == IRNodeType.IF_BLOCK:
            if _contains_goto(node):
                goto_label = _extract_goto_label(node)
                if goto_label:
                    goto_indices[goto_label.upper()] = i

    if not label_indices or not goto_indices:
        return body

    # For each label that has a corresponding GOTO that jumps back to it
    result = list(body)
    for label_name, label_idx in label_indices.items():
        if label_name not in goto_indices:
            continue
        goto_idx = goto_indices[label_name]
        if goto_idx <= label_idx:
            continue  # Forward GOTO, not a loop

        # Extract the region between label and GOTO (inclusive)
        loop_body = result[label_idx + 1:goto_idx + 1]

        # Transform: the IF containing GOTO becomes a break condition
        transformed_body = []
        for node in loop_body:
            if node.node_type == IRNodeType.IF_BLOCK and _contains_goto(node):
                # Convert IF(condition) GOTO to IF NOT(condition) break
                break_node = _make_break_node(node)
                if break_node:
                    transformed_body.append(break_node)
            elif node.node_type == IRNodeType.GOTO:
                # Bare GOTO at end — this means unconditional continue
                continue
            else:
                transformed_body.append(node)

        # Create WHILE_LOOP node
        while_node = IRNode(
            node_type=IRNodeType.WHILE_LOOP,
            condition="True",
            children=transformed_body,
            conversion_path="antlr",
        )

        # Replace the label..goto region with the while loop
        result = result[:label_idx] + [while_node] + result[goto_idx + 1:]

    return result


def _contains_goto(node: IRNode) -> bool:
    """Check if an IF block contains a GOTO."""
    for child in node.children:
        if child.node_type == IRNodeType.GOTO:
            return True
        for grandchild in child.children:
            if grandchild.node_type == IRNodeType.GOTO:
                return True
    return False


def _extract_goto_label(node: IRNode) -> str | None:
    """Extract the GOTO label from an IF block."""
    for child in node.children:
        for grandchild in child.children:
            if grandchild.node_type == IRNodeType.GOTO:
                return grandchild.label_name
        if child.node_type == IRNodeType.GOTO:
            return child.label_name
    return None


def _make_break_node(if_node: IRNode) -> IRNode | None:
    """Convert IF(condition) { ...; GOTO label } to IF NOT(condition) { break }.

    The IF block that contains GOTO becomes a break condition. Other statements
    before the GOTO in the IF body are kept.
    """
    if not if_node.children:
        return None

    first_branch = if_node.children[0]
    condition = first_branch.condition or if_node.condition or ''

    # Collect non-GOTO children from the IF body
    pre_goto_children = []
    for child in first_branch.children:
        if child.node_type != IRNodeType.GOTO:
            pre_goto_children.append(child)

    # Create a new IF block: if NOT condition: break
    # Plus any statements that were before the GOTO
    result = IRNode(
        node_type=IRNodeType.IF_BLOCK,
        condition=condition,
        conversion_path="antlr",
    )

    # The condition means "continue looping", so break when NOT condition
    # Build the result differently: pre-goto children first, then break check
    result_children = []

    # Pre-GOTO statements (like Dbms_Session.Sleep) go into the condition branch
    if pre_goto_children:
        cond_branch = IRNode(
            node_type=IRNodeType.IF_BLOCK,
            condition=condition,
            children=pre_goto_children,
            conversion_path="antlr",
        )
        break_branch = IRNode(
            node_type=IRNodeType.ELSE_BRANCH,
            children=[IRNode(
                node_type=IRNodeType.UNKNOWN,
                raw_text='break',
                conversion_path="antlr",
            )],
            conversion_path="antlr",
        )
        result.children = [cond_branch, break_branch]
    else:
        # Simple case: just break when condition is not met
        break_branch = IRNode(
            node_type=IRNodeType.IF_BLOCK,
            condition=f'not ({condition})',
            children=[IRNode(
                node_type=IRNodeType.UNKNOWN,
                raw_text='break',
                conversion_path="antlr",
            )],
            conversion_path="antlr",
        )
        result.children = [break_branch]

    return result


# --- Utility functions ---

def _translate_condition(oracle_cond: str) -> str:
    """Translate Oracle IF condition to Python."""
    cond = oracle_cond.strip()
    cond = re.sub(r'\b(\w+)\s*<>\s*(\w+)', r'\1 != \2', cond)
    # Convert Oracle = to Python == (but NOT <=, >=, !=, :=)
    cond = re.sub(r'(?<![<>!:])=(?!=)', '==', cond)
    cond = re.sub(r'\bIs\s+Null\b', 'is None', cond, flags=re.IGNORECASE)
    cond = re.sub(r'\bIs\s+Not\s+Null\b', 'is not None', cond, flags=re.IGNORECASE)
    cond = re.sub(r'\bAnd\b', 'and', cond, flags=re.IGNORECASE)
    cond = re.sub(r'\bOr\b', 'or', cond, flags=re.IGNORECASE)
    cond = re.sub(r'\bNot\b', 'not', cond, flags=re.IGNORECASE)
    cond = cond.replace('Lv_', 'lv_').replace('v_', '')
    cond = cond.replace('Ln_', 'ln_')
    return cond


def _oracle_var_to_python(name: str) -> str:
    """Convert Oracle variable name to Python convention."""
    # v_Step_No -> step_no, Lv_Yyyymm -> lv_yyyymm
    # Only strip `v_`/`V_` at the START of the name, so `Lv_Yyyymm` is not
    # butchered into `l_yyyymm` (which breaks bind-var interpolation).
    name = re.sub(r'^[vV]_', '', name)
    # CamelCase -> snake_case WITHOUT inserting a second underscore after an
    # existing one (so `i_Yyyymm` -> `i_yyyymm`, not `i__yyyymm`).
    result = re.sub(
        r'(?<!^)(?<!_)([A-Z])',
        lambda m: '_' + m.group(1).lower(),
        name,
    )
    result = result.lstrip('_').lower()
    return result


def _format_sql_string(sql: str) -> str:
    """Format SQL for embedding in a Python triple-quoted string."""
    sql = sql.strip()
    sql = sql.replace('"""', '\\"\\"\\"')
    return sql


def _indent_code(code: str, spaces: int) -> str:
    """Indent all lines of code."""
    prefix = ' ' * spaces
    return '\n'.join(prefix + line if line.strip() else '' for line in code.split('\n'))
