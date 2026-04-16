"""
Parse Tree → IR Visitor

Converts the parse tree from plsql_parser.py into ProcedureIR nodes.
Handles SQL classification, cursor loop field references, and structured
control flow conversion.
"""

import re
from .ir import (
    IRNode, IRNodeType, ProcedureIR, SQLInfo, CursorLoopInfo, VariableInfo,
)
from .plsql_parser import ParseNode, NodeType
from .sql_extractor import (
    _extract_insert_target, _extract_source_tables,
    _detect_constructs, _is_status_tracking,
)


def parse_tree_to_ir(tree: ParseNode, status_name: str = "") -> ProcedureIR:
    """Convert a parse tree to ProcedureIR.

    Args:
        tree: Parse tree from plsql_parser.parse_procedure()
        status_name: Status tracking name for this procedure

    Returns:
        ProcedureIR with structured body nodes
    """
    assert tree.type == NodeType.PROCEDURE

    # Extract variable declarations
    variables = []
    body_node = None
    for child in tree.children:
        if child.type == NodeType.VAR_DECL:
            variables.append(VariableInfo(
                name=child.name,
                var_type=child.var_type,
                default_value=child.var_default or None,
            ))
        elif child.type == NodeType.BEGIN_BLOCK:
            body_node = child

    if not body_node:
        return ProcedureIR(
            name=tree.name,
            parameters=tree.params,
            variables=variables,
            body=[],
            status_name=status_name or tree.name.upper(),
            conversion_path="antlr",
        )

    # Convert body statements
    body_nodes = []
    exception_handler = None
    for child in body_node.children:
        if child.type == NodeType.EXCEPTION_BLOCK:
            exception_handler = _convert_exception_block(child)
        else:
            node = _convert_node(child)
            if node:
                body_nodes.append(node)

    # Detect step tracking
    has_step_tracking = any(
        n.node_type == IRNodeType.VARIABLE_ASSIGNMENT
        and n.variable_name and 'step_no' in n.variable_name.lower()
        for n in body_nodes
    )

    # Auto-detect status name from status tracking inserts
    if not status_name:
        for n in body_nodes:
            if (n.node_type == IRNodeType.STATUS_TRACKING
                    and n.sql_info and n.sql_info.statement_type == 'STATUS_INSERT'):
                m = re.search(r"'(\w+)'", n.sql_info.raw_sql)
                if m:
                    status_name = m.group(1)
                    break
        if not status_name:
            status_name = tree.name.upper()

    return ProcedureIR(
        name=tree.name,
        parameters=tree.params,
        variables=variables,
        body=body_nodes,
        exception_handler=exception_handler,
        is_status_tracked=True,
        status_name=status_name,
        has_step_tracking=has_step_tracking,
        conversion_path="antlr",
    )


def _convert_node(node: ParseNode) -> IRNode | None:
    """Convert a single parse tree node to an IR node."""
    converters = {
        NodeType.COMMIT_STMT: _convert_commit,
        NodeType.ROLLBACK_STMT: _convert_rollback,
        NodeType.NULL_STMT: _convert_null,
        NodeType.LABEL: _convert_label,
        NodeType.GOTO_STMT: _convert_goto,
        NodeType.IF_STMT: _convert_if,
        NodeType.FOR_LOOP: _convert_for_loop,
        NodeType.WHILE_LOOP: _convert_while_loop,
        NodeType.BEGIN_BLOCK: _convert_begin_end,
        NodeType.EXECUTE_IMMEDIATE: _convert_execute_immediate,
        NodeType.SQL_STATEMENT: _convert_sql,
        NodeType.VARIABLE_ASSIGNMENT: _convert_assignment,
        NodeType.PROCEDURE_CALL: _convert_proc_call,
        NodeType.DBMS_CALL: _convert_dbms_call,
    }

    converter = converters.get(node.type)
    if converter:
        return converter(node)

    # Unknown
    return IRNode(
        node_type=IRNodeType.UNKNOWN,
        raw_text=node.text,
        source_line_start=node.line,
        conversion_path="antlr",
    )


def _convert_commit(node: ParseNode) -> IRNode:
    return IRNode(
        node_type=IRNodeType.COMMIT,
        source_line_start=node.line,
        conversion_path="antlr",
    )


def _convert_rollback(node: ParseNode) -> IRNode:
    return IRNode(
        node_type=IRNodeType.ROLLBACK,
        source_line_start=node.line,
        conversion_path="antlr",
    )


def _convert_null(node: ParseNode) -> IRNode | None:
    return None  # NULL statements are no-ops


def _convert_label(node: ParseNode) -> IRNode:
    return IRNode(
        node_type=IRNodeType.LABEL,
        label_name=node.name,
        source_line_start=node.line,
        conversion_path="antlr",
    )


def _convert_goto(node: ParseNode) -> IRNode:
    return IRNode(
        node_type=IRNodeType.GOTO,
        label_name=node.name,
        source_line_start=node.line,
        conversion_path="antlr",
    )


def _convert_if(node: ParseNode) -> IRNode:
    """Convert IF/ELSIF/ELSE structure to IR."""
    ir_node = IRNode(
        node_type=IRNodeType.IF_BLOCK,
        condition=node.condition,
        source_line_start=node.line,
        conversion_path="antlr",
    )

    for child in node.children:
        if child.type == NodeType.IF_STMT:
            # The IF branch
            branch = IRNode(
                node_type=IRNodeType.IF_BLOCK,
                condition=child.condition,
                children=[_convert_node(c) for c in child.children if _convert_node(c)],
                conversion_path="antlr",
            )
            ir_node.children.append(branch)
        elif child.type == NodeType.ELSIF_CLAUSE:
            branch = IRNode(
                node_type=IRNodeType.ELSIF_BRANCH,
                condition=child.condition,
                children=[_convert_node(c) for c in child.children if _convert_node(c)],
                conversion_path="antlr",
            )
            ir_node.children.append(branch)
        elif child.type == NodeType.ELSE_CLAUSE:
            branch = IRNode(
                node_type=IRNodeType.ELSE_BRANCH,
                children=[_convert_node(c) for c in child.children if _convert_node(c)],
                conversion_path="antlr",
            )
            ir_node.children.append(branch)

    return ir_node


def _convert_for_loop(node: ParseNode) -> IRNode:
    """Convert FOR cursor loop to IR with structured body."""
    # Convert body children
    body_children = []
    for child in node.children:
        ir_child = _convert_node(child)
        if ir_child:
            body_children.append(ir_child)

    # Detect cursor field references in body
    cursor_var = node.cursor_var
    field_refs = set()
    _collect_field_refs(body_children, cursor_var, field_refs)

    cursor_info = CursorLoopInfo(
        cursor_variable=cursor_var,
        select_sql=node.cursor_sql,
        field_references=sorted(field_refs),
    )

    return IRNode(
        node_type=IRNodeType.FOR_CURSOR_LOOP,
        cursor_loop_info=cursor_info,
        children=body_children,
        source_line_start=node.line,
        conversion_path="antlr",
    )


def _convert_while_loop(node: ParseNode) -> IRNode:
    """Convert WHILE loop to IR."""
    body_children = []
    for child in node.children:
        ir_child = _convert_node(child)
        if ir_child:
            body_children.append(ir_child)

    return IRNode(
        node_type=IRNodeType.WHILE_LOOP,
        condition=node.condition or "True",
        children=body_children,
        source_line_start=node.line,
        conversion_path="antlr",
    )


def _collect_field_refs(nodes: list[IRNode], cursor_var: str, refs: set):
    """Recursively collect cursor_var.field references from IR nodes."""
    pattern = re.compile(rf'\b{re.escape(cursor_var)}\.(\w+)', re.IGNORECASE)
    for node in nodes:
        # Check raw_text and sql_info.raw_sql
        for text in [node.raw_text, node.sql_info.raw_sql if node.sql_info else None]:
            if text:
                for m in pattern.finditer(text):
                    refs.add(m.group(1))
        # Check condition
        if node.condition:
            for m in pattern.finditer(node.condition):
                refs.add(m.group(1))
        # Recurse into children
        if node.children:
            _collect_field_refs(node.children, cursor_var, refs)


def _convert_begin_end(node: ParseNode) -> IRNode:
    """Convert nested BEGIN...END block."""
    ir_node = IRNode(
        node_type=IRNodeType.BEGIN_END_BLOCK,
        source_line_start=node.line,
        conversion_path="antlr",
    )

    for child in node.children:
        if child.type == NodeType.EXCEPTION_BLOCK:
            exc_handler = _convert_exception_block(child)
            ir_node.children.append(exc_handler)
        else:
            ir_child = _convert_node(child)
            if ir_child:
                ir_node.children.append(ir_child)

    return ir_node


def _convert_exception_block(node: ParseNode) -> IRNode:
    """Convert EXCEPTION block with WHEN clauses."""
    ir_node = IRNode(
        node_type=IRNodeType.EXCEPTION_HANDLER,
        source_line_start=node.line,
        conversion_path="antlr",
    )

    for child in node.children:
        if child.type == NodeType.WHEN_CLAUSE:
            when_ir = IRNode(
                node_type=IRNodeType.WHEN_CLAUSE,
                condition=child.exception_name,
                children=[_convert_node(c) for c in child.children if _convert_node(c)],
                source_line_start=child.line,
                conversion_path="antlr",
            )
            ir_node.children.append(when_ir)

    return ir_node


def _convert_execute_immediate(node: ParseNode) -> IRNode:
    """Convert EXECUTE IMMEDIATE statement."""
    raw_sql = f"Execute Immediate {node.text}"

    # Determine if it's a truncate
    if 'truncate' in node.text.lower():
        target = re.search(r"truncate\s+table\s+([\w\.]+)", node.text, re.IGNORECASE)
        return IRNode(
            node_type=IRNodeType.SQL_STATEMENT,
            sql_info=SQLInfo(
                raw_sql=raw_sql,
                statement_type='TRUNCATE',
                target_table=target.group(1) if target else None,
            ),
            source_line_start=node.line,
            conversion_path="antlr",
        )

    return IRNode(
        node_type=IRNodeType.EXECUTE_IMMEDIATE,
        sql_info=SQLInfo(
            raw_sql=raw_sql,
            statement_type='EXECUTE_IMMEDIATE_DDL',
        ),
        source_line_start=node.line,
        conversion_path="antlr",
    )


def _convert_sql(node: ParseNode) -> IRNode:
    """Convert an opaque SQL statement to IR with classification."""
    raw_sql = node.text
    keyword = node.sql_keyword.upper()

    # Classify the statement
    if keyword == 'INSERT':
        return _classify_insert(raw_sql, node.line)
    elif keyword == 'UPDATE':
        return _classify_update(raw_sql, node.line)
    elif keyword == 'DELETE':
        return _classify_delete(raw_sql, node.line)
    elif keyword == 'SELECT':
        return _classify_select(raw_sql, node.line)
    elif keyword == 'TRUNCATE':
        target = re.search(r"truncate\s+table\s+([\w\.]+)", raw_sql, re.IGNORECASE)
        return IRNode(
            node_type=IRNodeType.SQL_STATEMENT,
            sql_info=SQLInfo(
                raw_sql=raw_sql,
                statement_type='TRUNCATE',
                target_table=target.group(1) if target else None,
            ),
            source_line_start=node.line,
            conversion_path="antlr",
        )

    elif keyword == 'MERGE':
        target_match = re.search(r'MERGE\s+INTO\s+([\w\.]+)', raw_sql, re.IGNORECASE)
        target = target_match.group(1) if target_match else None
        return IRNode(
            node_type=IRNodeType.SQL_STATEMENT,
            sql_info=SQLInfo(
                raw_sql=raw_sql,
                statement_type='MERGE',
                target_table=target,
            ),
            source_line_start=node.line,
            conversion_path="antlr",
        )

    return IRNode(
        node_type=IRNodeType.UNKNOWN,
        raw_text=raw_sql,
        source_line_start=node.line,
        conversion_path="antlr",
    )


def _classify_insert(raw_sql: str, line: int) -> IRNode:
    """Classify INSERT statement."""
    target = _extract_insert_target(raw_sql)
    is_status = _is_status_tracking(raw_sql)

    if is_status:
        return IRNode(
            node_type=IRNodeType.STATUS_TRACKING,
            sql_info=SQLInfo(
                raw_sql=raw_sql,
                statement_type='STATUS_INSERT',
                target_table=target,
            ),
            source_line_start=line,
            conversion_path="antlr",
        )

    if 'values' in raw_sql.lower() and 'select' not in raw_sql.lower():
        stype = 'INSERT_VALUES'
    else:
        stype = 'INSERT_SELECT'

    return IRNode(
        node_type=IRNodeType.SQL_STATEMENT,
        sql_info=SQLInfo(
            raw_sql=raw_sql,
            statement_type=stype,
            target_table=target,
            source_tables=_extract_source_tables(raw_sql),
            oracle_constructs=_detect_constructs(raw_sql),
            has_db_link=bool(re.search(r'@\w+', raw_sql)),
            has_hints=bool(re.search(r'/\*\+', raw_sql)),
        ),
        source_line_start=line,
        conversion_path="antlr",
    )


def _classify_update(raw_sql: str, line: int) -> IRNode:
    """Classify UPDATE statement."""
    target_match = re.search(r'Update\s+(?:/\*.*?\*/)?\s*([\w\.]+)', raw_sql, re.IGNORECASE)
    target = target_match.group(1) if target_match else None
    is_status = _is_status_tracking(raw_sql)

    if is_status:
        return IRNode(
            node_type=IRNodeType.STATUS_TRACKING,
            sql_info=SQLInfo(
                raw_sql=raw_sql,
                statement_type='STATUS_UPDATE',
                target_table=target,
            ),
            source_line_start=line,
            conversion_path="antlr",
        )

    has_subquery = bool(re.search(r'\(\s*Select\s+', raw_sql, re.IGNORECASE))
    stype = 'UPDATE_CORRELATED' if has_subquery else 'UPDATE_SIMPLE'

    return IRNode(
        node_type=IRNodeType.SQL_STATEMENT,
        sql_info=SQLInfo(
            raw_sql=raw_sql,
            statement_type=stype,
            target_table=target,
            source_tables=_extract_source_tables(raw_sql),
            oracle_constructs=_detect_constructs(raw_sql),
            is_correlated_update=has_subquery,
            has_db_link=bool(re.search(r'@\w+', raw_sql)),
            has_hints=bool(re.search(r'/\*\+', raw_sql)),
        ),
        source_line_start=line,
        conversion_path="antlr",
    )


def _classify_delete(raw_sql: str, line: int) -> IRNode:
    """Classify DELETE statement."""
    target_match = re.search(r'Delete\s+From\s+([\w\.]+)', raw_sql, re.IGNORECASE)
    target = target_match.group(1) if target_match else None

    return IRNode(
        node_type=IRNodeType.SQL_STATEMENT,
        sql_info=SQLInfo(
            raw_sql=raw_sql,
            statement_type='DELETE',
            target_table=target,
            source_tables=_extract_source_tables(raw_sql),
            oracle_constructs=_detect_constructs(raw_sql),
        ),
        source_line_start=line,
        conversion_path="antlr",
    )


def _classify_select(raw_sql: str, line: int) -> IRNode:
    """Classify SELECT INTO statement."""
    return IRNode(
        node_type=IRNodeType.SQL_STATEMENT,
        sql_info=SQLInfo(
            raw_sql=raw_sql,
            statement_type='SELECT_INTO',
            source_tables=_extract_source_tables(raw_sql),
            oracle_constructs=_detect_constructs(raw_sql),
        ),
        source_line_start=line,
        conversion_path="antlr",
    )


def _convert_assignment(node: ParseNode) -> IRNode:
    """Convert variable assignment."""
    raw_sql = f"{node.name} := {node.text}"

    # Check for step tracking
    if 'step_no' in node.name.lower():
        return IRNode(
            node_type=IRNodeType.VARIABLE_ASSIGNMENT,
            variable_name=node.name,
            variable_value=node.text,
            sql_info=SQLInfo(raw_sql=raw_sql, statement_type='VARIABLE_ASSIGNMENT'),
            source_line_start=node.line,
            conversion_path="antlr",
        )

    return IRNode(
        node_type=IRNodeType.VARIABLE_ASSIGNMENT,
        variable_name=node.name,
        variable_value=node.text,
        sql_info=SQLInfo(raw_sql=raw_sql, statement_type='VARIABLE_ASSIGNMENT'),
        source_line_start=node.line,
        conversion_path="antlr",
    )


def _convert_proc_call(node: ParseNode) -> IRNode:
    """Convert procedure call."""
    name = node.name
    text = node.text

    # Check for remote proc (has @)
    if '@' in name:
        return IRNode(
            node_type=IRNodeType.PROCEDURE_CALL,
            raw_text=text,
            sql_info=SQLInfo(
                raw_sql=text,
                statement_type='REMOTE_PROC_CALL',
            ),
            source_line_start=node.line,
            conversion_path="antlr",
        )

    return IRNode(
        node_type=IRNodeType.PROCEDURE_CALL,
        raw_text=text,
        sql_info=SQLInfo(
            raw_sql=text,
            statement_type='PROCEDURE_CALL',
        ),
        source_line_start=node.line,
        conversion_path="antlr",
    )


def _convert_dbms_call(node: ParseNode) -> IRNode:
    """Convert DBMS_* procedure call."""
    upper_name = node.name.upper()
    text = node.text

    if 'DBMS_STATS' in upper_name:
        return IRNode(
            node_type=IRNodeType.DBMS_STATS,
            sql_info=SQLInfo(raw_sql=text, statement_type='DBMS_STATS'),
            source_line_start=node.line,
            conversion_path="antlr",
        )
    elif 'DBMS_SESSION' in upper_name:
        return IRNode(
            node_type=IRNodeType.DBMS_SESSION,
            sql_info=SQLInfo(raw_sql=text, statement_type='DBMS_SESSION'),
            source_line_start=node.line,
            conversion_path="antlr",
        )
    elif 'DBMS_SCHEDULER' in upper_name:
        return IRNode(
            node_type=IRNodeType.DBMS_SCHEDULER,
            sql_info=SQLInfo(raw_sql=text, statement_type='DBMS_SCHEDULER'),
            source_line_start=node.line,
            conversion_path="antlr",
        )

    return IRNode(
        node_type=IRNodeType.UNKNOWN,
        raw_text=text,
        source_line_start=node.line,
        conversion_path="antlr",
    )
