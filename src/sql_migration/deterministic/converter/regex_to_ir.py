"""
Regex-to-IR Adapter

Converts the existing regex path output (list[SQLStatement]) into the
unified IR format (ProcedureIR with IRNode tree). This ensures the
existing regex path can feed into the new IR-based code generator.
"""

import re
from .ir import (
    IRNode, IRNodeType, ProcedureIR, SQLInfo, CursorLoopInfo, VariableInfo,
)
from .sql_extractor import SQLStatement, StatementType
from .plsql_splitter import ProcedureInfo, extract_variable_declarations


# Map StatementType to IRNodeType
_STMT_TYPE_TO_IR = {
    StatementType.INSERT_SELECT: IRNodeType.SQL_STATEMENT,
    StatementType.INSERT_VALUES: IRNodeType.SQL_STATEMENT,
    StatementType.UPDATE_CORRELATED: IRNodeType.SQL_STATEMENT,
    StatementType.UPDATE_SIMPLE: IRNodeType.SQL_STATEMENT,
    StatementType.DELETE: IRNodeType.SQL_STATEMENT,
    StatementType.TRUNCATE: IRNodeType.SQL_STATEMENT,
    StatementType.SELECT_INTO: IRNodeType.SQL_STATEMENT,
    StatementType.EXECUTE_IMMEDIATE_DDL: IRNodeType.EXECUTE_IMMEDIATE,
    StatementType.DBMS_STATS: IRNodeType.DBMS_STATS,
    StatementType.DBMS_SCHEDULER: IRNodeType.DBMS_SCHEDULER,
    StatementType.DBMS_SESSION: IRNodeType.DBMS_SESSION,
    StatementType.REMOTE_PROC_CALL: IRNodeType.PROCEDURE_CALL,
    StatementType.COMMIT: IRNodeType.COMMIT,
    StatementType.ROLLBACK: IRNodeType.ROLLBACK,
    StatementType.STATUS_INSERT: IRNodeType.STATUS_TRACKING,
    StatementType.STATUS_UPDATE: IRNodeType.STATUS_TRACKING,
    StatementType.VARIABLE_ASSIGNMENT: IRNodeType.VARIABLE_ASSIGNMENT,
    StatementType.IF_BLOCK: IRNodeType.IF_BLOCK,
    StatementType.FOR_LOOP: IRNodeType.FOR_CURSOR_LOOP,
    StatementType.GOTO: IRNodeType.GOTO,
    StatementType.LABEL: IRNodeType.LABEL,
    StatementType.COMMENT_BLOCK: IRNodeType.COMMENT_BLOCK,
    StatementType.BEGIN_END_BLOCK: IRNodeType.BEGIN_END_BLOCK,
    StatementType.UNKNOWN: IRNodeType.UNKNOWN,
}


def statements_to_ir(
    procedure: ProcedureInfo,
    statements: list[SQLStatement],
    status_name: str = "",
) -> ProcedureIR:
    """Convert a procedure's extracted SQLStatements into ProcedureIR.

    Args:
        procedure: The ProcedureInfo from the splitter
        statements: Classified SQL statements from the extractor
        status_name: Status tracking name for this procedure

    Returns:
        ProcedureIR with body nodes adapted from SQLStatements
    """
    # Extract variable declarations
    var_decls = extract_variable_declarations(procedure)
    variables = [
        VariableInfo(name=name, var_type=vtype)
        for name, vtype in var_decls
    ]

    # Determine if procedure has step tracking
    has_step_tracking = any(
        s.type == StatementType.VARIABLE_ASSIGNMENT and 'step_no' in s.sql.lower()
        for s in statements
    )

    # Determine status name from statements if not provided
    if not status_name:
        status_name = _extract_status_name(statements) or procedure.name.upper()

    # Convert each statement to an IR node
    body_nodes = []
    for stmt in statements:
        node = _statement_to_node(stmt)
        if node:
            body_nodes.append(node)

    return ProcedureIR(
        name=procedure.name,
        parameters=procedure.parameters,
        variables=variables,
        body=body_nodes,
        is_status_tracked=True,  # All procedures use @track_status
        status_name=status_name,
        has_step_tracking=has_step_tracking,
        conversion_path="regex",
    )


def _statement_to_node(stmt: SQLStatement) -> IRNode:
    """Convert a single SQLStatement to an IRNode."""
    ir_type = _STMT_TYPE_TO_IR.get(stmt.type, IRNodeType.UNKNOWN)

    # Build SQLInfo
    sql_info = SQLInfo(
        raw_sql=stmt.sql,
        statement_type=stmt.type.value,
        target_table=stmt.target_table,
        source_tables=stmt.source_tables or [],
        oracle_constructs=stmt.oracle_constructs,
        is_correlated_update=(stmt.type == StatementType.UPDATE_CORRELATED),
        has_db_link=stmt.has_db_link,
        has_hints=stmt.has_hints,
        is_commented=stmt.is_commented,
    )

    node = IRNode(
        node_type=ir_type,
        sql_info=sql_info,
        source_line_start=stmt.line_start,
        source_line_end=stmt.line_end,
        conversion_path="regex",
    )

    # Enrich specific node types
    if ir_type == IRNodeType.VARIABLE_ASSIGNMENT:
        _enrich_variable_assignment(node, stmt)
    elif ir_type == IRNodeType.IF_BLOCK:
        _enrich_if_block(node, stmt)
    elif ir_type == IRNodeType.FOR_CURSOR_LOOP:
        _enrich_for_loop(node, stmt)
    elif ir_type == IRNodeType.GOTO:
        _enrich_goto(node, stmt)
    elif ir_type == IRNodeType.LABEL:
        _enrich_label(node, stmt)

    return node


def _enrich_variable_assignment(node: IRNode, stmt: SQLStatement):
    """Extract variable name and value from assignment statement."""
    sql = stmt.sql.strip().rstrip(';')
    match = re.match(r'(\w+)\s*:=\s*(.+)', sql, re.DOTALL)
    if match:
        node.variable_name = match.group(1)
        node.variable_value = match.group(2).strip()


def _enrich_if_block(node: IRNode, stmt: SQLStatement):
    """Parse IF block into child branches.

    The regex path captures the entire IF/ELSIF/ELSE/END IF block as a
    single SQLStatement. Here we decompose it into child IRNodes for
    each branch. Inner statements remain as raw_text in children since
    the regex path doesn't deeply parse them.
    """
    lines = stmt.sql.split('\n')
    current_branch = None
    current_body_lines = []

    for line in lines:
        stripped = line.strip()

        # IF condition THEN
        if_match = re.match(r'^If\s+(.+?)\s+Then\s*$', stripped, re.IGNORECASE)
        if if_match:
            # Save previous branch
            if current_branch:
                current_branch.raw_text = '\n'.join(current_body_lines)
                node.children.append(current_branch)

            current_branch = IRNode(
                node_type=IRNodeType.IF_BLOCK,
                condition=if_match.group(1),
                conversion_path="regex",
            )
            current_body_lines = []
            continue

        # ELSIF condition THEN
        elsif_match = re.match(r'^Elsif\s+(.+?)\s+Then\s*$', stripped, re.IGNORECASE)
        if elsif_match:
            if current_branch:
                current_branch.raw_text = '\n'.join(current_body_lines)
                node.children.append(current_branch)

            current_branch = IRNode(
                node_type=IRNodeType.ELSIF_BRANCH,
                condition=elsif_match.group(1),
                conversion_path="regex",
            )
            current_body_lines = []
            continue

        # ELSE
        if re.match(r'^Else\s*$', stripped, re.IGNORECASE):
            if current_branch:
                current_branch.raw_text = '\n'.join(current_body_lines)
                node.children.append(current_branch)

            current_branch = IRNode(
                node_type=IRNodeType.ELSE_BRANCH,
                conversion_path="regex",
            )
            current_body_lines = []
            continue

        # END IF
        if re.match(r'^End\s+If\s*;?\s*$', stripped, re.IGNORECASE):
            continue

        # Body line
        if stripped:
            current_body_lines.append(stripped)

    # Save last branch
    if current_branch:
        current_branch.raw_text = '\n'.join(current_body_lines)
        node.children.append(current_branch)

    # Store the original condition on the IF_BLOCK node itself
    if node.children and node.children[0].condition:
        node.condition = node.children[0].condition


def _enrich_for_loop(node: IRNode, stmt: SQLStatement):
    """Extract cursor loop metadata."""
    match = re.search(
        r'For\s+(\w+)\s+In\s+\((.+?)\)\s*Loop\s*(.+?)\s*End\s+Loop',
        stmt.sql, re.IGNORECASE | re.DOTALL
    )
    if match:
        cursor_var = match.group(1)
        select_sql = match.group(2)
        loop_body = match.group(3)

        # Find field references: cursor_var.field_name
        field_refs = re.findall(
            rf'\b{re.escape(cursor_var)}\.(\w+)',
            loop_body, re.IGNORECASE
        )

        node.cursor_loop_info = CursorLoopInfo(
            cursor_variable=cursor_var,
            select_sql=select_sql,
            field_references=list(set(field_refs)),
        )
        node.raw_text = loop_body.strip()


def _enrich_goto(node: IRNode, stmt: SQLStatement):
    """Extract GOTO label name."""
    match = re.search(r'Goto\s+(\w+)', stmt.sql, re.IGNORECASE)
    if match:
        node.label_name = match.group(1)


def _enrich_label(node: IRNode, stmt: SQLStatement):
    """Extract label name."""
    match = re.search(r'<<(\w+)>>', stmt.sql)
    if match:
        node.label_name = match.group(1)


def _extract_status_name(statements: list[SQLStatement]) -> str | None:
    """Extract the status tracking name from STATUS_INSERT statements."""
    for stmt in statements:
        if stmt.type == StatementType.STATUS_INSERT:
            match = re.search(r"'(\w+)'", stmt.sql)
            if match:
                return match.group(1)
    return None
