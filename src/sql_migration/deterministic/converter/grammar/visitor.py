"""
ANTLR Parse Tree → ParseNode Visitor

Converts the ANTLR4 parse tree into the same ParseNode tree structure
that the hand-rolled parser (plsql_parser.py) produces. This means
antlr_to_ir.py stays completely unchanged.
"""

from antlr4 import CommonTokenStream, InputStream

from .generated.PLSQLProcedureLexer import PLSQLProcedureLexer
from .generated.PLSQLProcedureParser import PLSQLProcedureParser
from .generated.PLSQLProcedureVisitor import PLSQLProcedureVisitor
from ..plsql_parser import ParseNode, NodeType


def _get_text(ctx, input_stream):
    """Extract original source text for a parse tree context."""
    if ctx is None:
        return ""
    start = ctx.start
    stop = ctx.stop
    if start is None or stop is None:
        return ""
    return input_stream.getText(start.start, stop.stop)


def _get_identifier_text(ctx):
    """Get text from an identifier context."""
    if ctx is None:
        return ""
    return ctx.getText()


def _get_dblink_name(ctx):
    """Safely get the dblink identifier text from a labeled alternative.

    In labeled alternatives (#callNoArgs, #callWithArgs, #assignStmt),
    ctx.identifier() returns a single context (not a list) when there's
    only one identifier reference in that alternative.
    """
    id_ctx = ctx.identifier()
    if id_ctx:
        if isinstance(id_ctx, list):
            return _get_identifier_text(id_ctx[0])
        else:
            return _get_identifier_text(id_ctx)
    return ""


class ParseNodeVisitor(PLSQLProcedureVisitor):
    """Visitor that produces ParseNode trees matching the hand-rolled parser output."""

    def __init__(self, input_stream):
        self.input_stream = input_stream

    def visitProcedureDef(self, ctx):
        node = ParseNode(type=NodeType.PROCEDURE)
        # Name
        identifiers = ctx.identifier()
        node.name = _get_identifier_text(identifiers[0]) if identifiers else ""

        # Parameters
        param_list = ctx.parameterList()
        if param_list:
            node.params = self._extract_params(param_list)

        # Variable declarations
        decl_section = ctx.declarationSection()
        if decl_section:
            for var_decl_ctx in decl_section.varDecl():
                var_node = self.visitVarDecl(var_decl_ctx)
                if var_node:
                    node.children.append(var_node)

        # BEGIN block
        begin_ctx = ctx.beginBlock()
        if begin_ctx:
            begin_node = self.visitBeginBlock(begin_ctx)
            node.children.append(begin_node)

        return node

    def _extract_params(self, ctx):
        """Extract parameter names from parameterList."""
        params = []
        for param_ctx in ctx.parameter():
            id_ctx = param_ctx.identifier()
            if id_ctx:
                params.append(_get_identifier_text(id_ctx))
        return params

    def visitVarDecl(self, ctx):
        id_ctx = ctx.identifier()
        if id_ctx is None:
            return None
        name = _get_identifier_text(id_ctx)

        # Type from typeRef
        type_ref = ctx.typeRef()
        var_type = _get_text(type_ref, self.input_stream).strip() if type_ref else ""

        # Type modifier like (4000)
        type_mod = ctx.typeMod()
        if type_mod:
            mod_text = _get_text(type_mod, self.input_stream)
            var_type += mod_text

        # Default value
        var_default = ""
        sql_rest = ctx.sqlRest()
        if sql_rest:
            var_default = _get_text(sql_rest, self.input_stream).strip()

        return ParseNode(
            type=NodeType.VAR_DECL,
            name=name,
            var_type=var_type,
            var_default=var_default,
        )

    def visitBeginBlock(self, ctx):
        node = ParseNode(type=NodeType.BEGIN_BLOCK)

        # Body statements
        body_ctx = ctx.bodyStatements()
        if body_ctx:
            stmts = self._visit_body_statements(body_ctx)
            node.children.extend(stmts)

        # Exception block
        exc_ctx = ctx.exceptionBlock()
        if exc_ctx:
            exc_node = self.visitExceptionBlock(exc_ctx)
            node.children.append(exc_node)

        return node

    def _visit_body_statements(self, ctx):
        """Visit all statements in a bodyStatements context."""
        stmts = []
        for stmt_ctx in ctx.statement():
            result = self.visitStatement(stmt_ctx)
            if result is not None:
                stmts.append(result)
        return stmts

    def visitStatement(self, ctx):
        """Dispatch to the appropriate statement visitor."""
        # The statement rule is an alternation; get the child rule context
        child = ctx.getChild(0)
        if child is None:
            return None
        return child.accept(self)

    def visitLabelStmt(self, ctx):
        name = _get_identifier_text(ctx.identifier())
        return ParseNode(type=NodeType.LABEL, name=name)

    def visitCommitStmt(self, ctx):
        return ParseNode(type=NodeType.COMMIT_STMT)

    def visitRollbackStmt(self, ctx):
        return ParseNode(type=NodeType.ROLLBACK_STMT)

    def visitNullStmt(self, ctx):
        return ParseNode(type=NodeType.NULL_STMT)

    def visitGotoStmt(self, ctx):
        name = _get_identifier_text(ctx.identifier())
        return ParseNode(type=NodeType.GOTO_STMT, name=name)

    def visitIfStmt(self, ctx):
        node = ParseNode(type=NodeType.IF_STMT)

        # Main IF condition
        condition_text = _get_text(ctx.condition(), self.input_stream).strip()
        node.condition = condition_text

        # IF branch body
        body_ctx = ctx.bodyStatements()
        if body_ctx:
            if_body = self._visit_body_statements(body_ctx)
            if_child = ParseNode(
                type=NodeType.IF_STMT,
                condition=condition_text,
                children=if_body,
            )
            node.children.append(if_child)

        # ELSIF branches
        for elsif_ctx in ctx.elsifClause():
            elsif_node = self.visitElsifClause(elsif_ctx)
            node.children.append(elsif_node)

        # ELSE branch
        else_ctx = ctx.elseClause()
        if else_ctx:
            else_node = self.visitElseClause(else_ctx)
            node.children.append(else_node)

        return node

    def visitElsifClause(self, ctx):
        condition_text = _get_text(ctx.condition(), self.input_stream).strip()
        body = self._visit_body_statements(ctx.bodyStatements())
        return ParseNode(
            type=NodeType.ELSIF_CLAUSE,
            condition=condition_text,
            children=body,
        )

    def visitElseClause(self, ctx):
        body = self._visit_body_statements(ctx.bodyStatements())
        return ParseNode(
            type=NodeType.ELSE_CLAUSE,
            children=body,
        )

    def visitForLoopStmt(self, ctx):
        cursor_var = _get_identifier_text(ctx.identifier())

        # Check if this is a cursor loop (has cursorSql) or a range loop
        cursor_sql_ctx = ctx.cursorSql()
        if cursor_sql_ctx:
            cursor_sql = _get_text(cursor_sql_ctx, self.input_stream).strip()
        else:
            # Range loop: FOR i IN [REVERSE] expr..expr LOOP
            range_text = _get_text(ctx.rangeExpr(), self.input_stream).strip()
            reverse_token = ctx.REVERSE()
            if reverse_token:
                cursor_sql = "REVERSE " + range_text
            else:
                cursor_sql = range_text

        body = self._visit_body_statements(ctx.bodyStatements())

        return ParseNode(
            type=NodeType.FOR_LOOP,
            cursor_var=cursor_var,
            cursor_sql=cursor_sql,
            children=body,
        )

    def visitWhileLoopStmt(self, ctx):
        condition_text = _get_text(ctx.whileCondition(), self.input_stream).strip()
        body = self._visit_body_statements(ctx.bodyStatements())
        return ParseNode(
            type=NodeType.WHILE_LOOP,
            condition=condition_text,
            children=body,
        )

    def visitCaseStmt(self, ctx):
        """Map CASE statement to IF/ELSIF/ELSE structure."""
        node = ParseNode(type=NodeType.IF_STMT)

        # Get case selector if present (simple CASE)
        selector_text = ""
        selector_ctx = ctx.caseSelector()
        if selector_ctx:
            selector_text = _get_text(selector_ctx, self.input_stream).strip()

        branches = ctx.caseWhenBranch()
        for i, branch_ctx in enumerate(branches):
            cond_text = _get_text(branch_ctx.condition(), self.input_stream).strip()
            # Build full condition: "selector = value" for simple CASE
            if selector_text:
                full_cond = f"{selector_text} = {cond_text}"
            else:
                full_cond = cond_text

            body = self._visit_body_statements(branch_ctx.bodyStatements())

            if i == 0:
                # First WHEN → IF branch
                node.condition = full_cond
                if_child = ParseNode(
                    type=NodeType.IF_STMT,
                    condition=full_cond,
                    children=body,
                )
                node.children.append(if_child)
            else:
                # Subsequent WHEN → ELSIF
                node.children.append(ParseNode(
                    type=NodeType.ELSIF_CLAUSE,
                    condition=full_cond,
                    children=body,
                ))

        # ELSE branch
        else_ctx = ctx.caseElseBranch()
        if else_ctx:
            else_body = self._visit_body_statements(else_ctx.bodyStatements())
            node.children.append(ParseNode(
                type=NodeType.ELSE_CLAUSE,
                children=else_body,
            ))

        return node

    def visitExitStmt(self, ctx):
        sql_rest = ctx.sqlRest()
        if sql_rest:
            text = "EXIT WHEN " + _get_text(sql_rest, self.input_stream).strip()
        else:
            text = "EXIT"
        return ParseNode(type=NodeType.UNKNOWN_STMT, text=text)

    def visitRaiseStmt(self, ctx):
        id_ctx = ctx.identifier()
        if id_ctx:
            text = "RAISE " + _get_identifier_text(id_ctx)
        else:
            text = "RAISE"
        return ParseNode(type=NodeType.UNKNOWN_STMT, text=text)

    def visitReturnStmt(self, ctx):
        return ParseNode(type=NodeType.UNKNOWN_STMT, text="RETURN")

    def visitOpenStmt(self, ctx):
        name = _get_identifier_text(ctx.identifier())
        sql_rest = ctx.sqlRest()
        if sql_rest:
            text = f"OPEN {name} {_get_text(sql_rest, self.input_stream).strip()}"
        else:
            text = f"OPEN {name}"
        return ParseNode(type=NodeType.UNKNOWN_STMT, text=text)

    def visitFetchStmt(self, ctx):
        name = _get_identifier_text(ctx.identifier())
        into_text = _get_text(ctx.sqlRest(), self.input_stream).strip()
        text = f"FETCH {name} INTO {into_text}"
        return ParseNode(type=NodeType.UNKNOWN_STMT, text=text)

    def visitCloseStmt(self, ctx):
        name = _get_identifier_text(ctx.identifier())
        text = f"CLOSE {name}"
        return ParseNode(type=NodeType.UNKNOWN_STMT, text=text)

    def visitNestedBeginBlock(self, ctx):
        return self.visitBeginBlock(ctx.beginBlock())

    def visitExecuteImmediateStmt(self, ctx):
        text = _get_text(ctx.sqlRest(), self.input_stream).strip()
        return ParseNode(type=NodeType.EXECUTE_IMMEDIATE, text=text)

    def visitSqlStatement(self, ctx):
        # First token is the SQL keyword (SELECT/INSERT/UPDATE/DELETE/TRUNCATE/MERGE)
        keyword_token = ctx.getChild(0)
        keyword = keyword_token.getText().upper()

        # Full statement text including keyword
        full_text = _get_text(ctx, self.input_stream).strip()
        # Remove trailing semicolon
        if full_text.endswith(';'):
            full_text = full_text[:-1].strip()

        return ParseNode(
            type=NodeType.SQL_STATEMENT,
            text=full_text,
            sql_keyword=keyword,
        )

    def visitAssignStmt(self, ctx):
        """idStatement with ASSIGN: variable := value ;"""
        dotted_ctx = ctx.dottedName()
        full_name = _get_text(dotted_ctx, self.input_stream).strip()

        # Add @dblink if present
        at_sign = ctx.AT_SIGN()
        if at_sign:
            dblink = _get_dblink_name(ctx)
            if dblink:
                full_name += '@' + dblink

        value = _get_text(ctx.sqlRest(), self.input_stream).strip()

        return ParseNode(
            type=NodeType.VARIABLE_ASSIGNMENT,
            name=full_name,
            text=value,
        )

    def visitCallWithArgs(self, ctx):
        """idStatement with LPAREN: procedure_call(args) ;"""
        dotted_ctx = ctx.dottedName()
        full_name = _get_text(dotted_ctx, self.input_stream).strip()

        # Add @dblink if present
        at_sign = ctx.AT_SIGN()
        if at_sign:
            dblink = _get_dblink_name(ctx)
            if dblink:
                full_name += '@' + dblink

        # Full call text including name and args
        full_text = _get_text(ctx, self.input_stream).strip()
        if full_text.endswith(';'):
            full_text = full_text[:-1].strip()

        node_type = self._classify_call(full_name)
        return ParseNode(
            type=node_type,
            name=full_name,
            text=full_text,
        )

    def visitCallNoArgs(self, ctx):
        """idStatement with just SEMI: procedure_name ;"""
        dotted_ctx = ctx.dottedName()
        full_name = _get_text(dotted_ctx, self.input_stream).strip()

        # Add @dblink if present
        at_sign = ctx.AT_SIGN()
        if at_sign:
            dblink = _get_dblink_name(ctx)
            if dblink:
                full_name += '@' + dblink

        node_type = self._classify_call(full_name)
        return ParseNode(
            type=node_type,
            name=full_name,
            text=full_name,
        )

    def _classify_call(self, name):
        """Classify a call as DBMS_CALL or PROCEDURE_CALL."""
        upper = name.upper()
        if any(pkg in upper for pkg in ('DBMS_STATS', 'DBMS_SESSION', 'DBMS_SCHEDULER')):
            return NodeType.DBMS_CALL
        return NodeType.PROCEDURE_CALL

    def visitExceptionBlock(self, ctx):
        node = ParseNode(type=NodeType.EXCEPTION_BLOCK)
        for when_ctx in ctx.whenClause():
            when_node = self.visitWhenClause(when_ctx)
            node.children.append(when_node)
        return node

    def visitWhenClause(self, ctx):
        exc_name_ctx = ctx.exceptionName()
        exc_name = _get_text(exc_name_ctx, self.input_stream).strip().upper()

        # Normalize common exception names
        if exc_name == 'NO_DATA_FOUND':
            exc_name = 'NO_DATA_FOUND'
        elif exc_name == 'OTHERS':
            exc_name = 'OTHERS'

        body = self._visit_body_statements(ctx.bodyStatements())
        return ParseNode(
            type=NodeType.WHEN_CLAUSE,
            exception_name=exc_name,
            children=body,
        )


def antlr_parse(source: str) -> ParseNode:
    """Parse PL/SQL source using the ANTLR4 grammar.

    Args:
        source: Raw PL/SQL procedure text

    Returns:
        ParseNode tree matching the hand-rolled parser's output

    Raises:
        Exception: If there are syntax errors
    """
    input_stream = InputStream(source)
    lexer = PLSQLProcedureLexer(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = PLSQLProcedureParser(token_stream)

    # Parse
    tree = parser.procedureDef()

    if parser.getNumberOfSyntaxErrors() > 0:
        raise Exception(f"ANTLR parse failed with {parser.getNumberOfSyntaxErrors()} syntax errors")

    # Convert to ParseNode tree
    visitor = ParseNodeVisitor(input_stream)
    return visitor.visitProcedureDef(tree)
