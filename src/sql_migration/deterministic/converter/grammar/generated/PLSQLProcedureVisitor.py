# Generated from /Users/lohithreddy/Desktop/APP/converter/grammar/PLSQLProcedure.g4 by ANTLR 4.13.2
from antlr4 import *
if "." in __name__:
    from .PLSQLProcedureParser import PLSQLProcedureParser
else:
    from PLSQLProcedureParser import PLSQLProcedureParser

# This class defines a complete generic visitor for a parse tree produced by PLSQLProcedureParser.

class PLSQLProcedureVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by PLSQLProcedureParser#procedureDef.
    def visitProcedureDef(self, ctx:PLSQLProcedureParser.ProcedureDefContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#parameterList.
    def visitParameterList(self, ctx:PLSQLProcedureParser.ParameterListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#parameter.
    def visitParameter(self, ctx:PLSQLProcedureParser.ParameterContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#paramRest.
    def visitParamRest(self, ctx:PLSQLProcedureParser.ParamRestContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#declarationSection.
    def visitDeclarationSection(self, ctx:PLSQLProcedureParser.DeclarationSectionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#varDecl.
    def visitVarDecl(self, ctx:PLSQLProcedureParser.VarDeclContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#typeRef.
    def visitTypeRef(self, ctx:PLSQLProcedureParser.TypeRefContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#typeMod.
    def visitTypeMod(self, ctx:PLSQLProcedureParser.TypeModContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#beginBlock.
    def visitBeginBlock(self, ctx:PLSQLProcedureParser.BeginBlockContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#bodyStatements.
    def visitBodyStatements(self, ctx:PLSQLProcedureParser.BodyStatementsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#statement.
    def visitStatement(self, ctx:PLSQLProcedureParser.StatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#labelStmt.
    def visitLabelStmt(self, ctx:PLSQLProcedureParser.LabelStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#commitStmt.
    def visitCommitStmt(self, ctx:PLSQLProcedureParser.CommitStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#rollbackStmt.
    def visitRollbackStmt(self, ctx:PLSQLProcedureParser.RollbackStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#nullStmt.
    def visitNullStmt(self, ctx:PLSQLProcedureParser.NullStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#gotoStmt.
    def visitGotoStmt(self, ctx:PLSQLProcedureParser.GotoStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#ifStmt.
    def visitIfStmt(self, ctx:PLSQLProcedureParser.IfStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#elsifClause.
    def visitElsifClause(self, ctx:PLSQLProcedureParser.ElsifClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#elseClause.
    def visitElseClause(self, ctx:PLSQLProcedureParser.ElseClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#condition.
    def visitCondition(self, ctx:PLSQLProcedureParser.ConditionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#condToken.
    def visitCondToken(self, ctx:PLSQLProcedureParser.CondTokenContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#forLoopStmt.
    def visitForLoopStmt(self, ctx:PLSQLProcedureParser.ForLoopStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#rangeExpr.
    def visitRangeExpr(self, ctx:PLSQLProcedureParser.RangeExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#rangeBound.
    def visitRangeBound(self, ctx:PLSQLProcedureParser.RangeBoundContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#rangeBoundToken.
    def visitRangeBoundToken(self, ctx:PLSQLProcedureParser.RangeBoundTokenContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#cursorSql.
    def visitCursorSql(self, ctx:PLSQLProcedureParser.CursorSqlContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#cursorToken.
    def visitCursorToken(self, ctx:PLSQLProcedureParser.CursorTokenContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#whileLoopStmt.
    def visitWhileLoopStmt(self, ctx:PLSQLProcedureParser.WhileLoopStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#whileCondition.
    def visitWhileCondition(self, ctx:PLSQLProcedureParser.WhileConditionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#whileCondToken.
    def visitWhileCondToken(self, ctx:PLSQLProcedureParser.WhileCondTokenContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#caseStmt.
    def visitCaseStmt(self, ctx:PLSQLProcedureParser.CaseStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#caseSelector.
    def visitCaseSelector(self, ctx:PLSQLProcedureParser.CaseSelectorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#caseSelectorToken.
    def visitCaseSelectorToken(self, ctx:PLSQLProcedureParser.CaseSelectorTokenContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#caseWhenBranch.
    def visitCaseWhenBranch(self, ctx:PLSQLProcedureParser.CaseWhenBranchContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#caseElseBranch.
    def visitCaseElseBranch(self, ctx:PLSQLProcedureParser.CaseElseBranchContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#exitStmt.
    def visitExitStmt(self, ctx:PLSQLProcedureParser.ExitStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#raiseStmt.
    def visitRaiseStmt(self, ctx:PLSQLProcedureParser.RaiseStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#returnStmt.
    def visitReturnStmt(self, ctx:PLSQLProcedureParser.ReturnStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#openStmt.
    def visitOpenStmt(self, ctx:PLSQLProcedureParser.OpenStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#fetchStmt.
    def visitFetchStmt(self, ctx:PLSQLProcedureParser.FetchStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#closeStmt.
    def visitCloseStmt(self, ctx:PLSQLProcedureParser.CloseStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#nestedBeginBlock.
    def visitNestedBeginBlock(self, ctx:PLSQLProcedureParser.NestedBeginBlockContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#executeImmediateStmt.
    def visitExecuteImmediateStmt(self, ctx:PLSQLProcedureParser.ExecuteImmediateStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#sqlStatement.
    def visitSqlStatement(self, ctx:PLSQLProcedureParser.SqlStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#assignStmt.
    def visitAssignStmt(self, ctx:PLSQLProcedureParser.AssignStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#callWithArgs.
    def visitCallWithArgs(self, ctx:PLSQLProcedureParser.CallWithArgsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#callNoArgs.
    def visitCallNoArgs(self, ctx:PLSQLProcedureParser.CallNoArgsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#dottedName.
    def visitDottedName(self, ctx:PLSQLProcedureParser.DottedNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#sqlRest.
    def visitSqlRest(self, ctx:PLSQLProcedureParser.SqlRestContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#sqlToken.
    def visitSqlToken(self, ctx:PLSQLProcedureParser.SqlTokenContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#exceptionBlock.
    def visitExceptionBlock(self, ctx:PLSQLProcedureParser.ExceptionBlockContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#whenClause.
    def visitWhenClause(self, ctx:PLSQLProcedureParser.WhenClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#exceptionName.
    def visitExceptionName(self, ctx:PLSQLProcedureParser.ExceptionNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by PLSQLProcedureParser#identifier.
    def visitIdentifier(self, ctx:PLSQLProcedureParser.IdentifierContext):
        return self.visitChildren(ctx)



del PLSQLProcedureParser