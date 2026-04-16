/**
 * PL/SQL Procedure Grammar for ANTLR4
 *
 * Parses PL/SQL procedural structure while treating SQL as opaque text.
 * Combined lexer+parser grammar. Case-insensitive keywords via fragments.
 */
grammar PLSQLProcedure;

// ============================================================
// Parser Rules
// ============================================================

procedureDef
    : PROCEDURE identifier parameterList? (IS | AS) declarationSection? beginBlock identifier? SEMI? EOF
    ;

parameterList
    : LPAREN parameter (COMMA parameter)* RPAREN
    ;

parameter
    : identifier paramRest*
    ;

paramRest
    : ~(COMMA | RPAREN)
    ;

declarationSection
    : varDecl+
    ;

varDecl
    : identifier typeRef typeMod? (ASSIGN sqlRest)? SEMI
    ;

typeRef
    : identifier (DOT identifier)* (PERCENT identifier)?
    ;

typeMod
    : LPAREN sqlRest RPAREN
    ;

beginBlock
    : BEGIN bodyStatements exceptionBlock? END
    ;

bodyStatements
    : statement*
    ;

statement
    : labelStmt
    | commitStmt
    | rollbackStmt
    | nullStmt
    | gotoStmt
    | ifStmt
    | forLoopStmt
    | whileLoopStmt
    | caseStmt
    | exitStmt
    | raiseStmt
    | openStmt
    | fetchStmt
    | closeStmt
    | returnStmt
    | nestedBeginBlock
    | executeImmediateStmt
    | sqlStatement
    | idStatement
    ;

labelStmt
    : LABEL_OPEN identifier LABEL_CLOSE
    ;

commitStmt
    : COMMIT SEMI
    ;

rollbackStmt
    : ROLLBACK SEMI
    ;

nullStmt
    : NULL_ SEMI
    ;

gotoStmt
    : GOTO identifier SEMI
    ;

ifStmt
    : IF condition THEN bodyStatements elsifClause* elseClause? END IF SEMI
    ;

elsifClause
    : ELSIF condition THEN bodyStatements
    ;

elseClause
    : ELSE bodyStatements
    ;

condition
    : condToken+
    ;

condToken
    : LPAREN condition RPAREN
    | ~(THEN | LPAREN | RPAREN)
    ;

forLoopStmt
    : FOR identifier IN LPAREN cursorSql RPAREN LOOP bodyStatements END LOOP SEMI
    | FOR identifier IN REVERSE? rangeExpr LOOP bodyStatements END LOOP SEMI
    ;

rangeExpr
    : rangeBound DOTDOT rangeBound
    ;

rangeBound
    : rangeBoundToken+
    ;

rangeBoundToken
    : LPAREN rangeBound RPAREN
    | ~(DOTDOT | LOOP | LPAREN | RPAREN | SEMI)
    ;

cursorSql
    : cursorToken+
    ;

cursorToken
    : LPAREN cursorSql RPAREN
    | ~(LPAREN | RPAREN)
    ;

whileLoopStmt
    : WHILE whileCondition LOOP bodyStatements END LOOP SEMI
    ;

whileCondition
    : whileCondToken+
    ;

whileCondToken
    : LPAREN whileCondition RPAREN
    | ~(LOOP | LPAREN | RPAREN)
    ;

caseStmt
    : CASE caseSelector? caseWhenBranch+ caseElseBranch? END CASE SEMI
    ;

caseSelector
    : caseSelectorToken+
    ;

caseSelectorToken
    : LPAREN caseSelector RPAREN
    | ~(WHEN | LPAREN | RPAREN)
    ;

caseWhenBranch
    : WHEN condition THEN bodyStatements
    ;

caseElseBranch
    : ELSE bodyStatements
    ;

exitStmt
    : EXIT WHEN sqlRest SEMI
    | EXIT SEMI
    ;

raiseStmt
    : RAISE identifier? SEMI
    ;

returnStmt
    : RETURN SEMI
    ;

openStmt
    : OPEN identifier sqlRest? SEMI
    ;

fetchStmt
    : FETCH identifier INTO sqlRest SEMI
    ;

closeStmt
    : CLOSE identifier SEMI
    ;

nestedBeginBlock
    : beginBlock identifier? SEMI
    ;

executeImmediateStmt
    : EXECUTE IMMEDIATE sqlRest SEMI
    ;

sqlStatement
    : (SELECT | INSERT | UPDATE | DELETE | TRUNCATE | MERGE) sqlRest SEMI
    ;

idStatement
    : dottedName (AT_SIGN identifier)? ASSIGN sqlRest SEMI        # assignStmt
    | dottedName (AT_SIGN identifier)? LPAREN sqlRest RPAREN SEMI  # callWithArgs
    | dottedName (AT_SIGN identifier)? SEMI                        # callNoArgs
    ;

dottedName
    : identifier (DOT identifier)*
    ;

// Opaque SQL: captures everything up to SEMI at balanced paren depth
sqlRest
    : sqlToken+
    ;

sqlToken
    : LPAREN sqlRest RPAREN
    | ~(SEMI | LPAREN | RPAREN)
    ;

exceptionBlock
    : EXCEPTION whenClause+
    ;

whenClause
    : WHEN exceptionName THEN bodyStatements
    ;

exceptionName
    : identifier (DOT identifier)*
    ;

// Allow keywords as identifiers in certain contexts
identifier
    : ID
    | PROCEDURE
    | IS
    | AS
    | EXCEPTION
    | WHEN
    | THEN
    | OTHERS
    | NULL_
    | IN
    | LOOP
    | GOTO
    | COMMIT
    | ROLLBACK
    | EXECUTE
    | IMMEDIATE
    | SELECT
    | INSERT
    | UPDATE
    | DELETE
    | TRUNCATE
    | WHILE
    | EXIT
    | RAISE
    | CASE
    | MERGE
    | REVERSE
    | OPEN
    | FETCH
    | CLOSE
    | INTO
    | RETURN
    ;


// ============================================================
// Lexer Rules — Case-insensitive keywords via fragments
// ============================================================

// Fragments for case-insensitive matching
fragment A: [aA]; fragment B: [bB]; fragment C: [cC]; fragment D: [dD];
fragment E: [eE]; fragment F: [fF]; fragment G: [gG]; fragment H: [hH];
fragment I_: [iI]; fragment J: [jJ]; fragment K: [kK]; fragment L: [lL];
fragment M: [mM]; fragment N: [nN]; fragment O: [oO]; fragment P: [pP];
fragment Q: [qQ]; fragment R: [rR]; fragment S: [sS]; fragment T: [tT];
fragment U: [uU]; fragment V: [vV]; fragment W: [wW]; fragment X: [xX];
fragment Y: [yY]; fragment Z: [zZ];

// Keywords
PROCEDURE    : P R O C E D U R E ;
IS           : I_ S ;
AS           : A S ;
BEGIN        : B E G I_ N ;
END          : E N D ;
EXCEPTION    : E X C E P T I_ O N ;
WHEN         : W H E N ;
THEN         : T H E N ;
OTHERS       : O T H E R S ;
NULL_        : N U L L ;
IF           : I_ F ;
ELSIF        : E L S I_ F ;
ELSE         : E L S E ;
FOR          : F O R ;
IN           : I_ N ;
LOOP         : L O O P ;
GOTO         : G O T O ;
COMMIT       : C O M M I_ T ;
ROLLBACK     : R O L L B A C K ;
EXECUTE      : E X E C U T E ;
IMMEDIATE    : I_ M M E D I_ A T E ;
SELECT       : S E L E C T ;
INSERT       : I_ N S E R T ;
UPDATE       : U P D A T E ;
DELETE       : D E L E T E ;
TRUNCATE     : T R U N C A T E ;
WHILE        : W H I_ L E ;
EXIT         : E X I_ T ;
RAISE        : R A I_ S E ;
CASE         : C A S E ;
MERGE        : M E R G E ;
REVERSE      : R E V E R S E ;
OPEN         : O P E N ;
FETCH        : F E T C H ;
CLOSE        : C L O S E ;
INTO         : I_ N T O ;
RETURN       : R E T U R N ;

// Symbols — DOTDOT must come before DOT
SEMI         : ';' ;
LPAREN       : '(' ;
RPAREN       : ')' ;
COMMA        : ',' ;
DOTDOT       : '..' ;
DOT          : '.' ;
ASSIGN       : ':=' ;
LABEL_OPEN   : '<<' ;
LABEL_CLOSE  : '>>' ;
AT_SIGN      : '@' ;
PERCENT      : '%' ;

// Literals
STRING_LITERAL
    : '\'' ( '\'\'' | ~'\'' )* '\''
    ;

NUMBER_LITERAL
    : [0-9]+ ('.' [0-9]+)?
    ;

// Identifier (must come after keywords)
// $ is allowed in Oracle identifiers (e.g. SYS$SESSION, V$PARAMETER)
ID
    : [a-zA-Z_$] [a-zA-Z0-9_$]*
    ;

// Operators — catch-all for things like ||, >=, !=, +, -, etc.
// Note: % is handled by PERCENT token
OP
    : [+\-*/=<>!^~&|?#:]+
    ;

// Skip whitespace and comments
WS
    : [ \t\r\n]+ -> skip
    ;

LINE_COMMENT
    : '--' ~[\r\n]* -> skip
    ;

BLOCK_COMMENT
    : '/*' .*? '*/' -> skip
    ;
