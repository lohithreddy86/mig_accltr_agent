"""
Recursive Descent PL/SQL Parser

Replaces ANTLR grammar + generated parser with a hand-rolled recursive descent
parser. Produces parse tree nodes that the visitor (antlr_to_ir.py) converts
to ProcedureIR.

The parser treats SQL statements as opaque text (captured up to `;` at paren
depth 0), while structurally parsing PL/SQL procedural constructs:
  - Procedure header + variable declarations
  - BEGIN/END blocks with EXCEPTION handlers
  - IF/ELSIF/ELSE/END IF
  - FOR cursor loops
  - WHILE loops (future)
  - GOTO + labels
  - EXECUTE IMMEDIATE
  - DBMS_* calls
  - Variable assignments
  - COMMIT/ROLLBACK
"""

import re
from dataclasses import dataclass, field
from enum import Enum, auto


class TokenType(Enum):
    # Keywords
    PROCEDURE = auto()
    IS = auto()
    AS = auto()
    BEGIN = auto()
    END = auto()
    EXCEPTION = auto()
    WHEN = auto()
    THEN = auto()
    OTHERS = auto()
    NO_DATA_FOUND = auto()
    NULL = auto()
    IF = auto()
    ELSIF = auto()
    ELSE = auto()
    FOR = auto()
    IN = auto()
    LOOP = auto()
    WHILE = auto()
    GOTO = auto()
    COMMIT = auto()
    ROLLBACK = auto()
    EXECUTE = auto()
    IMMEDIATE = auto()
    SELECT = auto()
    INSERT = auto()
    UPDATE = auto()
    DELETE = auto()
    TRUNCATE = auto()
    INTO = auto()
    FROM = auto()
    WHERE = auto()
    SET = auto()
    VALUES = auto()
    AND = auto()
    OR = auto()
    NOT = auto()

    # Symbols
    SEMI = auto()
    LPAREN = auto()
    RPAREN = auto()
    COMMA = auto()
    DOT = auto()
    ASSIGN = auto()       # :=
    LABEL_OPEN = auto()   # <<
    LABEL_CLOSE = auto()  # >>
    CONCAT = auto()       # ||
    AT_SIGN = auto()      # @

    # Literals
    STRING_LITERAL = auto()
    NUMBER_LITERAL = auto()
    ID = auto()

    # Special
    EOF = auto()
    NEWLINE = auto()


# Case-insensitive keyword map
_KEYWORDS = {
    'PROCEDURE': TokenType.PROCEDURE,
    'IS': TokenType.IS,
    'AS': TokenType.AS,
    'BEGIN': TokenType.BEGIN,
    'END': TokenType.END,
    'EXCEPTION': TokenType.EXCEPTION,
    'WHEN': TokenType.WHEN,
    'THEN': TokenType.THEN,
    'OTHERS': TokenType.OTHERS,
    'NO_DATA_FOUND': TokenType.NO_DATA_FOUND,
    'NULL': TokenType.NULL,
    'IF': TokenType.IF,
    'ELSIF': TokenType.ELSIF,
    'ELSE': TokenType.ELSE,
    'FOR': TokenType.FOR,
    'IN': TokenType.IN,
    'LOOP': TokenType.LOOP,
    'WHILE': TokenType.WHILE,
    'GOTO': TokenType.GOTO,
    'COMMIT': TokenType.COMMIT,
    'ROLLBACK': TokenType.ROLLBACK,
    'EXECUTE': TokenType.EXECUTE,
    'IMMEDIATE': TokenType.IMMEDIATE,
    'SELECT': TokenType.SELECT,
    'INSERT': TokenType.INSERT,
    'UPDATE': TokenType.UPDATE,
    'DELETE': TokenType.DELETE,
    'TRUNCATE': TokenType.TRUNCATE,
    'INTO': TokenType.INTO,
    'FROM': TokenType.FROM,
    'WHERE': TokenType.WHERE,
    'SET': TokenType.SET,
    'VALUES': TokenType.VALUES,
    'AND': TokenType.AND,
    'OR': TokenType.OR,
    'NOT': TokenType.NOT,
}


@dataclass
class Token:
    type: TokenType
    text: str
    pos: int      # char offset in source
    line: int     # 1-based line number


class ParseError(Exception):
    """Raised when the parser cannot recover from a syntax error."""
    def __init__(self, message: str, token: Token | None = None):
        self.token = token
        loc = f" at line {token.line}" if token else ""
        super().__init__(f"{message}{loc}")


# --- Lexer ---

def tokenize(source: str) -> list[Token]:
    """Tokenize PL/SQL source into a token list."""
    tokens = []
    pos = 0
    line = 1

    while pos < len(source):
        c = source[pos]

        # Whitespace (track newlines)
        if c in ' \t\r':
            pos += 1
            continue
        if c == '\n':
            line += 1
            pos += 1
            continue

        # Line comment
        if c == '-' and pos + 1 < len(source) and source[pos + 1] == '-':
            end = source.find('\n', pos)
            if end == -1:
                pos = len(source)
            else:
                pos = end  # newline handled next iteration
            continue

        # Block comment
        if c == '/' and pos + 1 < len(source) and source[pos + 1] == '*':
            end = source.find('*/', pos + 2)
            if end == -1:
                pos = len(source)
            else:
                # Count newlines inside comment
                line += source[pos:end + 2].count('\n')
                pos = end + 2
            continue

        # Label open <<
        if c == '<' and pos + 1 < len(source) and source[pos + 1] == '<':
            tokens.append(Token(TokenType.LABEL_OPEN, '<<', pos, line))
            pos += 2
            continue

        # Label close >>
        if c == '>' and pos + 1 < len(source) and source[pos + 1] == '>':
            tokens.append(Token(TokenType.LABEL_CLOSE, '>>', pos, line))
            pos += 2
            continue

        # Assign :=
        if c == ':' and pos + 1 < len(source) and source[pos + 1] == '=':
            tokens.append(Token(TokenType.ASSIGN, ':=', pos, line))
            pos += 2
            continue

        # Concat ||
        if c == '|' and pos + 1 < len(source) and source[pos + 1] == '|':
            tokens.append(Token(TokenType.CONCAT, '||', pos, line))
            pos += 2
            continue

        # Single-char symbols
        if c == ';':
            tokens.append(Token(TokenType.SEMI, ';', pos, line))
            pos += 1
            continue
        if c == '(':
            tokens.append(Token(TokenType.LPAREN, '(', pos, line))
            pos += 1
            continue
        if c == ')':
            tokens.append(Token(TokenType.RPAREN, ')', pos, line))
            pos += 1
            continue
        if c == ',':
            tokens.append(Token(TokenType.COMMA, ',', pos, line))
            pos += 1
            continue
        if c == '.':
            tokens.append(Token(TokenType.DOT, '.', pos, line))
            pos += 1
            continue
        if c == '@':
            tokens.append(Token(TokenType.AT_SIGN, '@', pos, line))
            pos += 1
            continue

        # String literal (single quotes, with '' escapes)
        if c == "'":
            end = pos + 1
            while end < len(source):
                if source[end] == "'":
                    if end + 1 < len(source) and source[end + 1] == "'":
                        end += 2  # escaped quote
                    else:
                        end += 1
                        break
                else:
                    if source[end] == '\n':
                        line += 1
                    end += 1
            text = source[pos:end]
            tokens.append(Token(TokenType.STRING_LITERAL, text, pos, line))
            pos = end
            continue

        # Number literal
        if c.isdigit() or (c == '.' and pos + 1 < len(source) and source[pos + 1].isdigit()):
            end = pos
            while end < len(source) and (source[end].isdigit() or source[end] == '.'):
                end += 1
            text = source[pos:end]
            tokens.append(Token(TokenType.NUMBER_LITERAL, text, pos, line))
            pos = end
            continue

        # Identifier or keyword
        if c.isalpha() or c == '_':
            end = pos
            while end < len(source) and (source[end].isalnum() or source[end] == '_'):
                end += 1
            text = source[pos:end]
            upper = text.upper()
            ttype = _KEYWORDS.get(upper, TokenType.ID)
            tokens.append(Token(ttype, text, pos, line))
            pos = end
            continue

        # Operators and other characters — emit as ID tokens for opaque SQL capture
        # Covers: +, -, *, /, =, <, >, !, %, etc.
        if c in '+-*/=<>!%^~&?#:':
            # Multi-char operators
            end = pos + 1
            while end < len(source) and source[end] in '+-*/=<>!%^~&':
                end += 1
            text = source[pos:end]
            tokens.append(Token(TokenType.ID, text, pos, line))
            pos = end
            continue

        # Skip unknown characters
        pos += 1

    tokens.append(Token(TokenType.EOF, '', pos, line))
    return tokens


# --- Parse Tree Nodes ---

class NodeType(Enum):
    PROCEDURE = auto()
    VAR_DECL = auto()
    BEGIN_BLOCK = auto()
    EXCEPTION_BLOCK = auto()
    WHEN_CLAUSE = auto()
    IF_STMT = auto()
    ELSIF_CLAUSE = auto()
    ELSE_CLAUSE = auto()
    FOR_LOOP = auto()
    GOTO_STMT = auto()
    LABEL = auto()
    COMMIT_STMT = auto()
    ROLLBACK_STMT = auto()
    EXECUTE_IMMEDIATE = auto()
    VARIABLE_ASSIGNMENT = auto()
    SQL_STATEMENT = auto()     # opaque SQL (INSERT/UPDATE/DELETE/SELECT/TRUNCATE)
    PROCEDURE_CALL = auto()
    DBMS_CALL = auto()
    NULL_STMT = auto()
    WHILE_LOOP = auto()
    UNKNOWN_STMT = auto()


@dataclass
class ParseNode:
    """Parse tree node."""
    type: NodeType
    children: list['ParseNode'] = field(default_factory=list)
    text: str = ""             # raw text for opaque SQL, assignments, etc.
    name: str = ""             # procedure name, variable name, label name
    condition: str = ""        # IF/ELSIF condition text
    params: list[str] = field(default_factory=list)
    var_type: str = ""         # variable declaration type
    var_default: str = ""      # variable default value
    sql_keyword: str = ""      # INSERT/UPDATE/DELETE/SELECT/TRUNCATE
    exception_name: str = ""   # OTHERS, NO_DATA_FOUND, etc.
    cursor_var: str = ""       # FOR loop cursor variable
    cursor_sql: str = ""       # FOR loop SELECT SQL
    line: int = 0              # source line number


# --- Parser ---

class ProcedureParser:
    """Recursive descent parser for PL/SQL procedures."""

    def __init__(self, tokens: list[Token], source: str):
        self.tokens = tokens
        self.source = source
        self.pos = 0
        self.errors: list[str] = []

    def peek(self) -> Token:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else self.tokens[-1]

    def advance(self) -> Token:
        tok = self.tokens[self.pos]
        if self.pos < len(self.tokens) - 1:
            self.pos += 1
        return tok

    def match(self, *types: TokenType) -> Token | None:
        if self.peek().type in types:
            return self.advance()
        return None

    def expect(self, ttype: TokenType) -> Token:
        tok = self.match(ttype)
        if tok is None:
            raise ParseError(f"Expected {ttype.name}, got {self.peek().type.name} ('{self.peek().text}')", self.peek())
        return tok

    def at(self, *types: TokenType) -> bool:
        return self.peek().type in types

    def at_keyword_seq(self, *keywords: str) -> bool:
        """Check if upcoming tokens match a keyword sequence (case-insensitive)."""
        for i, kw in enumerate(keywords):
            idx = self.pos + i
            if idx >= len(self.tokens):
                return False
            if self.tokens[idx].text.upper() != kw.upper():
                return False
        return True

    def _text_between(self, start_pos: int, end_pos: int) -> str:
        """Get raw source text between two character positions."""
        return self.source[start_pos:end_pos]

    def _collect_until_semi(self) -> str:
        """Collect all tokens until `;` at paren depth 0, returning raw source text."""
        start = self.peek().pos
        depth = 0
        while not self.at(TokenType.EOF):
            if self.at(TokenType.LPAREN):
                depth += 1
                self.advance()
            elif self.at(TokenType.RPAREN):
                depth -= 1
                self.advance()
            elif self.at(TokenType.SEMI) and depth <= 0:
                end = self.peek().pos
                self.advance()  # consume ;
                return self.source[start:end].strip()
            else:
                self.advance()
        return self.source[start:].strip()

    def _collect_condition(self) -> str:
        """Collect tokens forming an IF/ELSIF condition until THEN."""
        start = self.peek().pos
        depth = 0
        while not self.at(TokenType.EOF):
            if self.at(TokenType.LPAREN):
                depth += 1
                self.advance()
            elif self.at(TokenType.RPAREN):
                depth -= 1
                self.advance()
            elif self.at(TokenType.THEN) and depth <= 0:
                end = self.peek().pos
                return self.source[start:end].strip()
            else:
                self.advance()
        return self.source[start:].strip()

    def _collect_for_cursor_sql(self) -> str:
        """Collect the SELECT SQL inside FOR var IN ( ... ) LOOP."""
        # We're positioned right after LPAREN
        start = self.peek().pos
        depth = 1
        while not self.at(TokenType.EOF):
            if self.at(TokenType.LPAREN):
                depth += 1
                self.advance()
            elif self.at(TokenType.RPAREN):
                depth -= 1
                if depth == 0:
                    end = self.peek().pos
                    self.advance()  # consume )
                    return self.source[start:end].strip()
                self.advance()
            else:
                self.advance()
        return self.source[start:].strip()

    # --- Entry point ---

    def parse_procedure(self) -> ParseNode:
        """Parse a complete procedure definition."""
        node = ParseNode(type=NodeType.PROCEDURE, line=self.peek().line)

        # PROCEDURE name
        self.expect(TokenType.PROCEDURE)
        name_tok = self.expect(TokenType.ID)
        node.name = name_tok.text

        # Optional parameters
        if self.match(TokenType.LPAREN):
            node.params = self._parse_param_list()

        # IS or AS
        if not self.match(TokenType.IS):
            self.expect(TokenType.AS)

        # Variable declarations (between IS/AS and BEGIN)
        while not self.at(TokenType.BEGIN, TokenType.EOF):
            var_node = self._try_parse_var_decl()
            if var_node:
                node.children.append(var_node)
            else:
                # Skip unknown declaration lines
                self.advance()

        # BEGIN ... END
        begin_node = self._parse_begin_block()
        node.children.append(begin_node)

        # END procedure_name ;
        # The begin_block parser already consumed the END.
        # But the top-level procedure has END name ;
        # Actually, _parse_begin_block handles END; so we might have name left
        # Let's handle this: after the begin block, we may have the proc name and ;
        self.match(TokenType.ID)  # optional procedure name after END
        self.match(TokenType.SEMI)

        return node

    def _parse_param_list(self) -> list[str]:
        """Parse parameter list inside parentheses (already consumed LPAREN)."""
        params = []
        while not self.at(TokenType.RPAREN, TokenType.EOF):
            if self.at(TokenType.ID):
                param_name = self.advance().text
                params.append(param_name)
                # Skip type info: IN, OUT, IN OUT, type name, default value
                while not self.at(TokenType.COMMA, TokenType.RPAREN, TokenType.EOF):
                    self.advance()
                self.match(TokenType.COMMA)
            else:
                self.advance()
        self.expect(TokenType.RPAREN)
        return params

    def _try_parse_var_decl(self) -> ParseNode | None:
        """Try to parse a variable declaration. Returns None if not a var decl."""
        if not self.at(TokenType.ID):
            return None

        # Lookahead: ID followed by a type keyword (Varchar2, Number, Date, etc.)
        # or ID followed by another ID (the type)
        saved_pos = self.pos
        name_tok = self.advance()

        # Check if this looks like a variable declaration
        # Variable declarations have: name type [(:= expr)];
        if self.at(TokenType.ID):
            var_type_start = self.peek().pos
            var_type = self.advance().text

            # Handle type modifiers like Varchar2(4000)
            if self.match(TokenType.LPAREN):
                while not self.at(TokenType.RPAREN, TokenType.EOF):
                    var_type += self.advance().text
                    if self.at(TokenType.RPAREN):
                        break
                self.match(TokenType.RPAREN)
                var_type += ')'

            default_val = ""
            if self.match(TokenType.ASSIGN):
                # Collect default value until ;
                default_start = self.peek().pos
                default_val = self._collect_until_semi()
                node = ParseNode(
                    type=NodeType.VAR_DECL,
                    name=name_tok.text,
                    var_type=var_type,
                    var_default=default_val,
                    line=name_tok.line,
                )
                return node
            elif self.match(TokenType.SEMI):
                node = ParseNode(
                    type=NodeType.VAR_DECL,
                    name=name_tok.text,
                    var_type=var_type,
                    line=name_tok.line,
                )
                return node

        # Not a variable declaration, restore position
        self.pos = saved_pos
        return None

    def _parse_begin_block(self) -> ParseNode:
        """Parse BEGIN ... [EXCEPTION ...] END [name] ;"""
        node = ParseNode(type=NodeType.BEGIN_BLOCK, line=self.peek().line)
        self.expect(TokenType.BEGIN)

        # Parse body statements until EXCEPTION or END
        node.children = self._parse_body_statements()

        # EXCEPTION block
        if self.match(TokenType.EXCEPTION):
            exc_node = self._parse_exception_block()
            node.children.append(exc_node)

        # END [name] ;
        self.expect(TokenType.END)
        # Optional name or IF after END
        # Don't consume — let the caller deal with trailing name/semi
        return node

    def _parse_body_statements(self) -> list[ParseNode]:
        """Parse statements until EXCEPTION, END, ELSIF, ELSE, or EOF."""
        stmts = []
        while not self.at(TokenType.EXCEPTION, TokenType.END, TokenType.EOF,
                          TokenType.ELSIF, TokenType.ELSE):
            stmt = self._parse_statement()
            if stmt:
                stmts.append(stmt)
        return stmts

    def _parse_statement(self) -> ParseNode | None:
        """Parse a single statement. Returns None for skip."""
        tok = self.peek()

        # Label: <<name>>
        if self.at(TokenType.LABEL_OPEN):
            return self._parse_label()

        # COMMIT;
        if self.at(TokenType.COMMIT):
            self.advance()
            self.match(TokenType.SEMI)
            return ParseNode(type=NodeType.COMMIT_STMT, line=tok.line)

        # ROLLBACK;
        if self.at(TokenType.ROLLBACK):
            self.advance()
            self.match(TokenType.SEMI)
            return ParseNode(type=NodeType.ROLLBACK_STMT, line=tok.line)

        # NULL;
        if self.at(TokenType.NULL):
            self.advance()
            self.match(TokenType.SEMI)
            return ParseNode(type=NodeType.NULL_STMT, line=tok.line)

        # GOTO label;
        if self.at(TokenType.GOTO):
            return self._parse_goto()

        # IF ... THEN ... END IF;
        if self.at(TokenType.IF):
            return self._parse_if()

        # FOR ... IN ... LOOP ... END LOOP;
        if self.at(TokenType.FOR):
            return self._parse_for_loop()

        # BEGIN ... END; (nested block)
        if self.at(TokenType.BEGIN):
            block = self._parse_begin_block()
            self.match(TokenType.ID)  # optional name after END
            self.match(TokenType.SEMI)
            return block

        # EXECUTE IMMEDIATE
        if self.at(TokenType.EXECUTE):
            return self._parse_execute_immediate()

        # INSERT/UPDATE/DELETE/SELECT/TRUNCATE — opaque SQL
        if self.at(TokenType.INSERT, TokenType.UPDATE, TokenType.DELETE,
                   TokenType.SELECT, TokenType.TRUNCATE):
            return self._parse_sql_statement()

        # DBMS_* calls or other identifier-starting statements
        if self.at(TokenType.ID):
            return self._parse_id_statement()

        # Unknown — skip token
        self.advance()
        return None

    def _parse_label(self) -> ParseNode:
        """Parse <<label_name>>"""
        line = self.peek().line
        self.expect(TokenType.LABEL_OPEN)
        name = self.expect(TokenType.ID).text
        self.expect(TokenType.LABEL_CLOSE)
        return ParseNode(type=NodeType.LABEL, name=name, line=line)

    def _parse_goto(self) -> ParseNode:
        """Parse GOTO label_name ;"""
        line = self.peek().line
        self.advance()  # GOTO
        name = self.expect(TokenType.ID).text
        self.match(TokenType.SEMI)
        return ParseNode(type=NodeType.GOTO_STMT, name=name, line=line)

    def _parse_if(self) -> ParseNode:
        """Parse IF condition THEN body [ELSIF...] [ELSE...] END IF ;"""
        node = ParseNode(type=NodeType.IF_STMT, line=self.peek().line)

        # IF condition THEN
        self.advance()  # IF
        condition = self._collect_condition()
        self.expect(TokenType.THEN)

        # IF branch body
        if_body = self._parse_body_statements()
        if_child = ParseNode(
            type=NodeType.IF_STMT,
            condition=condition,
            children=if_body,
            line=node.line,
        )
        node.children.append(if_child)

        # ELSIF branches
        while self.match(TokenType.ELSIF):
            elsif_cond = self._collect_condition()
            self.expect(TokenType.THEN)
            elsif_body = self._parse_body_statements()
            node.children.append(ParseNode(
                type=NodeType.ELSIF_CLAUSE,
                condition=elsif_cond,
                children=elsif_body,
                line=self.peek().line,
            ))

        # ELSE branch
        if self.match(TokenType.ELSE):
            else_body = self._parse_body_statements()
            node.children.append(ParseNode(
                type=NodeType.ELSE_CLAUSE,
                children=else_body,
                line=self.peek().line,
            ))

        # END IF ;
        self.expect(TokenType.END)
        self.match(TokenType.IF)  # optional — some code has END IF, some END
        self.match(TokenType.SEMI)

        node.condition = condition
        return node

    def _parse_for_loop(self) -> ParseNode:
        """Parse FOR var IN (SELECT ...) LOOP body END LOOP ;"""
        node = ParseNode(type=NodeType.FOR_LOOP, line=self.peek().line)

        self.advance()  # FOR
        cursor_var = self.expect(TokenType.ID).text
        node.cursor_var = cursor_var
        self.expect(TokenType.IN)
        self.expect(TokenType.LPAREN)

        # Collect cursor SQL
        node.cursor_sql = self._collect_for_cursor_sql()

        # LOOP
        self.expect(TokenType.LOOP)

        # Loop body
        node.children = self._parse_body_statements()

        # END LOOP ;
        self.expect(TokenType.END)
        self.match(TokenType.LOOP)
        self.match(TokenType.SEMI)

        return node

    def _parse_execute_immediate(self) -> ParseNode:
        """Parse EXECUTE IMMEDIATE expression ;"""
        line = self.peek().line
        self.advance()  # EXECUTE
        self.expect(TokenType.IMMEDIATE)
        text = self._collect_until_semi()
        return ParseNode(type=NodeType.EXECUTE_IMMEDIATE, text=text, line=line)

    def _parse_sql_statement(self) -> ParseNode:
        """Parse an opaque SQL statement (INSERT, UPDATE, DELETE, SELECT, TRUNCATE)."""
        tok = self.peek()
        keyword = tok.text.upper()
        line = tok.line
        text = self._collect_until_semi()
        return ParseNode(
            type=NodeType.SQL_STATEMENT,
            text=text,
            sql_keyword=keyword,
            line=line,
        )

    def _parse_id_statement(self) -> ParseNode:
        """Parse a statement starting with an identifier.

        Could be:
        - Variable assignment: var := expr ;
        - DBMS_STATS/SESSION/SCHEDULER call
        - Local/remote procedure call
        """
        tok = self.peek()
        line = tok.line

        # Look ahead for :=  (assignment)
        # We need to check if this is var := or schema.pkg.func(...)
        saved_pos = self.pos
        name_parts = [self.advance().text]

        # Collect dotted name: a.b.c
        while self.match(TokenType.DOT):
            if self.at(TokenType.ID) or self.at_any_keyword():
                name_parts.append(self.advance().text)

        full_name = '.'.join(name_parts)

        # Check for @ (db link)
        if self.match(TokenType.AT_SIGN):
            if self.at(TokenType.ID):
                full_name += '@' + self.advance().text

        # Variable assignment: name := expr ;
        if self.match(TokenType.ASSIGN):
            value = self._collect_until_semi()
            return ParseNode(
                type=NodeType.VARIABLE_ASSIGNMENT,
                name=full_name,
                text=value,
                line=line,
            )

        # DBMS_* call
        upper_name = full_name.upper()
        if 'DBMS_STATS' in upper_name or 'DBMS_SESSION' in upper_name or 'DBMS_SCHEDULER' in upper_name:
            # Restore and collect full statement
            self.pos = saved_pos
            text = self._collect_until_semi()
            return ParseNode(
                type=NodeType.DBMS_CALL,
                name=full_name,
                text=text,
                line=line,
            )

        # Procedure call (with optional args)
        if self.at(TokenType.LPAREN):
            # Restore to get full text
            self.pos = saved_pos
            text = self._collect_until_semi()
            return ParseNode(
                type=NodeType.PROCEDURE_CALL,
                name=full_name,
                text=text,
                line=line,
            )

        # Procedure call without args (just name;)
        self.match(TokenType.SEMI)
        return ParseNode(
            type=NodeType.PROCEDURE_CALL,
            name=full_name,
            text=full_name,
            line=line,
        )

    def at_any_keyword(self) -> bool:
        """Check if current token is any keyword (keywords can appear as identifiers in SQL)."""
        return self.peek().type in _KEYWORDS.values()

    def _parse_exception_block(self) -> ParseNode:
        """Parse EXCEPTION handler with WHEN clauses."""
        node = ParseNode(type=NodeType.EXCEPTION_BLOCK, line=self.peek().line)

        while self.at(TokenType.WHEN):
            when_node = self._parse_when_clause()
            node.children.append(when_node)

        return node

    def _parse_when_clause(self) -> ParseNode:
        """Parse WHEN exception THEN body."""
        line = self.peek().line
        self.expect(TokenType.WHEN)

        # Exception name: OTHERS, NO_DATA_FOUND, or custom
        if self.match(TokenType.OTHERS):
            exc_name = "OTHERS"
        elif self.at_keyword_seq("NO_DATA_FOUND"):
            self.advance()
            exc_name = "NO_DATA_FOUND"
        elif self.at(TokenType.ID):
            exc_name = self.advance().text
        else:
            exc_name = "UNKNOWN"

        self.expect(TokenType.THEN)

        # Parse body until next WHEN or END
        body = self._parse_body_statements()

        return ParseNode(
            type=NodeType.WHEN_CLAUSE,
            exception_name=exc_name,
            children=body,
            line=line,
        )


def parse_procedure(source: str) -> ParseNode:
    """Parse a PL/SQL procedure from source text.

    Args:
        source: Raw PL/SQL procedure text (from ProcedureInfo.raw_text)

    Returns:
        ParseNode tree rooted at a PROCEDURE node

    Raises:
        ParseError: If the procedure cannot be parsed
    """
    tokens = tokenize(source)
    parser = ProcedureParser(tokens, source)
    return parser.parse_procedure()
