"""
Intermediate Representation (IR) for PL/SQL → PySpark Conversion

Defines a tree-structured IR that both the regex path and the ANTLR path
produce. The IR code generator walks this tree to emit PySpark Python code.
"""

from dataclasses import dataclass, field
from enum import Enum


class IRNodeType(Enum):
    PROCEDURE = "PROCEDURE"
    SQL_STATEMENT = "SQL_STATEMENT"
    VARIABLE_DECL = "VARIABLE_DECL"
    VARIABLE_ASSIGNMENT = "VARIABLE_ASSIGNMENT"
    IF_BLOCK = "IF_BLOCK"
    ELSIF_BRANCH = "ELSIF_BRANCH"
    ELSE_BRANCH = "ELSE_BRANCH"
    FOR_CURSOR_LOOP = "FOR_CURSOR_LOOP"
    WHILE_LOOP = "WHILE_LOOP"
    BEGIN_END_BLOCK = "BEGIN_END_BLOCK"
    EXCEPTION_HANDLER = "EXCEPTION_HANDLER"
    WHEN_CLAUSE = "WHEN_CLAUSE"
    GOTO = "GOTO"
    LABEL = "LABEL"
    PROCEDURE_CALL = "PROCEDURE_CALL"
    EXECUTE_IMMEDIATE = "EXECUTE_IMMEDIATE"
    COMMIT = "COMMIT"
    ROLLBACK = "ROLLBACK"
    STATUS_TRACKING = "STATUS_TRACKING"
    COMMENT_BLOCK = "COMMENT_BLOCK"
    DBMS_STATS = "DBMS_STATS"
    DBMS_SCHEDULER = "DBMS_SCHEDULER"
    DBMS_SESSION = "DBMS_SESSION"
    UNKNOWN = "UNKNOWN"


@dataclass
class SQLInfo:
    """Metadata about a SQL statement within an IR node."""
    raw_sql: str
    statement_type: str  # matches StatementType.value
    target_table: str | None = None
    source_tables: list[str] = field(default_factory=list)
    oracle_constructs: list[str] = field(default_factory=list)
    is_correlated_update: bool = False
    has_db_link: bool = False
    has_hints: bool = False
    is_commented: bool = False
    alias_map: dict[str, str] = field(default_factory=dict)


@dataclass
class CursorLoopInfo:
    """Metadata for FOR cursor loops."""
    cursor_variable: str          # e.g., "i", "Lr_Data"
    select_sql: str
    field_references: list[str] = field(default_factory=list)


@dataclass
class VariableInfo:
    """Variable declaration metadata."""
    name: str
    var_type: str
    default_value: str | None = None


@dataclass
class IRNode:
    """A single node in the IR tree."""
    node_type: IRNodeType
    children: list['IRNode'] = field(default_factory=list)
    sql_info: SQLInfo | None = None
    cursor_loop_info: CursorLoopInfo | None = None
    condition: str | None = None       # For IF/ELSIF/WHILE
    label_name: str | None = None      # For LABEL/GOTO
    variable_name: str | None = None   # For VARIABLE_ASSIGNMENT
    variable_value: str | None = None  # For VARIABLE_ASSIGNMENT
    raw_text: str | None = None        # Fallback raw text
    source_line_start: int = 0
    source_line_end: int = 0
    conversion_path: str = ""          # "regex" or "antlr"


@dataclass
class ProcedureIR:
    """Top-level IR for a single procedure."""
    name: str
    parameters: list[str] = field(default_factory=list)
    variables: list[VariableInfo] = field(default_factory=list)
    body: list[IRNode] = field(default_factory=list)
    exception_handler: IRNode | None = None
    is_status_tracked: bool = False
    status_name: str = ""
    has_step_tracking: bool = False
    conversion_path: str = ""  # "regex" or "antlr"
