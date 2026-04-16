"""
SQL Statement Extractor and Classifier

Extracts individual SQL statements from PL/SQL procedure bodies and classifies
them by type for targeted translation.
"""

import re
from dataclasses import dataclass
from enum import Enum


class StatementType(Enum):
    INSERT_SELECT = "INSERT_SELECT"
    INSERT_VALUES = "INSERT_VALUES"
    UPDATE_CORRELATED = "UPDATE_CORRELATED"
    UPDATE_SIMPLE = "UPDATE_SIMPLE"
    DELETE = "DELETE"
    TRUNCATE = "TRUNCATE"
    EXECUTE_IMMEDIATE_DDL = "EXECUTE_IMMEDIATE_DDL"
    DBMS_STATS = "DBMS_STATS"
    DBMS_SCHEDULER = "DBMS_SCHEDULER"
    DBMS_SESSION = "DBMS_SESSION"
    REMOTE_PROC_CALL = "REMOTE_PROC_CALL"
    SELECT_INTO = "SELECT_INTO"
    COMMIT = "COMMIT"
    ROLLBACK = "ROLLBACK"
    STATUS_INSERT = "STATUS_INSERT"
    STATUS_UPDATE = "STATUS_UPDATE"
    VARIABLE_ASSIGNMENT = "VARIABLE_ASSIGNMENT"
    IF_BLOCK = "IF_BLOCK"
    FOR_LOOP = "FOR_LOOP"
    GOTO = "GOTO"
    LABEL = "LABEL"
    COMMENT_BLOCK = "COMMENT_BLOCK"
    BEGIN_END_BLOCK = "BEGIN_END_BLOCK"
    UNKNOWN = "UNKNOWN"


@dataclass
class SQLStatement:
    type: StatementType
    sql: str
    line_start: int
    line_end: int
    oracle_constructs: list[str]
    target_table: str | None = None
    source_tables: list[str] | None = None
    is_commented: bool = False
    has_db_link: bool = False
    has_hints: bool = False


def extract_statements(procedure_body: str) -> list[SQLStatement]:
    """Extract and classify SQL statements from a procedure body.

    Args:
        procedure_body: The executable body text of a PL/SQL procedure

    Returns:
        List of classified SQLStatement objects
    """
    lines = procedure_body.split('\n')
    statements = []

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Skip empty lines
        if not line:
            i += 1
            continue

        # Skip single-line comments
        if line.startswith('--'):
            i += 1
            continue

        # Multi-line comment blocks
        if line.startswith('/*'):
            comment_start = i
            comment_text = [lines[i]]
            while i < len(lines) and '*/' not in lines[i]:
                i += 1
                if i < len(lines):
                    comment_text.append(lines[i])
            # Check if comment contains SQL (commented-out code)
            full_comment = '\n'.join(comment_text)
            if any(kw in full_comment.upper() for kw in ['INSERT', 'UPDATE', 'DELETE', 'SELECT']):
                statements.append(SQLStatement(
                    type=StatementType.COMMENT_BLOCK,
                    sql=full_comment,
                    line_start=comment_start + 1,
                    line_end=i + 1,
                    oracle_constructs=[],
                    is_commented=True
                ))
            i += 1
            continue

        # COMMIT
        if re.match(r'^Commit\s*;', line, re.IGNORECASE):
            statements.append(SQLStatement(
                type=StatementType.COMMIT,
                sql='COMMIT;',
                line_start=i + 1,
                line_end=i + 1,
                oracle_constructs=['COMMIT']
            ))
            i += 1
            continue

        # ROLLBACK
        if re.match(r'^Rollback\s*;', line, re.IGNORECASE):
            statements.append(SQLStatement(
                type=StatementType.ROLLBACK,
                sql='ROLLBACK;',
                line_start=i + 1,
                line_end=i + 1,
                oracle_constructs=['ROLLBACK']
            ))
            i += 1
            continue

        # Variable assignment (v_Err := ..., v_Step_No := ...)
        if re.match(r'^v_\w+\s*:=', line, re.IGNORECASE) or re.match(r'^\w+\s*:=', line, re.IGNORECASE):
            stmt_text, end_i = _collect_statement(lines, i)
            statements.append(SQLStatement(
                type=StatementType.VARIABLE_ASSIGNMENT,
                sql=stmt_text,
                line_start=i + 1,
                line_end=end_i + 1,
                oracle_constructs=_detect_constructs(stmt_text)
            ))
            i = end_i + 1
            continue

        # Labels (<<label_name>>)
        if re.match(r'^<<\w+>>', line):
            statements.append(SQLStatement(
                type=StatementType.LABEL,
                sql=line,
                line_start=i + 1,
                line_end=i + 1,
                oracle_constructs=['LABEL']
            ))
            i += 1
            continue

        # GOTO
        if re.match(r'^Goto\s+\w+', line, re.IGNORECASE):
            statements.append(SQLStatement(
                type=StatementType.GOTO,
                sql=line,
                line_start=i + 1,
                line_end=i + 1,
                oracle_constructs=['GOTO']
            ))
            i += 1
            continue

        # EXECUTE IMMEDIATE
        if re.match(r"^Execute\s+Immediate", line, re.IGNORECASE):
            stmt_text, end_i = _collect_statement(lines, i)
            target = _extract_truncate_target(stmt_text)
            statements.append(SQLStatement(
                type=StatementType.TRUNCATE if 'truncate' in stmt_text.lower() else StatementType.EXECUTE_IMMEDIATE_DDL,
                sql=stmt_text,
                line_start=i + 1,
                line_end=end_i + 1,
                oracle_constructs=_detect_constructs(stmt_text),
                target_table=target
            ))
            i = end_i + 1
            continue

        # DBMS_STATS
        if re.match(r'^Dbms_Stats', line, re.IGNORECASE):
            stmt_text, end_i = _collect_statement(lines, i)
            statements.append(SQLStatement(
                type=StatementType.DBMS_STATS,
                sql=stmt_text,
                line_start=i + 1,
                line_end=end_i + 1,
                oracle_constructs=['DBMS_STATS']
            ))
            i = end_i + 1
            continue

        # DBMS_SESSION
        if re.match(r'^Dbms_Session', line, re.IGNORECASE):
            stmt_text, end_i = _collect_statement(lines, i)
            statements.append(SQLStatement(
                type=StatementType.DBMS_SESSION,
                sql=stmt_text,
                line_start=i + 1,
                line_end=end_i + 1,
                oracle_constructs=['DBMS_SESSION']
            ))
            i = end_i + 1
            continue

        # Remote procedure call (schema.proc@dblink)
        if re.match(r'^[\w\.]+@\w+', line, re.IGNORECASE):
            stmt_text, end_i = _collect_statement(lines, i)
            statements.append(SQLStatement(
                type=StatementType.REMOTE_PROC_CALL,
                sql=stmt_text,
                line_start=i + 1,
                line_end=end_i + 1,
                oracle_constructs=['DB_LINK', 'REMOTE_PROC'],
                has_db_link=True
            ))
            i = end_i + 1
            continue

        # IF blocks
        if re.match(r'^If\s+', line, re.IGNORECASE):
            block_text, end_i = _collect_if_block(lines, i)
            statements.append(SQLStatement(
                type=StatementType.IF_BLOCK,
                sql=block_text,
                line_start=i + 1,
                line_end=end_i + 1,
                oracle_constructs=_detect_constructs(block_text)
            ))
            i = end_i + 1
            continue

        # FOR loops
        if re.match(r'^For\s+', line, re.IGNORECASE):
            block_text, end_i = _collect_block(lines, i, 'FOR', 'END LOOP')
            statements.append(SQLStatement(
                type=StatementType.FOR_LOOP,
                sql=block_text,
                line_start=i + 1,
                line_end=end_i + 1,
                oracle_constructs=_detect_constructs(block_text) + ['CURSOR_LOOP']
            ))
            i = end_i + 1
            continue

        # BEGIN...END blocks (nested)
        if re.match(r'^Begin\s*$', line, re.IGNORECASE):
            block_text, end_i = _collect_begin_end(lines, i)
            # Check if it contains DBMS_SCHEDULER
            if 'dbms_scheduler' in block_text.lower():
                statements.append(SQLStatement(
                    type=StatementType.DBMS_SCHEDULER,
                    sql=block_text,
                    line_start=i + 1,
                    line_end=end_i + 1,
                    oracle_constructs=['DBMS_SCHEDULER']
                ))
            else:
                statements.append(SQLStatement(
                    type=StatementType.BEGIN_END_BLOCK,
                    sql=block_text,
                    line_start=i + 1,
                    line_end=end_i + 1,
                    oracle_constructs=_detect_constructs(block_text)
                ))
            i = end_i + 1
            continue

        # INSERT statements
        if re.match(r'^Insert\s+', line, re.IGNORECASE):
            stmt_text, end_i = _collect_statement(lines, i)
            target = _extract_insert_target(stmt_text)
            is_status = _is_status_tracking(stmt_text)

            if is_status:
                stype = StatementType.STATUS_INSERT
            elif 'values' in stmt_text.lower() and 'select' not in stmt_text.lower():
                stype = StatementType.INSERT_VALUES
            else:
                stype = StatementType.INSERT_SELECT

            statements.append(SQLStatement(
                type=stype,
                sql=stmt_text,
                line_start=i + 1,
                line_end=end_i + 1,
                oracle_constructs=_detect_constructs(stmt_text),
                target_table=target,
                source_tables=_extract_source_tables(stmt_text),
                has_db_link=bool(re.search(r'@\w+', stmt_text)),
                has_hints=bool(re.search(r'/\*\+', stmt_text))
            ))
            i = end_i + 1
            continue

        # UPDATE statements
        if re.match(r'^Update\s+', line, re.IGNORECASE):
            stmt_text, end_i = _collect_statement(lines, i)
            target = _extract_update_target(stmt_text)
            is_status = _is_status_tracking(stmt_text)

            # Detect correlated vs simple update
            has_subquery = bool(re.search(r'\(\s*Select\s+', stmt_text, re.IGNORECASE))

            if is_status:
                stype = StatementType.STATUS_UPDATE
            elif has_subquery:
                stype = StatementType.UPDATE_CORRELATED
            else:
                stype = StatementType.UPDATE_SIMPLE

            statements.append(SQLStatement(
                type=stype,
                sql=stmt_text,
                line_start=i + 1,
                line_end=end_i + 1,
                oracle_constructs=_detect_constructs(stmt_text),
                target_table=target,
                source_tables=_extract_source_tables(stmt_text),
                has_db_link=bool(re.search(r'@\w+', stmt_text)),
                has_hints=bool(re.search(r'/\*\+', stmt_text))
            ))
            i = end_i + 1
            continue

        # DELETE statements
        if re.match(r'^Delete\s+', line, re.IGNORECASE):
            stmt_text, end_i = _collect_statement(lines, i)
            target = _extract_delete_target(stmt_text)
            statements.append(SQLStatement(
                type=StatementType.DELETE,
                sql=stmt_text,
                line_start=i + 1,
                line_end=end_i + 1,
                oracle_constructs=_detect_constructs(stmt_text),
                target_table=target,
                source_tables=_extract_source_tables(stmt_text)
            ))
            i = end_i + 1
            continue

        # SELECT INTO
        if re.match(r'^Select\s+', line, re.IGNORECASE):
            stmt_text, end_i = _collect_statement(lines, i)
            statements.append(SQLStatement(
                type=StatementType.SELECT_INTO,
                sql=stmt_text,
                line_start=i + 1,
                line_end=end_i + 1,
                oracle_constructs=_detect_constructs(stmt_text),
                source_tables=_extract_source_tables(stmt_text)
            ))
            i = end_i + 1
            continue

        # Procedure/function calls (local) — generic detection:
        # Any identifier followed by optional (args) and ; that isn't a SQL keyword
        if re.match(r'^[A-Za-z]\w*(?:\.\w+)*\s*(?:\(|;)', line) and \
           not re.match(r'^(?:Insert|Update|Delete|Select|Truncate|Execute|Dbms_|Begin|End|If|For|While|Commit|Rollback|Goto|Null)\b', line, re.IGNORECASE):
            stmt_text, end_i = _collect_statement(lines, i)
            statements.append(SQLStatement(
                type=StatementType.REMOTE_PROC_CALL if '@' in stmt_text else StatementType.UNKNOWN,
                sql=stmt_text,
                line_start=i + 1,
                line_end=end_i + 1,
                oracle_constructs=['LOCAL_PROC_CALL'],
                has_db_link='@' in stmt_text
            ))
            i = end_i + 1
            continue

        # Catch-all: try to collect as a statement
        if line and not line.startswith('--'):
            stmt_text, end_i = _collect_statement(lines, i)
            statements.append(SQLStatement(
                type=StatementType.UNKNOWN,
                sql=stmt_text,
                line_start=i + 1,
                line_end=end_i + 1,
                oracle_constructs=_detect_constructs(stmt_text)
            ))
            i = end_i + 1
            continue

        i += 1

    return statements


def _collect_statement(lines: list[str], start: int) -> tuple[str, int]:
    """Collect a multi-line SQL statement ending with semicolon."""
    stmt_lines = [lines[start]]
    i = start

    # Check if first line ends with semicolon
    if lines[start].rstrip().endswith(';'):
        return lines[start].strip(), start

    i += 1
    depth = 0
    while i < len(lines):
        line = lines[i].strip()
        stmt_lines.append(lines[i])

        # Track parentheses depth
        depth += line.count('(') - line.count(')')

        if line.endswith(';') and depth <= 0:
            break
        i += 1

    return '\n'.join(l for l in stmt_lines), min(i, len(lines) - 1)


def _collect_if_block(lines: list[str], start: int) -> tuple[str, int]:
    """Collect an IF...END IF block, handling nesting."""
    block_lines = [lines[start]]
    i = start + 1
    depth = 1  # We're inside one IF

    while i < len(lines):
        line = lines[i].strip().upper()
        block_lines.append(lines[i])

        if re.match(r'^IF\s+', line):
            depth += 1
        elif re.match(r'^END\s+IF\s*;', line):
            depth -= 1
            if depth == 0:
                break
        i += 1

    return '\n'.join(block_lines), min(i, len(lines) - 1)


def _collect_block(lines: list[str], start: int, open_kw: str, close_kw: str) -> tuple[str, int]:
    """Collect a block delimited by open/close keywords."""
    block_lines = [lines[start]]
    i = start + 1
    depth = 1

    while i < len(lines):
        line = lines[i].strip().upper()
        block_lines.append(lines[i])

        if re.match(rf'^{open_kw}\s+', line):
            depth += 1
        elif re.match(rf'^{close_kw}\s*;', line):
            depth -= 1
            if depth == 0:
                break
        i += 1

    return '\n'.join(block_lines), min(i, len(lines) - 1)


def _collect_begin_end(lines: list[str], start: int) -> tuple[str, int]:
    """Collect a BEGIN...END block, handling nesting and exceptions."""
    block_lines = [lines[start]]
    i = start + 1
    depth = 1

    while i < len(lines):
        line = lines[i].strip()
        line_upper = line.upper()
        block_lines.append(lines[i])

        if re.match(r'^BEGIN\b', line_upper):
            depth += 1
        elif re.match(r'^END\s*;', line_upper) or re.match(r'^END\s+\w+', line_upper):
            depth -= 1
            if depth == 0:
                break
        i += 1

    return '\n'.join(block_lines), min(i, len(lines) - 1)


def _detect_constructs(sql: str) -> list[str]:
    """Detect Oracle-specific constructs in a SQL statement."""
    constructs = []
    sql_upper = sql.upper()

    patterns = {
        'NVL': r'\bNVL\s*\(',
        'NVL2': r'\bNVL2\s*\(',
        'DECODE': r'\bDECODE\s*\(',
        'SYSDATE': r'\bSYSDATE\b',
        'TRUNC': r'\bTRUNC\s*\(',
        'TO_CHAR': r'\bTO_CHAR\s*\(',
        'TO_DATE': r'\bTO_DATE\s*\(',
        'TO_NUMBER': r'\bTO_NUMBER\s*\(',
        'ADD_MONTHS': r'\bADD_MONTHS\s*\(',
        'MONTHS_BETWEEN': r'\bMONTHS_BETWEEN\s*\(',
        'LAST_DAY': r'\bLAST_DAY\s*\(',
        'INSTR': r'\bINSTR\s*\(',
        'SUBSTR': r'\bSUBSTR\s*\(',
        'ROWNUM': r'\bROWNUM\b',
        'ROWID': r'\bROWID\b',
        'OUTER_JOIN_PLUS': r'\(\+\)',
        'DB_LINK': r'@\w+',
        'PARALLEL_HINT': r'/\*\+.*?parallel',
        'APPEND_HINT': r'/\*\+.*?APPEND',
        'DUAL': r'\bFROM\s+DUAL\b',
        'LEAD': r'\bLEAD\s*\(',
        'LAG': r'\bLAG\s*\(',
        'MINUS': r'\bMINUS\b',
    }

    for name, pattern in patterns.items():
        if re.search(pattern, sql_upper if name != 'DB_LINK' else sql, re.IGNORECASE):
            constructs.append(name)

    return constructs


def _extract_insert_target(sql: str) -> str | None:
    """Extract target table from INSERT statement."""
    match = re.search(r'Insert\s+(?:/\*.*?\*/)?\s*Into\s+([\w\.]+)', sql, re.IGNORECASE)
    return match.group(1) if match else None


def _extract_update_target(sql: str) -> str | None:
    """Extract target table from UPDATE statement."""
    match = re.search(r'Update\s+(?:/\*.*?\*/)?\s*([\w\.]+)', sql, re.IGNORECASE)
    return match.group(1) if match else None


def _extract_delete_target(sql: str) -> str | None:
    """Extract target table from DELETE statement."""
    match = re.search(r'Delete\s+From\s+([\w\.]+)', sql, re.IGNORECASE)
    return match.group(1) if match else None


def _extract_truncate_target(sql: str) -> str | None:
    """Extract target table from TRUNCATE statement."""
    match = re.search(r"truncate\s+table\s+([\w\.]+)", sql, re.IGNORECASE)
    return match.group(1) if match else None


def _extract_source_tables(sql: str) -> list[str]:
    """Extract source tables from FROM and JOIN clauses."""
    tables = []

    # FROM clause tables
    for match in re.finditer(r'\bFrom\s+([\w\.@]+)', sql, re.IGNORECASE):
        table = match.group(1)
        # Remove db link suffix for table name
        clean = re.sub(r'@\w+', '', table)
        if clean.upper() != 'DUAL':
            tables.append(clean)

    # JOIN tables
    for match in re.finditer(r'\bJoin\s+([\w\.@]+)', sql, re.IGNORECASE):
        table = match.group(1)
        clean = re.sub(r'@\w+', '', table)
        tables.append(clean)

    # Comma-separated tables in FROM (e.g., FROM a, b)
    from_match = re.search(r'\bFrom\s+(.+?)(?:\bWhere\b|\bGroup\b|\bOrder\b|\bHaving\b|$)', sql, re.IGNORECASE | re.DOTALL)
    if from_match:
        from_clause = from_match.group(1)
        for part in from_clause.split(','):
            table_match = re.match(r'\s*([\w\.@]+)', part.strip())
            if table_match:
                clean = re.sub(r'@\w+', '', table_match.group(1))
                if clean.upper() != 'DUAL' and clean not in tables:
                    tables.append(clean)

    return list(set(tables))


# Module-level configurable status table name.
# Detected from input at runtime (see main.py _detect_status_table),
# or set explicitly. If None, status tracking detection is disabled.
_status_table_pattern: str | None = None


def set_status_table_pattern(pattern: str | None):
    """Set the status tracking table name pattern for classification.

    Called by the pipeline after scanning the input to auto-detect the
    status table, or set to None to disable status tracking detection.
    """
    global _status_table_pattern
    _status_table_pattern = pattern


def _is_status_tracking(sql: str) -> bool:
    """Check if this is a status tracking statement.

    Uses the auto-detected or configured status table pattern.
    Falls back to a generic heuristic.

    IMPORTANT: The heuristic must match the INSERT/UPDATE TARGET TABLE name,
    not column names or literals inside the body. Without this distinction,
    a business INSERT with 300 columns (some named Lac_Status, etc.) is
    falsely classified as status-tracking and the entire statement is dropped.
    """
    if _status_table_pattern:
        return bool(re.search(re.escape(_status_table_pattern), sql, re.IGNORECASE))

    # Extract the actual target table from the INSERT/UPDATE
    target_match = re.search(
        r'(?:Insert\s+(?:/\*.*?\*/)?\s*Into|Update)\s+([\w\.]+)',
        sql, re.IGNORECASE)
    if not target_match:
        return False
    target_table = target_match.group(1).lower()

    # Only match if the TARGET TABLE name itself contains 'status'
    if 'status' not in target_table:
        return False

    # Additional guard: status tracking INSERTs use VALUES (small),
    # not INSERT-SELECT (large business queries). A 50+ line INSERT is
    # almost certainly not status tracking.
    line_count = sql.count('\n') + 1
    if line_count > 30:
        return False

    has_status_values = bool(re.search(r"Status\s*=\s*'[YN]'", sql, re.IGNORECASE))
    has_tracking_cols = bool(re.search(r'\b(?:Start_Date|End_Date|Dwld_Date)\b', sql, re.IGNORECASE))
    return has_status_values or has_tracking_cols


def get_statement_summary(statements: list[SQLStatement]) -> dict:
    """Generate a summary of statement types and constructs."""
    type_counts = {}
    construct_counts = {}
    db_link_count = 0
    hint_count = 0

    for stmt in statements:
        type_name = stmt.type.value
        type_counts[type_name] = type_counts.get(type_name, 0) + 1

        for construct in stmt.oracle_constructs:
            construct_counts[construct] = construct_counts.get(construct, 0) + 1

        if stmt.has_db_link:
            db_link_count += 1
        if stmt.has_hints:
            hint_count += 1

    return {
        'total_statements': len(statements),
        'type_counts': type_counts,
        'construct_counts': construct_counts,
        'db_link_statements': db_link_count,
        'hint_statements': hint_count,
        'tables_written': list(set(s.target_table for s in statements if s.target_table)),
        'tables_read': list(set(t for s in statements if s.source_tables for t in s.source_tables))
    }
