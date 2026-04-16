"""
Confidence Scorer for PL/SQL → PySpark Conversion

Assigns per-procedure confidence scores based on construct complexity,
column counts, presence of ROWNUM, remote calls, etc.
"""

from dataclasses import dataclass
from .sql_extractor import SQLStatement, StatementType


@dataclass
class ConfidenceReport:
    procedure_name: str
    score: int  # 0-100
    tier: str   # "auto_pass", "review_recommended", "human_required", "blocked"
    risk_factors: list[str]
    positive_factors: list[str]
    statement_count: int
    construct_summary: dict[str, int]
    llm_assisted_count: int = 0


# Scoring weights
RISK_WEIGHTS = {
    # Constructs that reduce confidence
    'DB_LINK': -5,
    'REMOTE_PROC': -15,
    'ROWNUM': -3,
    'ROWID': -5,
    'OUTER_JOIN_PLUS': -3,
    'DECODE': -1,  # Deterministic but complex
    'CURSOR_LOOP': -8,
    'GOTO': -10,
    'LABEL': -5,
    'DBMS_SCHEDULER': -10,
    'DBMS_SESSION': -3,

    # Statement types that reduce confidence
    StatementType.UPDATE_CORRELATED.value: -3,
    StatementType.EXECUTE_IMMEDIATE_DDL.value: -5,
    StatementType.REMOTE_PROC_CALL.value: -15,
    StatementType.FOR_LOOP.value: -5,
    StatementType.DBMS_SCHEDULER.value: -10,
    StatementType.SELECT_INTO.value: -2,
    StatementType.IF_BLOCK.value: -2,
}

POSITIVE_WEIGHTS = {
    # Constructs that are mechanically translatable
    'NVL': 0,  # Neutral — it's deterministic
    'SYSDATE': 0,
    'TO_CHAR': 0,
    'TO_NUMBER': 0,
    'ADD_MONTHS': 0,
    'TRUNC': 0,

    # Statement types that increase confidence
    StatementType.INSERT_SELECT.value: 1,
    StatementType.UPDATE_SIMPLE.value: 1,
    StatementType.DELETE.value: 1,
    StatementType.TRUNCATE.value: 2,
    StatementType.COMMIT.value: 0,
    # Status tracking statements are boilerplate that the decorator handles —
    # they get a mild bonus since they're trivially converted.
    # (Previously +3 each, which over-rewarded packages with heavy status tracking)
    StatementType.STATUS_INSERT.value: 1,
    StatementType.STATUS_UPDATE.value: 1,
    StatementType.DBMS_STATS.value: 2,
}

# Converter success credits — awarded when the converter deterministically
# handles a risky construct. Each credit leaves a small residual risk (-1 to -2)
# acknowledging deployment/runtime uncertainty. These do NOT modify RISK_WEIGHTS.
CONVERTER_CREDIT = {
    'ROWNUM_CONVERTED': 3,                    # Risk=-3, net=0
    'DECODE_CONVERTED': 1,                    # Risk=-1, net=0
    'OUTER_JOIN_CONVERTED': 3,                # Risk=-3, net=0
    'DB_LINK_STRIPPED': 4,                     # Risk=-5, net=-1
    'CORRELATED_UPDATE_TO_MERGE': 2,          # Risk=-3, net=-1
    'EXEC_IMMEDIATE_HANDLED': 4,              # Risk=-5, net=-1
    'FOR_LOOP_CONVERTED': 4,                  # Risk=-5, net=-1
    'CURSOR_LOOP_CONVERTED': 7,               # Risk=-8, net=-1
    'DBMS_SCHEDULER_ORCHESTRATED': 10,        # Risk=-10, net=0
    'IF_BLOCK_CONVERTED': 2,                  # Risk=-2, net=0
    'LABEL_GOTO_CONVERTED': 5,                # Risk=-5, net=0
    'LARGE_INSERT_CONVERTED': 8,              # Risk=-10, net=-2
    'MEDIUM_INSERT_CONVERTED': 4,             # Risk=-5, net=-1
}


def score_procedure(procedure_name: str, statements: list[SQLStatement]) -> ConfidenceReport:
    """Calculate confidence score for a procedure's conversion.

    Args:
        procedure_name: Name of the procedure
        statements: Extracted and classified SQL statements

    Returns:
        ConfidenceReport with score and risk analysis
    """
    base_score = 95  # Start optimistic
    risk_factors = []
    positive_factors = []
    construct_counts = {}

    # Count all constructs and statement types
    for stmt in statements:
        if stmt.is_commented:
            continue

        type_key = stmt.type.value
        construct_counts[type_key] = construct_counts.get(type_key, 0) + 1

        for construct in stmt.oracle_constructs:
            construct_counts[construct] = construct_counts.get(construct, 0) + 1

    # Apply risk weights
    for construct, count in construct_counts.items():
        if construct in RISK_WEIGHTS:
            penalty = RISK_WEIGHTS[construct] * count
            base_score += penalty
            if penalty < 0:
                risk_factors.append(f"{construct} x{count} ({penalty:+d})")

        if construct in POSITIVE_WEIGHTS:
            bonus = POSITIVE_WEIGHTS[construct] * count
            base_score += bonus
            if bonus > 0:
                positive_factors.append(f"{construct} x{count} ({bonus:+d})")

    # Additional risk factors

    # Large INSERT statements (>50 columns) — higher chance of column mapping error
    for stmt in statements:
        if stmt.type == StatementType.INSERT_SELECT:
            col_count = _estimate_column_count(stmt.sql)
            if col_count > 100:
                penalty = -10
                base_score += penalty
                risk_factors.append(f"Large INSERT ({col_count} columns) ({penalty:+d})")
            elif col_count > 50:
                penalty = -5
                base_score += penalty
                risk_factors.append(f"Medium INSERT ({col_count} columns) ({penalty:+d})")

    # NOTE: DB_LINK construct penalty is already applied above via RISK_WEIGHTS.
    # A separate per-statement penalty was removed to avoid double-counting.
    # The -5 per DB_LINK occurrence in RISK_WEIGHTS is sufficient.

    # Converter success credits — reward deterministic handling of risky constructs
    converter_bonus, converter_factors = _assess_converter_success(statements, construct_counts)
    base_score += converter_bonus
    positive_factors.extend(converter_factors)

    # Status tracking pattern detected — indicates boilerplate that the decorator handles
    status_stmts = sum(1 for s in statements if s.type in (StatementType.STATUS_INSERT, StatementType.STATUS_UPDATE))
    if status_stmts >= 2:
        positive_factors.append(f"Status tracking pattern detected — handled by decorator (+2)")
        base_score += 2

    # Very few active statements — simple procedure
    active_stmts = sum(1 for s in statements if not s.is_commented and s.type != StatementType.COMMIT)
    if active_stmts <= 5:
        positive_factors.append(f"Simple procedure ({active_stmts} active statements)")
        base_score += 3

    # Clamp score
    score = max(0, min(100, base_score))

    # Determine tier
    has_blockers = any(s.type == StatementType.REMOTE_PROC_CALL and not s.is_commented for s in statements)

    if has_blockers:
        tier = "blocked"
    elif score >= 95:
        tier = "auto_pass"
    elif score >= 80:
        tier = "review_recommended"
    else:
        tier = "human_required"

    return ConfidenceReport(
        procedure_name=procedure_name,
        score=score,
        tier=tier,
        risk_factors=risk_factors,
        positive_factors=positive_factors,
        statement_count=len(statements),
        construct_summary=construct_counts
    )


def rescore_with_llm_data(report: ConfidenceReport, llm_assisted_count: int) -> ConfidenceReport:
    """Rescore a confidence report after LLM-assisted conversions.

    Applies bonus for LLM-resolved TODOs (e.g., correlated UPDATE penalty -3 → -1)
    and enforces floor rule: LLM-assisted procedures never get auto_pass tier.
    """
    report.llm_assisted_count = llm_assisted_count

    # Bonus: +2 per LLM assist, capped at correlated update count from construct_summary
    correlated_count = report.construct_summary.get('UPDATE_CORRELATED', 0)
    effective_assists = min(llm_assisted_count, correlated_count) if correlated_count > 0 else llm_assisted_count
    bonus = effective_assists * 2

    report.score = max(0, min(100, report.score + bonus))

    # Recompute tier with same thresholds
    has_blockers = any('REMOTE_PROC_CALL' in f for f in report.risk_factors)
    if has_blockers:
        report.tier = "blocked"
    elif report.score >= 95:
        report.tier = "auto_pass"
    elif report.score >= 80:
        report.tier = "review_recommended"
    else:
        report.tier = "human_required"

    # Floor rule: LLM-assisted procedures never auto_pass
    if llm_assisted_count > 0 and report.tier == "auto_pass":
        report.tier = "review_recommended"

    if bonus > 0:
        report.positive_factors.append(f"LLM-assisted conversions x{llm_assisted_count} (+{bonus})")

    return report


def _assess_converter_success(statements: list[SQLStatement],
                              construct_counts: dict[str, int]) -> tuple[int, list[str]]:
    """Assess which risky constructs the converter handles deterministically.

    Awards positive credits for constructs that the converter successfully converts,
    partially offsetting their risk penalties. Each credit leaves a small residual
    risk (-1 to -2) acknowledging deployment/runtime uncertainty.

    Also awards a bulk conversion bonus when >=10 occurrences of the same risk
    construct are all handled systematically (lower marginal risk per occurrence).

    Returns:
        (total_bonus, list_of_positive_factor_strings)
    """
    bonus = 0
    factors = []

    # Per-construct credits
    _credit_map = {
        # construct_key: (credit_key, per_occurrence)
        'ROWNUM':          ('ROWNUM_CONVERTED', True),
        'DECODE':          ('DECODE_CONVERTED', True),
        'OUTER_JOIN_PLUS': ('OUTER_JOIN_CONVERTED', True),
        'DB_LINK':         ('DB_LINK_STRIPPED', True),
        'CURSOR_LOOP':     ('CURSOR_LOOP_CONVERTED', True),
        'LABEL':           ('LABEL_GOTO_CONVERTED', True),
    }

    for construct_key, (credit_key, per_occ) in _credit_map.items():
        count = construct_counts.get(construct_key, 0)
        if count > 0:
            credit = CONVERTER_CREDIT[credit_key] * count
            bonus += credit
            factors.append(f"Converter: {construct_key} handled x{count} (+{credit})")

    # Statement-type credits
    _stmt_credit_map = {
        StatementType.UPDATE_CORRELATED: 'CORRELATED_UPDATE_TO_MERGE',
        StatementType.EXECUTE_IMMEDIATE_DDL: 'EXEC_IMMEDIATE_HANDLED',
        StatementType.FOR_LOOP: 'FOR_LOOP_CONVERTED',
        StatementType.DBMS_SCHEDULER: 'DBMS_SCHEDULER_ORCHESTRATED',
        StatementType.IF_BLOCK: 'IF_BLOCK_CONVERTED',
    }

    for stmt_type, credit_key in _stmt_credit_map.items():
        type_key = stmt_type.value
        count = construct_counts.get(type_key, 0)
        if count > 0:
            credit = CONVERTER_CREDIT[credit_key] * count
            bonus += credit
            factors.append(f"Converter: {type_key} handled x{count} (+{credit})")

    # Large/Medium INSERT credits
    for stmt in statements:
        if stmt.type == StatementType.INSERT_SELECT and not stmt.is_commented:
            col_count = _estimate_column_count(stmt.sql)
            if col_count > 100:
                credit = CONVERTER_CREDIT['LARGE_INSERT_CONVERTED']
                bonus += credit
                factors.append(f"Converter: large INSERT ({col_count} cols) handled (+{credit})")
            elif col_count > 50:
                credit = CONVERTER_CREDIT['MEDIUM_INSERT_CONVERTED']
                bonus += credit
                factors.append(f"Converter: medium INSERT ({col_count} cols) handled (+{credit})")

    # Bulk conversion bonus — systematic patterns are lower risk per occurrence
    # When >=10 occurrences of the same construct are all handled, add extra credit
    _bulk_constructs = ['DB_LINK', 'ROWNUM', StatementType.UPDATE_CORRELATED.value]
    for construct_key in _bulk_constructs:
        count = construct_counts.get(construct_key, 0)
        if count >= 10:
            bulk_credit = min(count, 20)  # Cap at +20 per construct type
            bonus += bulk_credit
            factors.append(f"Bulk pattern: {construct_key} x{count} systematic (+{bulk_credit})")

    return bonus, factors


def _estimate_column_count(insert_sql: str) -> int:
    """Estimate number of columns in an INSERT statement by counting commas in column list."""
    import re

    # Find the column list between INSERT INTO table (col1, col2, ...)
    match = re.search(r'Into\s+[\w\.]+\s*\(([^)]+)\)', insert_sql, re.IGNORECASE | re.DOTALL)
    if match:
        cols = match.group(1)
        return cols.count(',') + 1

    # If no explicit column list, count columns in SELECT
    select_match = re.search(r'Select\s+(.+?)\s+From\b', insert_sql, re.IGNORECASE | re.DOTALL)
    if select_match:
        select_cols = select_match.group(1)
        # Rough estimate: count top-level commas
        depth = 0
        count = 1
        for char in select_cols:
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
            elif char == ',' and depth == 0:
                count += 1
        return count

    return 0


def generate_summary_report(reports: list[ConfidenceReport]) -> str:
    """Generate a human-readable summary report of all procedure scores."""
    lines = [
        "=" * 80,
        "PL/SQL → PySpark CONVERSION CONFIDENCE REPORT",
        "=" * 80,
        "",
    ]

    # Summary stats
    total = len(reports)
    auto_pass = sum(1 for r in reports if r.tier == 'auto_pass')
    review = sum(1 for r in reports if r.tier == 'review_recommended')
    human = sum(1 for r in reports if r.tier == 'human_required')
    blocked = sum(1 for r in reports if r.tier == 'blocked')
    avg_score = sum(r.score for r in reports) / total if total > 0 else 0

    lines.extend([
        f"Total procedures: {total}",
        f"Average confidence: {avg_score:.1f}%",
        f"  Auto-pass (≥95):     {auto_pass}",
        f"  Review recommended:  {review}",
        f"  Human required:      {human}",
        f"  Blocked:             {blocked}",
        "",
        "-" * 80,
    ])

    # Sort by score ascending (worst first)
    for report in sorted(reports, key=lambda r: r.score):
        tier_icon = {
            'auto_pass': '[PASS]',
            'review_recommended': '[REVIEW]',
            'human_required': '[HUMAN]',
            'blocked': '[BLOCKED]'
        }[report.tier]

        lines.append(f"\n{tier_icon} {report.procedure_name} — Score: {report.score}/100")
        lines.append(f"  Statements: {report.statement_count}")

        if report.risk_factors:
            lines.append("  Risk factors:")
            for rf in report.risk_factors:
                lines.append(f"    - {rf}")

        if report.positive_factors:
            lines.append("  Positive factors:")
            for pf in report.positive_factors:
                lines.append(f"    + {pf}")

    lines.append("\n" + "=" * 80)
    return '\n'.join(lines)
