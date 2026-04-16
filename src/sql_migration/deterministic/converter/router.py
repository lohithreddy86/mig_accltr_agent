"""
Conversion Path Router

Analyzes each procedure's complexity signature and selects
the appropriate conversion path (regex or ANTLR).
"""

import re
from dataclasses import dataclass
from enum import Enum


class ConversionPath(Enum):
    REGEX = "regex"
    ANTLR = "antlr"


@dataclass
class ComplexitySignature:
    """Complexity features extracted from raw procedure text."""
    nested_begin_depth: int = 0
    has_for_loop: bool = False
    has_goto: bool = False
    has_cursor_var_refs: bool = False
    correlated_update_count: int = 0
    dynamic_sql_count: int = 0
    exception_block_count: int = 0
    has_label: bool = False
    line_count: int = 0


def analyze_complexity(procedure_text: str) -> ComplexitySignature:
    """Analyze a procedure's raw text to determine its complexity signature.

    Args:
        procedure_text: The full raw text of the procedure (from ProcedureInfo.raw_text)

    Returns:
        ComplexitySignature with feature counts
    """
    sig = ComplexitySignature()
    lines = procedure_text.split('\n')
    sig.line_count = len(lines)

    # Nested BEGIN depth
    max_depth = 0
    depth = 0
    for line in lines:
        stripped = line.strip().upper()
        if re.match(r'^BEGIN\b', stripped):
            depth += 1
            max_depth = max(max_depth, depth)
        elif re.match(r'^END\b', stripped):
            depth = max(0, depth - 1)
    sig.nested_begin_depth = max_depth

    # FOR cursor loop
    sig.has_for_loop = bool(re.search(
        r'\bFor\s+\w+\s+In\s+\(',
        procedure_text, re.IGNORECASE
    ))

    # GOTO
    sig.has_goto = bool(re.search(r'\bGoto\s+\w+', procedure_text, re.IGNORECASE))

    # Labels
    sig.has_label = bool(re.search(r'<<\w+>>', procedure_text))

    # Cursor variable references (e.g., i.Column_Name inside FOR loop)
    if sig.has_for_loop:
        # Extract cursor variable name
        for_match = re.search(r'\bFor\s+(\w+)\s+In\s+\(', procedure_text, re.IGNORECASE)
        if for_match:
            var = for_match.group(1)
            # Check if cursor_var.field_name appears in the body
            sig.has_cursor_var_refs = bool(re.search(
                rf'\b{re.escape(var)}\.\w+',
                procedure_text, re.IGNORECASE
            ))

    # Correlated UPDATEs: UPDATE ... SET ... = (SELECT ...)
    sig.correlated_update_count = len(re.findall(
        r'\bUpdate\b.+?\bSet\b.+?\(\s*Select\b',
        procedure_text, re.IGNORECASE | re.DOTALL
    ))

    # Dynamic SQL: EXECUTE IMMEDIATE with variable (not literal string)
    # Count EXECUTE IMMEDIATE var_name (no quote after EXECUTE IMMEDIATE)
    exec_matches = re.findall(
        r'Execute\s+Immediate\s+(\S+)',
        procedure_text, re.IGNORECASE
    )
    sig.dynamic_sql_count = sum(
        1 for m in exec_matches if not m.startswith("'")
    )

    # Exception blocks
    sig.exception_block_count = len(re.findall(
        r'\bException\b',
        procedure_text, re.IGNORECASE
    ))

    return sig


def select_path(sig: ComplexitySignature,
                force_path: ConversionPath | None = None) -> ConversionPath:
    """Select the conversion path based on complexity signature.

    Args:
        sig: Complexity analysis results
        force_path: Override path selection (for CLI --path flag)

    Returns:
        ConversionPath.REGEX or ConversionPath.ANTLR
    """
    if force_path:
        return force_path

    is_simple = (
        sig.nested_begin_depth <= 1
        and not sig.has_for_loop
        and not sig.has_goto
        and not sig.has_cursor_var_refs
        and sig.correlated_update_count <= 3
        and sig.dynamic_sql_count <= 1
        and sig.exception_block_count <= 1
    )
    return ConversionPath.REGEX if is_simple else ConversionPath.ANTLR
