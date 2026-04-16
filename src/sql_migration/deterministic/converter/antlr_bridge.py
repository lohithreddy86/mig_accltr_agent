"""
ANTLR Bridge Module

Entry point for the ANTLR parsing path. Implements a fallback chain:
  Tier 1: ANTLR4 grammar parser (if runtime available)
  Tier 2: Hand-rolled recursive descent parser (plsql_parser.py)

On failure of both, raises ParseError so main.py can fall back to regex path.
"""

from .plsql_splitter import ProcedureInfo
from .plsql_parser import parse_procedure as handrolled_parse, ParseError
from .antlr_to_ir import parse_tree_to_ir
from .ir import ProcedureIR

_ANTLR_AVAILABLE = False
try:
    from .grammar.visitor import antlr_parse
    _ANTLR_AVAILABLE = True
except ImportError:
    pass


def parse_procedure_to_ir(proc: ProcedureInfo, status_name: str = "") -> ProcedureIR:
    """Parse a PL/SQL procedure and convert to IR.

    Fallback chain: ANTLR parser → hand-rolled parser.

    Args:
        proc: ProcedureInfo from the splitter
        status_name: Status tracking name

    Returns:
        ProcedureIR with structured body

    Raises:
        ParseError: If neither parser can handle the procedure
    """
    source = proc.raw_text

    # Tier 1: ANTLR grammar parser
    if _ANTLR_AVAILABLE:
        try:
            tree = antlr_parse(source)
            return parse_tree_to_ir(tree, status_name)
        except Exception:
            pass  # Fall through to Tier 2

    # Tier 2: Hand-rolled recursive descent parser
    tree = handrolled_parse(source)
    return parse_tree_to_ir(tree, status_name)


# Re-export ParseError for main.py
__all__ = ['parse_procedure_to_ir', 'ParseError']
