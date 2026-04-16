"""
interchange.py
==============
Data contract between the deterministic converter and the agentic pipeline.

The ConversionArtifact is the handoff object: the converter produces it,
the Conversion Agent consumes it. This decouples the two systems —
the converter can evolve independently as long as it fills this contract.

SCOPE GUARD: This module defines data structures only. It does not import
or call any agentic system code. It does not affect the TRINO_SQL path.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any


class ConversionRoute(str, Enum):
    """Route decision for a procedure based on deterministic confidence."""
    DETERMINISTIC_ACCEPT = "DETERMINISTIC_ACCEPT"       # confidence >= 80, skip LLM
    DETERMINISTIC_THEN_REPAIR = "DETERMINISTIC_THEN_REPAIR"  # confidence 50-79, LLM fixes TODOs
    AGENTIC_FULL = "AGENTIC_FULL"                       # confidence < 50 or parse failure
    NOT_APPLICABLE = "NOT_APPLICABLE"                   # Not a PySpark strategy — SQL-target strategies bypass this


@dataclass
class TodoItem:
    """A TODO left in the converter's output that needs agent or human attention."""
    line: int
    comment: str
    original_sql: str = ""
    construct_type: str = ""   # UNMAPPED_CONSTRUCT, DYNAMIC_SQL, REMOTE_CALL, etc.


@dataclass
class PreprocessedSQL:
    """Result of deterministic Oracle SQL preprocessing."""
    original_sql: str
    transpiled_sql: str
    transformations_applied: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    constructs_remaining: list[str] = field(default_factory=list)


@dataclass
class StatementClassification:
    """Classification metadata for a single SQL statement."""
    statement_type: str     # One of 23 StatementType values
    sql: str
    target_table: str = ""
    source_tables: list[str] = field(default_factory=list)
    oracle_constructs: list[str] = field(default_factory=list)
    has_db_link: bool = False
    is_correlated_update: bool = False


@dataclass
class ConversionArtifact:
    """
    Complete handoff artifact from deterministic converter to agentic system.

    This is the primary integration point. The converter fills this;
    the Conversion Agent reads it to decide whether to accept, repair, or
    fall through to full agentic conversion.
    """
    proc_name: str
    unit_type: str = "procedure"  # "procedure" or "function"
    original_plsql: str = ""
    generated_pyspark: str = ""
    confidence_score: int = 0      # 0-100
    confidence_tier: str = ""      # auto_pass, review_recommended, human_required, blocked
    route: ConversionRoute = ConversionRoute.AGENTIC_FULL

    # Residual work for the agent
    todos: list[TodoItem] = field(default_factory=list)
    oracle_constructs_remaining: list[str] = field(default_factory=list)

    # Metadata for agent context
    conversion_path: str = ""      # regex, antlr, antlr-fallback, parse_failure
    warnings: list[str] = field(default_factory=list)
    tables_read: list[str] = field(default_factory=list)
    tables_written: list[str] = field(default_factory=list)
    db_links_stripped: list[str] = field(default_factory=list)
    statement_classifications: list[StatementClassification] = field(default_factory=list)

    # Preprocessed SQL (for AGENTIC_FULL route — agent gets pre-transpiled input)
    preprocessed_sql: str = ""

    @property
    def needs_agent(self) -> bool:
        """Whether this artifact requires agent intervention."""
        return self.route in (
            ConversionRoute.DETERMINISTIC_THEN_REPAIR,
            ConversionRoute.AGENTIC_FULL,
        )

    @property
    def todo_count(self) -> int:
        return len(self.todos)

    def to_dict(self) -> dict[str, Any]:
        """Serialize for JSON storage in artifact store."""
        d = asdict(self)
        d["route"] = self.route.value
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ConversionArtifact:
        """Deserialize from JSON."""
        data = dict(data)
        data["route"] = ConversionRoute(data.get("route", "AGENTIC_FULL"))
        data["todos"] = [TodoItem(**t) if isinstance(t, dict) else t for t in data.get("todos", [])]
        data["statement_classifications"] = [
            StatementClassification(**s) if isinstance(s, dict) else s
            for s in data.get("statement_classifications", [])
        ]
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


def compute_route(confidence_score: int, confidence_tier: str,
                  todo_count: int, parse_failed: bool) -> ConversionRoute:
    """Determine the conversion route from deterministic confidence.

    Args:
        confidence_score: 0-100 from converter's confidence_scorer
        confidence_tier: auto_pass, review_recommended, human_required, blocked
        todo_count: Number of unresolved TODOs in converter output
        parse_failed: Whether the converter's parser failed on this procedure

    Returns:
        ConversionRoute indicating how the agentic system should handle this proc
    """
    if parse_failed:
        return ConversionRoute.AGENTIC_FULL

    # ACCEPT when either (a) perfect score + no TODOs, or (b) near-perfect score
    # with a handful of isolated TODOs (remote proc calls, dbms_* etc.) — those
    # are surfaced as `# TODO: ...` comments and the semantic-review stage runs
    # regardless, so we don't miss silent bugs. Empirically, Oracle procs in
    # this codebase land at 95-100 on sqlglot+IR; locking ACCEPT behind
    # `todos == 0` sent every proc with a single cross-DB call into a 10-call
    # agentic loop.
    if confidence_score >= 95 and todo_count <= 3:
        return ConversionRoute.DETERMINISTIC_ACCEPT
    if confidence_score >= 80 and todo_count == 0:
        return ConversionRoute.DETERMINISTIC_ACCEPT

    if confidence_score >= 50:
        return ConversionRoute.DETERMINISTIC_THEN_REPAIR

    return ConversionRoute.AGENTIC_FULL
