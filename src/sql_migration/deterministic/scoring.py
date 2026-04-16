"""
scoring.py
==========
Unified confidence scoring across deterministic and agent conversion paths.

Wraps the converter's confidence_scorer with agent-specific credits.
Ensures both paths are scored on the same 0-100 scale.

SCOPE GUARD: This module does not affect TRINO_SQL validation or scoring.
It is used ONLY for PySpark conversion confidence.
"""

from __future__ import annotations

from .interchange import ConversionArtifact, ConversionRoute


# Agent-specific credits — awarded when the agent resolves a TODO
# that the converter left behind.
AGENT_CREDITS = {
    "CORRELATED_UPDATE": 2,    # Agent resolved a correlated UPDATE TODO
    "DYNAMIC_SQL": 3,          # Agent resolved dynamic SQL
    "CURSOR_LOOP": 2,          # Agent resolved a cursor loop
    "REMOTE_CALL": 1,          # Agent stubbed a remote call (still needs review)
    "SCHEDULER": 2,            # Agent converted scheduler job
    "UNCLASSIFIED": 1,         # Agent resolved an unclassified statement
    "UNMAPPED_CONSTRUCT": 1,   # Agent resolved an unmapped construct
}

# Floor rule: agent-assisted procedures max out at review_recommended
AGENT_ASSISTED_MAX_TIER = "review_recommended"


def rescore_with_agent_results(artifact: ConversionArtifact,
                                resolved_todo_types: list[str]) -> ConversionArtifact:
    """Re-score a ConversionArtifact after agent intervention.

    Args:
        artifact: The original artifact from the deterministic converter
        resolved_todo_types: List of construct_type values for TODOs the agent fixed

    Returns:
        Updated artifact with adjusted confidence score and tier
    """
    if not resolved_todo_types:
        return artifact

    # Calculate agent bonus
    bonus = sum(
        AGENT_CREDITS.get(todo_type, 1)
        for todo_type in resolved_todo_types
    )

    # Apply bonus (capped at 100)
    new_score = min(100, artifact.confidence_score + bonus)

    # Determine new tier
    if new_score >= 95:
        new_tier = "auto_pass"
    elif new_score >= 80:
        new_tier = "review_recommended"
    else:
        new_tier = "human_required"

    # Floor rule: agent-assisted procedures never get auto_pass
    if new_tier == "auto_pass" and len(resolved_todo_types) > 0:
        new_tier = AGENT_ASSISTED_MAX_TIER

    # Update artifact
    artifact.confidence_score = new_score
    artifact.confidence_tier = new_tier

    # Remove resolved TODOs from the list
    remaining_todos = [
        t for t in artifact.todos
        if t.construct_type not in resolved_todo_types
    ]
    artifact.todos = remaining_todos

    return artifact


def compute_hybrid_summary(artifacts: list[ConversionArtifact]) -> dict:
    """Compute aggregate quality metrics across all artifacts.

    Returns a summary dict suitable for logging and reporting.
    """
    total = len(artifacts)
    if total == 0:
        return {"total": 0}

    deterministic_accepted = sum(
        1 for a in artifacts if a.route == ConversionRoute.DETERMINISTIC_ACCEPT
    )
    deterministic_repaired = sum(
        1 for a in artifacts if a.route == ConversionRoute.DETERMINISTIC_THEN_REPAIR
    )
    agentic_full = sum(
        1 for a in artifacts if a.route == ConversionRoute.AGENTIC_FULL
    )
    not_applicable = sum(
        1 for a in artifacts if a.route == ConversionRoute.NOT_APPLICABLE
    )

    avg_confidence = sum(a.confidence_score for a in artifacts) / total
    total_todos = sum(a.todo_count for a in artifacts)

    auto_pass = sum(1 for a in artifacts if a.confidence_tier == "auto_pass")
    review_recommended = sum(1 for a in artifacts if a.confidence_tier == "review_recommended")
    human_required = sum(1 for a in artifacts if a.confidence_tier == "human_required")
    blocked = sum(1 for a in artifacts if a.confidence_tier == "blocked")

    return {
        "total_procedures": total,
        "routing": {
            "deterministic_accepted": deterministic_accepted,
            "deterministic_repaired": deterministic_repaired,
            "agentic_full": agentic_full,
            "not_applicable_trino": not_applicable,
        },
        "confidence": {
            "average_score": round(avg_confidence, 1),
            "auto_pass": auto_pass,
            "review_recommended": review_recommended,
            "human_required": human_required,
            "blocked": blocked,
        },
        "quality": {
            "total_todos_remaining": total_todos,
            "procs_with_zero_todos": sum(1 for a in artifacts if a.todo_count == 0),
            "deterministic_coverage_pct": round(
                (deterministic_accepted + deterministic_repaired) / max(1, total - not_applicable) * 100, 1
            ),
        },
    }
