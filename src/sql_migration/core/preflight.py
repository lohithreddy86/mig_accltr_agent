"""
preflight.py
=============
Pre-flight summary and anti-hallucination risk assessment.

Runs after Planning Agent (P4) completes, before Orchestrator starts.
Produces a human-readable summary of what the pipeline will do, along
with a risk assessment that flags procs likely to hallucinate.

This is NOT about cost saving.  It is about:
  1. Giving the developer a chance to catch misconfigurations BEFORE
     burning through 100+ LLM calls
  2. Flagging high-hallucination-risk procs so the developer can
     pre-provide context (UDF bodies, write mode instructions, etc.)
  3. Surfacing the write-conflict and dependency gate results in one view

Design rationale:
  Every LLM call to a large proc that WILL fail is a hallucination
  opportunity.  The LLM doesn't know it will fail — it generates
  plausible but wrong code, the system retries, the LLM generates
  different plausible but wrong code, and after 9 attempts the proc
  freezes.  Preflight assessment identifies these procs in advance.

Anti-hallucination principle:
  "An ounce of prevention is worth a pound of correction loops."

Artifact written:
  artifacts/02_planning/preflight_summary.json

Integration:
  Called by main.py after planning_agent.run() and before orchestrator.run().
  In Streamlit: displayed on the Pipeline Monitor page before "Start" button.
  In CLI: printed to stdout, --dry-run stops here.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from sql_migration.core.artifact_store import ArtifactStore
from sql_migration.core.config_loader import get_config
from sql_migration.core.logger import get_logger

log = get_logger("preflight")


@dataclass
class ProcRisk:
    """Hallucination risk assessment for one proc."""
    proc_name: str
    strategy: str
    chunk_count: int
    risk_level: str = "LOW"  # LOW, MEDIUM, HIGH, CRITICAL
    risk_factors: list[str] = field(default_factory=list)
    mitigation_suggestions: list[str] = field(default_factory=list)
    estimated_llm_calls: int = 0


@dataclass
class PreflightSummary:
    """Complete pre-flight assessment."""
    run_id: str = ""
    # Scale
    total_procs: int = 0
    total_chunks: int = 0
    total_lines: int = 0
    # Strategy distribution
    strategy_counts: dict[str, int] = field(default_factory=dict)
    # Estimated LLM calls
    estimated_conversion_calls: int = 0
    estimated_correction_calls: int = 0    # worst-case self-corrections
    estimated_validation_calls: int = 0
    estimated_total_calls: int = 0
    # Risk assessment
    proc_risks: list[ProcRisk] = field(default_factory=list)
    critical_count: int = 0
    high_count: int = 0
    medium_count: int = 0
    # Blockers
    has_write_conflicts: bool = False
    write_conflict_tables: list[str] = field(default_factory=list)
    has_unresolved_deps: bool = False
    unresolved_dep_names: list[str] = field(default_factory=list)
    has_cycles: bool = False
    # Recommendations
    recommendations: list[str] = field(default_factory=list)
    # Metadata
    assessed_at: str = ""

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "scale": {
                "total_procs": self.total_procs,
                "total_chunks": self.total_chunks,
                "total_lines": self.total_lines,
            },
            "strategy_distribution": self.strategy_counts,
            "estimated_llm_calls": {
                "conversion": self.estimated_conversion_calls,
                "self_correction_worst_case": self.estimated_correction_calls,
                "validation": self.estimated_validation_calls,
                "total_estimated": self.estimated_total_calls,
            },
            "risk_assessment": {
                "critical": self.critical_count,
                "high": self.high_count,
                "medium": self.medium_count,
                "proc_risks": [
                    {
                        "proc": r.proc_name,
                        "strategy": r.strategy,
                        "chunks": r.chunk_count,
                        "risk": r.risk_level,
                        "factors": r.risk_factors,
                        "mitigations": r.mitigation_suggestions,
                    }
                    for r in self.proc_risks
                    if r.risk_level in ("HIGH", "CRITICAL")
                ],
            },
            "blockers": {
                "write_conflicts": self.write_conflict_tables,
                "unresolved_deps": self.unresolved_dep_names,
                "cycles": self.has_cycles,
            },
            "recommendations": self.recommendations,
            "assessed_at": self.assessed_at,
        }


class PreflightAssessor:
    """
    Assess pipeline risk and produce a pre-flight summary.
    """

    def __init__(self, store: ArtifactStore) -> None:
        self.store = store
        self.cfg = get_config()

    def assess(self, run_id: str) -> PreflightSummary:
        """Run full pre-flight assessment."""
        summary = PreflightSummary(
            run_id=run_id,
            assessed_at=datetime.now(timezone.utc).isoformat(),
        )

        # Load artifacts
        plan = self._load("planning", "plan.json")
        manifest = self._load("analysis", "manifest.json")
        complexity = self._load("analysis", "complexity_report.json")
        dep_graph = self._load("analysis", "dependency_graph.json")
        construct_map = self._load("analysis", "construct_map.json")

        if not plan or not manifest:
            summary.recommendations.append(
                "Could not load plan or manifest — run Detection and Analysis first."
            )
            return summary

        # Scale metrics
        procs = plan.get("procs", {})
        summary.total_procs = len(procs)
        summary.total_chunks = sum(
            len(p.get("chunks", [])) for p in procs.values()
        )
        summary.total_lines = sum(
            p.get("line_count", 0) for p in manifest.get("procs", [])
        )

        # Strategy distribution
        for p in procs.values():
            strat = p.get("strategy", "UNKNOWN")
            summary.strategy_counts[strat] = summary.strategy_counts.get(strat, 0) + 1

        # LLM call estimation
        max_corrections = self.cfg.conversion.max_chunk_self_corrections
        max_replans = self.cfg.planning.loop_guards.frozen_after

        summary.estimated_conversion_calls = summary.total_chunks
        summary.estimated_correction_calls = summary.total_chunks * max_corrections
        summary.estimated_validation_calls = summary.total_procs  # V4 calls
        summary.estimated_total_calls = (
            summary.estimated_conversion_calls
            + summary.estimated_correction_calls
            + summary.estimated_validation_calls
            + 2  # D5 + A5
        )

        # Write conflicts
        write_conflicts = self._load("planning", "write_conflicts.json")
        if write_conflicts:
            summary.has_write_conflicts = True
            summary.write_conflict_tables = list(write_conflicts.keys())

        # Unresolved deps
        dep_resolution = self._load("analysis", "dependency_resolution.json")
        external_deps = dep_graph.get("external_deps", {}) if dep_graph else {}
        if external_deps:
            all_deps = set()
            for deps in external_deps.values():
                all_deps.update(deps)
            summary.has_unresolved_deps = True
            summary.unresolved_dep_names = sorted(all_deps)

        # Cycles
        if dep_graph and dep_graph.get("has_cycles"):
            summary.has_cycles = True

        # Per-proc risk assessment
        unmapped = set(construct_map.get("unmapped", [])) if construct_map else set()
        complexity_by_name = {}
        if complexity:
            for s in complexity.get("scores", []):
                complexity_by_name[s["name"]] = s

        # Load external dependency flags from dialect_profile
        dp = self._load("analysis", "dialect_profile.json")
        external_flags = {
            "has_db_links":        dp.get("has_db_links", False) if dp else False,
            "has_external_calls":  dp.get("has_external_calls", False) if dp else False,
            "has_file_io":         dp.get("has_file_io", False) if dp else False,
            "has_scheduler_calls": dp.get("has_scheduler_calls", False) if dp else False,
        }

        for proc_name, proc_plan in procs.items():
            risk = self._assess_proc_risk(
                proc_name=proc_name,
                proc_plan=proc_plan,
                manifest=manifest,
                complexity_entry=complexity_by_name.get(proc_name, {}),
                unmapped=unmapped,
                has_write_conflict=proc_name in [
                    p for writers in (write_conflicts or {}).values()
                    for p in writers
                ],
                has_unresolved=bool(external_deps.get(proc_name)),
                external_flags=external_flags,
            )
            summary.proc_risks.append(risk)

        summary.critical_count = sum(
            1 for r in summary.proc_risks if r.risk_level == "CRITICAL"
        )
        summary.high_count = sum(
            1 for r in summary.proc_risks if r.risk_level == "HIGH"
        )
        summary.medium_count = sum(
            1 for r in summary.proc_risks if r.risk_level == "MEDIUM"
        )

        # Generate recommendations
        self._generate_recommendations(summary)

        # Persist
        self.store.write(
            "planning",
            "preflight_summary.json",
            summary.to_dict(),
        )

        log.info(
            "preflight_complete",
            total_procs=summary.total_procs,
            total_chunks=summary.total_chunks,
            estimated_calls=summary.estimated_total_calls,
            critical=summary.critical_count,
            high=summary.high_count,
        )

        return summary

    def _assess_proc_risk(
        self,
        proc_name: str,
        proc_plan: dict,
        manifest: dict,
        complexity_entry: dict,
        unmapped: set,
        has_write_conflict: bool,
        has_unresolved: bool,
        external_flags: dict | None = None,
    ) -> ProcRisk:
        """Assess hallucination risk for one proc."""
        strategy = proc_plan.get("strategy", "UNKNOWN")
        chunks = proc_plan.get("chunks", [])
        max_corrections = self.cfg.conversion.max_chunk_self_corrections

        risk = ProcRisk(
            proc_name=proc_name,
            strategy=strategy,
            chunk_count=len(chunks),
            estimated_llm_calls=len(chunks) * (1 + max_corrections),
        )

        proc_entry = next(
            (p for p in manifest.get("procs", []) if p["name"] == proc_name),
            {},
        )
        # v7: In module mode, proc_name is a module name — resolve via source_procs
        if not proc_entry:
            source_procs = proc_plan.get("source_procs", [])
            if source_procs:
                # Merge flags from all member procs (conservative: any True → True)
                member_entries = [
                    p for p in manifest.get("procs", [])
                    if p.get("name") in set(source_procs)
                ]
                proc_entry = {}
                for me in member_entries:
                    proc_entry["has_dynamic_sql"] = (
                        proc_entry.get("has_dynamic_sql", False)
                        or me.get("has_dynamic_sql", False)
                    )
                    proc_entry["has_unsupported"] = list(set(
                        proc_entry.get("has_unsupported", [])
                        + me.get("has_unsupported", [])
                    ))
                    # Use worst-case cursor nesting
                    me_depth = me.get("cursor_nesting_depth") or 0
                    cur_depth = proc_entry.get("cursor_nesting_depth") or 0
                    if me_depth > cur_depth:
                        proc_entry["cursor_nesting_depth"] = me_depth
        score = 0  # Risk score accumulator

        # Factor: complexity
        comp_score = complexity_entry.get("score", "LOW")
        if comp_score == "NEEDS_MANUAL":
            score += 40
            risk.risk_factors.append(f"Complexity: NEEDS_MANUAL")
            risk.mitigation_suggestions.append(
                "Consider providing manual rewrite for this proc"
            )
        elif comp_score == "HIGH":
            score += 20
            risk.risk_factors.append(f"Complexity: HIGH")

        # Factor: chunk count (more chunks = more context loss opportunities)
        if len(chunks) > 4:
            score += 15
            risk.risk_factors.append(
                f"Multi-chunk ({len(chunks)} chunks) — cross-chunk context loss risk"
            )

        # Factor: unresolved dependencies
        if has_unresolved:
            score += 25
            risk.risk_factors.append(
                "Has unresolved external dependencies — LLM may hallucinate function behavior"
            )
            risk.mitigation_suggestions.append(
                "Upload missing UDF definitions or provide inline function bodies"
            )

        # Factor: write conflicts
        if has_write_conflict:
            score += 20
            risk.risk_factors.append(
                "Writes to a shared table — write mode hallucination risk"
            )

        # Factor: dynamic SQL
        if proc_entry.get("has_dynamic_sql"):
            score += 15
            risk.risk_factors.append(
                "Contains dynamic SQL — LLM cannot determine runtime behavior"
            )

        # Factor: cursor nesting
        nesting = proc_entry.get("cursor_nesting_depth")
        if nesting and nesting > 2:
            score += 10
            risk.risk_factors.append(
                f"Deep cursor nesting (depth {nesting}) — complex row-by-row logic"
            )

        # Factor: unmapped constructs
        unmapped_in_proc = proc_entry.get("has_unsupported", [])
        if len(unmapped_in_proc) > 3:
            score += 15
            risk.risk_factors.append(
                f"{len(unmapped_in_proc)} unsupported constructs"
            )

        # Factor: external dependencies (DB links, external calls, file I/O, scheduler)
        flags = external_flags or {}
        if flags.get("has_db_links"):
            score += 15
            risk.risk_factors.append("Source uses DB links / remote DB calls")
        if flags.get("has_external_calls"):
            score += 10
            risk.risk_factors.append("Source uses external system calls")
        if flags.get("has_file_io"):
            score += 10
            risk.risk_factors.append("Source uses file I/O operations")
        if flags.get("has_scheduler_calls"):
            score += 5
            risk.risk_factors.append("Source uses scheduler / job calls")

        # Classify risk level (thresholds from config)
        pf_cfg = self.cfg.preflight
        if score >= pf_cfg.critical_threshold:
            risk.risk_level = "CRITICAL"
        elif score >= pf_cfg.high_threshold:
            risk.risk_level = "HIGH"
        elif score >= pf_cfg.medium_threshold:
            risk.risk_level = "MEDIUM"
        else:
            risk.risk_level = "LOW"

        return risk

    def _generate_recommendations(self, summary: PreflightSummary) -> None:
        """Generate actionable recommendations."""
        if summary.critical_count > 0:
            summary.recommendations.append(
                f"⚠️ {summary.critical_count} proc(s) have CRITICAL hallucination risk. "
                f"Review their risk factors before starting the pipeline."
            )

        if summary.has_unresolved_deps:
            summary.recommendations.append(
                f"📎 {len(summary.unresolved_dep_names)} unresolved external dependencies: "
                f"{', '.join(summary.unresolved_dep_names[:5])}. "
                f"Upload missing files or confirm they should be TODO stubs."
            )

        if summary.has_write_conflicts:
            summary.recommendations.append(
                f"📝 {len(summary.write_conflict_tables)} table(s) written by multiple procs: "
                f"{', '.join(summary.write_conflict_tables[:5])}. "
                f"Write semantics analysis will determine safe write modes."
            )

        if summary.has_cycles:
            summary.recommendations.append(
                "🔄 Circular dependencies detected. Affected procs will be marked NEEDS_MANUAL."
            )

        if summary.total_chunks > 50:
            summary.recommendations.append(
                f"📊 Large job: {summary.total_chunks} chunks across {summary.total_procs} procs. "
                f"Estimated {summary.estimated_total_calls} LLM calls (worst case with all corrections)."
            )

    def _load(self, agent: str, filename: str) -> dict | None:
        """Load an artifact, returning None if missing."""
        try:
            return self.store.read(agent, filename)
        except Exception:
            return None