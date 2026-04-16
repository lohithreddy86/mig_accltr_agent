"""
semantic.py
===========
Pydantic models for the A1.5 Semantic Analysis step.

The Pipeline Semantic Map is the central output — it captures what the
proc pipeline does at a business level, how data flows between procs,
which patterns are historical artifacts, and what the ideal target
PySpark/Trino architecture should look like.

Consumed by:
  - Planning Agent (P1): strategy selection informed by business logic
  - Planning Agent (P1.5): module grouping informed by pipeline stages
  - Conversion Agent (C2): per-chunk semantic context in the prompt
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class ProcSemanticEntry(BaseModel):
    """Semantic summary of what a single proc does."""
    proc_name:          str
    business_purpose:   str  = ""   # "Calculate daily compound interest"
    stage:              str  = ""   # "extraction" | "transformation" | "loading"
    inputs_description: str  = ""   # "Active accounts with balance > 0"
    outputs_description: str = ""   # "Daily interest amounts per account"
    key_business_rules: list[str] = Field(default_factory=list)
    # e.g. ["30/360 day count convention", "Tier boundaries at 1L, 5L, 10L"]
    historical_patterns: list[str] = Field(default_factory=list)
    # e.g. ["Cursor exists because Oracle 9i lacked MERGE — use JOIN in PySpark"]
    recommended_pyspark_approach: str = ""
    # e.g. "Single DataFrame pipeline with withColumn, no cursor needed"


class DataFlowEntry(BaseModel):
    """How a temp/intermediate table flows between procs."""
    table_name:     str
    produced_by:    str               = ""   # Proc name that writes it
    consumed_by:    list[str]         = Field(default_factory=list)
    nature:         str               = ""   # "intermediate" | "source" | "target"
    recommendation: str               = ""
    # e.g. "Convert to DataFrame variable — no need for temp table in Spark"


class PipelineStage(BaseModel):
    """A logical stage in the pipeline (group of procs with a shared purpose)."""
    stage_name:     str               # e.g. "extraction", "rate_calculation"
    description:    str               = ""
    procs:          list[str]         = Field(default_factory=list)
    inputs:         list[str]         = Field(default_factory=list)
    outputs:        list[str]         = Field(default_factory=list)


class TargetArchitecture(BaseModel):
    """Recommended PySpark/Trino architecture for the migrated pipeline."""
    summary:        str               = ""
    # e.g. "3 Spark jobs: extract → calculate → load"
    modules:        list[str]         = Field(default_factory=list)
    # Recommended module names
    rationale:      str               = ""
    design_notes:   list[str]         = Field(default_factory=list)
    # e.g. ["Collapse cursor-based procs 6-12 into a single DataFrame chain",
    #        "TMP_RATES can be a variable, not a temp view"]


class DiscoveredDependency(BaseModel):
    """A dependency discovered by the LLM that regex-based analysis missed."""
    name:           str
    called_by:      list[str]         = Field(default_factory=list)
    call_count:     int               = 0
    sample_lines:   list[int]         = Field(default_factory=list)
    resolution:     str               = "UNRESOLVED"
    # "UNRESOLVED" | "FOUND_IN_FILE:path" | "DIALECT_FUNCTION" | "SQL_BUILTIN"


class PipelineSemanticMap(BaseModel):
    """
    Complete semantic understanding of the source SQL pipeline.
    
    Written to artifacts/01_analysis/semantic_map.json
    Consumed by Planning Agent and Conversion Agent.
    """
    # Pipeline-level
    pipeline_intent:     str           = ""
    # "Calculate monthly compound interest for all active savings accounts"
    pipeline_complexity: str           = ""
    # "HIGH — 23 procs with tiered rate logic, cursor-based row processing,
    #  and multi-stage temp table chain"

    # Per-proc semantic summaries
    proc_summaries:      list[ProcSemanticEntry] = Field(default_factory=list)

    # Pipeline stages (logical grouping)
    stages:              list[PipelineStage]     = Field(default_factory=list)

    # Data flow through intermediate tables
    data_flow:           list[DataFlowEntry]     = Field(default_factory=list)

    # Target architecture recommendation
    target_architecture: TargetArchitecture      = Field(
        default_factory=TargetArchitecture
    )

    # v9: Dependencies discovered by LLM that regex missed
    # These are function calls found in source code that don't match any
    # proc in the manifest and aren't SQL keywords or dialect functions.
    # Fed into the Dependency Gate so the user is prompted before conversion.
    discovered_dependencies: list[DiscoveredDependency] = Field(default_factory=list)

    # Metadata
    procs_analyzed:      int           = 0
    procs_skipped:       list[str]     = Field(default_factory=list)
    analysis_notes:      str           = ""

    # ── Helper methods for downstream consumption ────────────────────────

    def get_proc_summary(self, proc_name: str) -> ProcSemanticEntry | None:
        return next(
            (p for p in self.proc_summaries if p.proc_name == proc_name), None
        )

    def get_stage_for_proc(self, proc_name: str) -> PipelineStage | None:
        return next(
            (s for s in self.stages if proc_name in s.procs), None
        )

    def context_for_conversion(self, proc_name: str) -> str:
        """
        Build a compact semantic context string for injection into
        the Conversion Agent's C2 prompt.

        Returns a human-readable block that tells the LLM:
          - What this proc does (business purpose)
          - Which pipeline stage it belongs to
          - What upstream procs produce and downstream procs expect
          - Historical patterns to avoid
          - Recommended PySpark approach
        """
        entry = self.get_proc_summary(proc_name)
        if not entry:
            return ""

        lines = [
            f"PIPELINE CONTEXT (from semantic analysis):",
            f"  Pipeline: {self.pipeline_intent}",
        ]

        stage = self.get_stage_for_proc(proc_name)
        if stage:
            lines.append(
                f"  Stage: {stage.stage_name} — {stage.description}"
            )

        lines.append(f"  This proc: {entry.business_purpose}")

        if entry.inputs_description:
            lines.append(f"  Inputs: {entry.inputs_description}")
        if entry.outputs_description:
            lines.append(f"  Outputs: {entry.outputs_description}")

        if entry.key_business_rules:
            lines.append(f"  Business rules:")
            for rule in entry.key_business_rules:
                lines.append(f"    - {rule}")

        if entry.historical_patterns:
            lines.append(f"  Historical patterns (DO NOT faithfully translate):")
            for pat in entry.historical_patterns:
                lines.append(f"    - {pat}")

        if entry.recommended_pyspark_approach:
            lines.append(
                f"  Recommended approach: {entry.recommended_pyspark_approach}"
            )

        # Data flow context — what intermediate tables connect to this proc
        relevant_flows = [
            df for df in self.data_flow
            if df.produced_by == proc_name
            or proc_name in df.consumed_by
        ]
        if relevant_flows:
            lines.append(f"  Data flow:")
            for df in relevant_flows:
                direction = "produces" if df.produced_by == proc_name else "consumes"
                lines.append(
                    f"    - {direction} {df.table_name}: {df.recommendation}"
                )

        return "\n".join(lines)

    def context_for_planning(self) -> str:
        """
        Build context string for the Planning Agent's P1 strategy selection.
        """
        lines = [
            f"PIPELINE SEMANTIC MAP:",
            f"  Intent: {self.pipeline_intent}",
            f"  Complexity: {self.pipeline_complexity}",
            f"  Stages: {len(self.stages)}",
        ]
        for stage in self.stages:
            lines.append(
                f"    {stage.stage_name}: {', '.join(stage.procs)} "
                f"— {stage.description}"
            )

        if self.target_architecture.summary:
            lines.append(
                f"  Target architecture: {self.target_architecture.summary}"
            )
            for note in self.target_architecture.design_notes:
                lines.append(f"    - {note}")

        return "\n".join(lines)
