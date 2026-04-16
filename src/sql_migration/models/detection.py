"""
detection.py
============
Pydantic I/O schemas for dialect detection and adapter configuration.

v10: Detection Agent removed. These models are now used by:
  - Analysis Agent A0 setup phase (README parsing, adapter loading)
  - Planning Agent (reads dialect_profile, construct_map)
  - Conversion Agent (reads construct_map for per-chunk hints)
  - Orchestrator (reads dialect_profile for pipeline config)

Models preserved:
  - ReadmeSignals       — parsed README.docx fields
  - DialectAdapter      — persisted adapter JSON (loaded from registry or LLM-generated)
  - DialectProfile      — pipeline-wide dialect config artifact
  - ConstructMap         — fn→Trino/PySpark lookup for conversion prompts
  - AdapterSeedProcBoundary, AdapterSeedTransactionPatterns — adapter sub-models
  - AdapterRegistryDescription — lightweight adapter summary for A0 LLM tool

Models removed (were D2-D6 specific):
  - CodeSignals, SqlglotScores, LLMClassificationInput/Output,
    AdapterSeed, DetectionAgentInput/Output, and all D5 prompt builders
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# README parsing output (formerly D1)
# ---------------------------------------------------------------------------

class ReadmeSignals(BaseModel):
    """Structured signals extracted from the user's README.docx."""

    # Section 1 — Source System Overview
    source_engine:        str       = ""   # e.g. "Oracle 19c"
    declared_dialect:     str       = ""   # e.g. "PL/SQL"
    business_domain:      str       = ""
    sme_contact:          str       = ""
    source_type:          str       = "stored_procedures"  # stored_procedures | sql_queries | ssis_package

    # Section 2 — Procedure Structure
    proc_start_example:   str       = ""   # Raw example from Section 2.1
    proc_end_example:     str       = ""
    exception_keyword:    str       = ""   # e.g. "EXCEPTION WHEN"
    transaction_begin:    str       = ""
    transaction_commit:   str       = ""
    transaction_rollback: str       = ""
    script_delimiter:     str       = ""
    has_packages:         bool      = False
    dynamic_sql_keyword:  str       = ""

    # Section 3 — Dialect Functions
    custom_functions:        list[dict[str, str]] = Field(default_factory=list)
    # Each dict: {name, what_it_does, example, suggested_equivalent}
    problematic_constructs:  list[dict[str, str]] = Field(default_factory=list)
    # Each dict: {construct, why_tricky, frequency, suggested_handling}

    # Section 3.2 — Pre-written UDF implementations
    udf_implementations:     list[dict[str, str]] = Field(default_factory=list)
    # Each dict: {name, code}

    # Section 4 — Tables & Catalog/Schema
    source_schemas:        list[str]        = Field(default_factory=list)
    temp_table_pattern:    str              = ""
    table_mapping:         dict[str, str]   = Field(default_factory=dict)
    # {source_table: trino_target_table}
    input_catalog:         str              = ""
    input_schema:          list[str]        = Field(default_factory=list)
    output_catalog:        str              = ""
    output_schema:         list[str]        = Field(default_factory=list)

    # Desired output format: AUTO, TRINO_SQL, or PYSPARK
    output_format:         str              = "AUTO"

    # Section 6 — Proc I/O tables
    proc_io_tables:        dict[str, dict[str, Any]] = Field(default_factory=dict)
    # {proc_name: {input_tables: [...], output_table: "...", notes: "..."}}

    # Section 7 — Exclusions
    procs_to_skip:         list[str]        = Field(default_factory=list)
    has_db_links:          bool             = False
    has_external_calls:    bool             = False
    has_file_io:           bool             = False
    has_scheduler_calls:   bool             = False

    # Computed quality score (set by parse_readme.py after extraction)
    quality_score:         float            = 0.0

    @property
    def function_names(self) -> list[str]:
        """Flat list of just function names from custom_functions."""
        return [f.get("name", "") for f in self.custom_functions if f.get("name")]

    @property
    def problematic_construct_names(self) -> list[str]:
        return [c.get("construct", "") for c in self.problematic_constructs if c.get("construct")]


# ---------------------------------------------------------------------------
# Adapter sub-models
# ---------------------------------------------------------------------------

class AdapterSeedProcBoundary(BaseModel):
    """Proc boundary patterns within a DialectAdapter."""
    start_pattern:      str
    end_pattern:        str       = ""
    statement_terminator: str     = ""
    start_patterns:     list[str] = Field(default_factory=list)  # v10: exhaustive list for docs
    name_capture_group: int       = 1


class AdapterSeedTransactionPatterns(BaseModel):
    """Transaction control patterns within a DialectAdapter."""
    begin:     str = ""
    commit:    str = ""
    rollback:  str = ""
    end_block: str = r"\bEND\b"


# ---------------------------------------------------------------------------
# Full Adapter model (persisted to adapters/{dialect}.json)
# ---------------------------------------------------------------------------

class DialectAdapter(BaseModel):
    """
    Complete persisted adapter for a source dialect.
    Loaded from registry (hand-crafted) or generated by A0 LLM.
    Validated against actual source code during A0.
    """
    dialect_id:            str
    dialect_display:       str

    proc_boundary:         AdapterSeedProcBoundary
    cursor_pattern:        str              = ""
    exception_pattern:     str              = ""
    transaction_patterns:  AdapterSeedTransactionPatterns = Field(
        default_factory=AdapterSeedTransactionPatterns
    )
    dynamic_sql_patterns:  list[str]        = Field(default_factory=list)
    dialect_functions:     list[str]        = Field(default_factory=list)
    trino_mappings:        dict[str, str | None] = Field(default_factory=dict)
    pyspark_mappings:      dict[str, str | None] = Field(default_factory=dict)
    type_mappings:         dict[str, str]        = Field(default_factory=dict)  # e.g. NUMBER→DECIMAL
    unsupported_constructs: list[str]       = Field(default_factory=list)
    temp_table_pattern:    str              = ""

    # Quality metadata
    confidence:            float            = 0.0
    hand_crafted:          bool             = False   # True for manually written adapters
    generated_at:          str              = ""
    validated:             bool             = False


# ---------------------------------------------------------------------------
# Dialect profile (pipeline-wide config)
# ---------------------------------------------------------------------------

class DialectProfile(BaseModel):
    """
    Pipeline-wide dialect configuration.
    Written to artifacts/00_analysis/dialect_profile.json.
    Read by Planning, Orchestrator, Conversion agents.
    """
    # Identity
    dialect_id:       str
    dialect_display:  str
    confidence:       float
    resolution:       str = ""    # e.g. "ADAPTER_REGISTRY", "LLM_GENERATED", "README_SYNTHESIZED"

    # Adapter reference
    adapter_path:     str    # Relative path under adapters_dir

    # Pipeline-wide signals from README
    procs_to_skip:             list[str]        = Field(default_factory=list)
    unsupported_constructs:    list[str]        = Field(default_factory=list)
    table_mapping:             dict[str, str]   = Field(default_factory=dict)
    proc_io_tables:            dict[str, Any]   = Field(default_factory=dict)
    input_catalog:             str              = ""
    input_schema:              list[str]        = Field(default_factory=list)
    output_catalog:            str              = ""
    output_schema:             list[str]        = Field(default_factory=list)
    output_format:             str              = "AUTO"
    source_type:               str              = "stored_procedures"  # stored_procedures | sql_queries | ssis_package
    udf_implementations:       list[dict[str, str]] = Field(default_factory=list)
    temp_table_pattern:        str              = ""
    has_db_links:              bool             = False

    # Flags
    fallback_mode:    bool   = False   # True if GENERIC adapter used
    warnings:         list[str] = Field(default_factory=list)

    # Provenance
    readme_quality_score: float = 0.0
    run_id:               str   = ""

    @property
    def is_query_mode(self) -> bool:
        return self.source_type == "sql_queries"

    @property
    def is_ssis_mode(self) -> bool:
        return self.source_type == "ssis_package"


# ---------------------------------------------------------------------------
# Construct map (flat lookup for Conversion Agent prompts)
# ---------------------------------------------------------------------------

class ConstructMap(BaseModel):
    """
    Flat source fn → Trino/PySpark lookup.
    Written to artifacts/00_analysis/construct_map.json.
    Injected per-chunk into Conversion Agent prompts.
    """
    dialect_id:      str
    trino_map:       dict[str, str] = Field(default_factory=dict)
    pyspark_map:     dict[str, str] = Field(default_factory=dict)
    unmapped:        list[str]      = Field(default_factory=list)
    type_mappings:   dict[str, str] = Field(default_factory=dict)  # e.g. NUMBER→DECIMAL

    def hint_for_chunk(
        self,
        functions_in_chunk: list[str],
        strategy: str = "",
    ) -> dict[str, str]:
        """
        Return only the mappings relevant to a specific chunk's functions.

        Args:
            functions_in_chunk: List of dialect function names found in this chunk.
            strategy: ConversionStrategy value (e.g. "TRINO_SQL", "PYSPARK_DF").
                      When strategy starts with "PYSPARK", returns PySpark mappings.
                      Otherwise returns Trino SQL mappings.
        """
        use_pyspark = strategy.upper().startswith("PYSPARK") if strategy else False
        primary_map = self.pyspark_map if use_pyspark else self.trino_map
        fallback_map = self.trino_map if use_pyspark else self.pyspark_map

        hints = {}
        for fn in functions_in_chunk:
            fn_upper = fn.upper()
            if fn_upper in primary_map:
                hints[fn] = primary_map[fn_upper]
            elif fn_upper in fallback_map:
                hints[fn] = fallback_map[fn_upper]
            elif fn_upper in self.unmapped:
                hints[fn] = "⚠️ UNMAPPED — emit TODO comment preserving original call"
        return hints


# ---------------------------------------------------------------------------
# A0 — Adapter registry description (sent to LLM for selection)
# ---------------------------------------------------------------------------

class AdapterRegistryDescription(BaseModel):
    """
    Lightweight summary of an adapter in the registry.
    Sent to the A0 LLM so it can decide which adapter to load.
    """
    filename:                str           # "oracle.json"
    dialect_id:              str           # "oracle"
    dialect_display:         str           # "Oracle PL/SQL (11g-23c)"
    start_patterns_summary:  str           # Human-readable summary of patterns
    dialect_functions_sample: list[str]    # First 10 functions as sample
    confidence:              float
    hand_crafted:            bool