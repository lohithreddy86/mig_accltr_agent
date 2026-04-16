"""
common.py
=========
Shared enums, base types, and small models used across all agents.
Import from here — never redefine these in individual agent modules.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class ComplexityScore(str, Enum):
    LOW          = "LOW"
    MEDIUM       = "MEDIUM"
    HIGH         = "HIGH"
    NEEDS_MANUAL = "NEEDS_MANUAL"


class ConversionStrategy(str, Enum):
    TRINO_SQL         = "TRINO_SQL"
    PYSPARK_DF        = "PYSPARK_DF"
    PYSPARK_PIPELINE  = "PYSPARK_PIPELINE"
    MANUAL_SKELETON   = "MANUAL_SKELETON"


class TableStatus(str, Enum):
    EXISTS_IN_TRINO   = "EXISTS_IN_TRINO"
    MISSING           = "MISSING"
    TEMP_TABLE        = "TEMP_TABLE"
    SCHEMA_MISMATCH   = "SCHEMA_MISMATCH"


class ProcStatus(str, Enum):
    PENDING             = "PENDING"
    DISPATCHED          = "DISPATCHED"
    CHUNK_CONVERTING    = "CHUNK_CONVERTING"
    CHUNK_DONE          = "CHUNK_DONE"
    ALL_CHUNKS_DONE     = "ALL_CHUNKS_DONE"
    VALIDATING          = "VALIDATING"
    VALIDATED           = "VALIDATED"
    PARTIAL             = "PARTIAL"
    FAILED              = "FAILED"
    FROZEN              = "FROZEN"
    SKIPPED             = "SKIPPED"


class ValidationLevel(int, Enum):
    STATIC_ONLY        = 1
    SCHEMA_PLUS_EXEC   = 2
    FULL_RECONCILIATION = 3


class ValidationOutcome(str, Enum):
    PASS    = "PASS"
    PARTIAL = "PARTIAL"
    FAIL    = "FAIL"


class ResolutionAction(str, Enum):
    MANUAL_REWRITE_PROVIDED     = "manual_rewrite_provided"
    MARK_SKIP                   = "mark_skip"
    OVERRIDE_PASS               = "override_pass"
    REPLAN_WITH_NOTES           = "replan_with_notes"
    # v6-fix: Upload missing source (UDF) and re-run Analysis → Planning → Conversion
    # for all affected procs. This is the "second chance" path when the user
    # misses the pre-conversion Dependency Gate.
    UPLOAD_SOURCE_AND_REANALYZE = "upload_source_and_reanalyze"


class DetectionResolution(str, Enum):
    README_PRIMARY = "README_PRIMARY"
    LLM_PRIMARY    = "LLM_PRIMARY"
    FALLBACK       = "FALLBACK"


class ColumnDiffStatus(str, Enum):
    MATCH             = "MATCH"
    RENAMED           = "RENAMED"
    TYPE_WIDENED      = "TYPE_WIDENED"
    TYPE_CHANGED      = "TYPE_CHANGED"
    MISSING_IN_TRINO  = "MISSING_IN_TRINO"
    EXTRA_IN_TRINO    = "EXTRA_IN_TRINO"


# ---------------------------------------------------------------------------
# Small shared models
# ---------------------------------------------------------------------------

class ColumnInfo(BaseModel):
    name:     str
    type:     str
    nullable: str  = ""
    comment:  str  = ""


class TargetTableSpec(BaseModel):
    """Where a write-role table should be materialised in the target catalog."""
    source_name: str                               # original Oracle name, e.g. "Mis.Lac_Mis_Archive"
    target_fqn:  str                               # e.g. "lz_lakehouse.lm_target_schema.lac_mis_archive"
    columns:     list[ColumnInfo] = Field(default_factory=list)
    status:      str = "SKIPPED"                   # EXISTS | NEEDS_CREATE | CREATED | SKIPPED


class TableRegistryEntry(BaseModel):
    source_name:  str
    trino_fqn:    str        = ""                  # back-compat: alias of source_fqn
    status:       TableStatus
    columns:      list[ColumnInfo] = Field(default_factory=list)
    is_temp:      bool        = False
    row_count:    int | None  = None               # Populated during Validation

    # Target-catalog routing (populated by A3 when output_catalog/schema are set)
    role:          str = "source"                  # source | target | both
    source_fqn:    str = ""                        # explicit read-side FQN
    target_fqn:    str = ""                        # explicit write-side FQN
    target_status: str = "SKIPPED"                 # EXISTS | NEEDS_CREATE | CREATED | SKIPPED


class ChunkInfo(BaseModel):
    chunk_id:        str
    start_line:      int
    end_line:        int
    line_count:      int
    tables:          list[str] = Field(default_factory=list)
    state_vars:      dict[str, str] = Field(default_factory=dict)
    schema_context:  dict[str, Any] = Field(default_factory=dict)
    construct_hints: dict[str, str] = Field(default_factory=dict)
    # Target-catalog routing: for every write-role table the chunk touches,
    # the allow-listed target FQN the LLM must use.
    target_context:  dict[str, Any] = Field(default_factory=dict)


class LoopGuards(BaseModel):
    max_chunk_retry:  int = 2
    max_replan_depth: int = 3
    frozen_after:     int = 3


class ErrorEntry(BaseModel):
    """Single error in an error trail, enriched with error classification fields."""
    error_type:  str
    message:     str
    agent:       str
    chunk_id:    str  = ""
    attempt:     int  = 1
    timestamp:   str  = ""
    # ErrorClassifier fields (populated by Orchestrator)
    signature:   str  = ""   # Normalised error signature for dedup
    occurrence:  int  = 1    # How many times this signature has been seen
    error_class: str  = ""   # unsolvable | transient | retryable | repeated


class TodoItem(BaseModel):
    """An UNMAPPED construct left as a TODO in converted code."""
    line:         int
    comment:      str
    original_fn:  str  = ""
    construct_type: str = "UNMAPPED_CONSTRUCT"
