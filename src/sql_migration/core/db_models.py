"""
db_models.py
============
SQLAlchemy 2.0 ORM models for the SQL Migration System.

Design decisions (derived from full codebase analysis):

WHAT GOES TO DB
  - All structured, frequently-queried data: run state, proc state, errors,
    complexity scores, validation results, feedback, column diffs, todo items.
  - Converted code (TEXT column): needed by Orchestrator to extract callee
    signatures without filesystem access, and by UI for code preview.
  - Extracted structural metadata: proc entries, table refs, dependency edges,
    table registry with columns.

WHAT STAYS AS FILES
  - Sample parquet data (binary Spark input, can't store in SQL)
  - Dialect adapter JSON files (shared across runs, not run-scoped)
  - Sandbox scripts (source code, not data)

TABLE HIERARCHY (foreign key chain)
  migration_runs
    ├── dialect_profiles          (one per run — Detection output)
    ├── proc_entries              (one per proc per run — Manifest)
    │     ├── proc_calls          (N:N edge list — dependency graph)
    │     └── proc_table_refs     (reads/writes per proc)
    ├── table_registry            (one per unique table per run)
    │     └── table_columns       (column list per table)
    ├── complexity_scores         (one per proc per run — A5 output)
    ├── proc_states               (one per proc per run — WRITE HOTSPOT)
    │     └── proc_errors         (append-only error trail)
    ├── proc_plans                (one per proc per run — Planning output)
    │     └── proc_chunks         (N per proc — chunk boundaries + context)
    ├── chunk_results             (one per (proc, chunk) per run — Conversion output)
    ├── conversion_logs           (one per proc per run — assembled conversion log)
    ├── converted_code            (one per proc per run — final .py or .sql text)
    ├── validation_results        (one per proc per run — Validation output)
    │     ├── column_diffs        (N per validation — LEVEL_3 semantic diff)
    │     └── todo_items          (N per validation — UNMAPPED constructs)
    ├── feedback_bundles          (one per FROZEN proc per run)
    ├── feedback_resolutions      (one per resolved proc per run)
    └── write_conflicts           (N per run — shared-write table detection)

INDEXING STRATEGY
  All tables have a composite index on (run_id, proc_name) where relevant.
  proc_states additionally indexed on status (for UI frozen-proc queries).
  proc_errors indexed on (run_id, proc_name, signature) for error dedup.

Usage:
    from sql_migration.core.db_models import Base, get_engine, get_session
    engine = get_engine()          # SQLite default; Postgres for production
    Base.metadata.create_all(engine)
    with get_session(engine) as session:
        run = MigrationRun(run_id="migration_001", ...)
        session.add(run)
        session.commit()
"""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    event,
    func,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
)


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# migration_runs
# ---------------------------------------------------------------------------

class MigrationRun(Base):
    """
    One entry per pipeline run.
    Top-level anchor for all other tables.
    """
    __tablename__ = "migration_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(128), unique=True, nullable=False, index=True)

    # Source inputs
    readme_path: Mapped[str | None] = mapped_column(Text)
    sql_file_paths: Mapped[list | None] = mapped_column(JSON)     # list[str]

    # Status
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="RUNNING",
        comment="RUNNING | COMPLETE | FAILED | PARTIAL"
    )
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # Summary counts (refreshed after every state transition by Orchestrator)
    total_procs: Mapped[int] = mapped_column(Integer, default=0)
    validated_count: Mapped[int] = mapped_column(Integer, default=0)
    partial_count: Mapped[int] = mapped_column(Integer, default=0)
    frozen_count: Mapped[int] = mapped_column(Integer, default=0)
    skipped_count: Mapped[int] = mapped_column(Integer, default=0)

    # Config snapshot (JSON blob — for audit/reproducibility)
    config_snapshot: Mapped[dict | None] = mapped_column(JSON)

    # Relationships
    dialect_profile: Mapped[DialectProfile | None] = relationship(
        "DialectProfile", back_populates="run", uselist=False, cascade="all, delete-orphan"
    )
    proc_entries: Mapped[list[ProcEntry]] = relationship(
        "ProcEntry", back_populates="run", cascade="all, delete-orphan"
    )
    table_registry: Mapped[list[TableRegistry]] = relationship(
        "TableRegistry", back_populates="run", cascade="all, delete-orphan"
    )
    complexity_scores: Mapped[list[ComplexityScore]] = relationship(
        "ComplexityScore", back_populates="run", cascade="all, delete-orphan"
    )
    proc_states: Mapped[list[ProcState]] = relationship(
        "ProcState", back_populates="run", cascade="all, delete-orphan"
    )
    proc_plans: Mapped[list[ProcPlan]] = relationship(
        "ProcPlan", back_populates="run", cascade="all, delete-orphan"
    )
    chunk_results: Mapped[list[ChunkResult]] = relationship(
        "ChunkResult", back_populates="run", cascade="all, delete-orphan"
    )
    conversion_logs: Mapped[list[ConversionLog]] = relationship(
        "ConversionLog", back_populates="run", cascade="all, delete-orphan"
    )
    converted_codes: Mapped[list[ConvertedCode]] = relationship(
        "ConvertedCode", back_populates="run", cascade="all, delete-orphan"
    )
    validation_results: Mapped[list[ValidationResult]] = relationship(
        "ValidationResult", back_populates="run", cascade="all, delete-orphan"
    )
    feedback_bundles: Mapped[list[FeedbackBundle]] = relationship(
        "FeedbackBundle", back_populates="run", cascade="all, delete-orphan"
    )
    feedback_resolutions: Mapped[list[FeedbackResolution]] = relationship(
        "FeedbackResolution", back_populates="run", cascade="all, delete-orphan"
    )
    write_conflicts: Mapped[list[WriteConflict]] = relationship(
        "WriteConflict", back_populates="run", cascade="all, delete-orphan"
    )


# ---------------------------------------------------------------------------
# dialect_profiles
# ---------------------------------------------------------------------------

class DialectProfile(Base):
    """
    Detection Agent output (D6).
    One per run — captures what dialect was identified and how.
    """
    __tablename__ = "dialect_profiles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("migration_runs.run_id", ondelete="CASCADE"),
        nullable=False, unique=True
    )

    dialect_id: Mapped[str] = mapped_column(String(64), nullable=False)
    dialect_display: Mapped[str] = mapped_column(String(128), default="")
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    resolution: Mapped[str] = mapped_column(
        String(32), default="LLM_PRIMARY",
        comment="README_PRIMARY | LLM_PRIMARY | FALLBACK"
    )
    adapter_path: Mapped[str] = mapped_column(String(256), default="")
    fallback_mode: Mapped[bool] = mapped_column(Boolean, default=False)
    readme_quality_score: Mapped[float] = mapped_column(Float, default=0.0)
    warnings: Mapped[list | None] = mapped_column(JSON)            # list[str]

    # Full adapter and readme signals stored as JSON blobs for
    # portability — queried as a unit, never column-by-column
    adapter_json: Mapped[dict | None] = mapped_column(JSON)
    readme_signals_json: Mapped[dict | None] = mapped_column(JSON)

    run: Mapped[MigrationRun] = relationship("MigrationRun", back_populates="dialect_profile")


# ---------------------------------------------------------------------------
# proc_entries  (Manifest)
# ---------------------------------------------------------------------------

class ProcEntry(Base):
    """
    One stored procedure extracted from source files (Manifest / A1).
    Structural metadata only — no raw code.
    """
    __tablename__ = "proc_entries"
    __table_args__ = (
        UniqueConstraint("run_id", "proc_name", name="uq_proc_entries_run_proc"),
        Index("ix_proc_entries_run", "run_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("migration_runs.run_id", ondelete="CASCADE"), nullable=False
    )
    proc_name: Mapped[str] = mapped_column(String(256), nullable=False)
    source_file: Mapped[str] = mapped_column(Text, default="")
    start_line: Mapped[int] = mapped_column(Integer, default=0)
    end_line: Mapped[int] = mapped_column(Integer, default=0)
    line_count: Mapped[int] = mapped_column(Integer, default=0)

    # Flags (set by generic extractor using adapter patterns)
    has_cursor: Mapped[bool] = mapped_column(Boolean, default=False)
    has_dynamic_sql: Mapped[bool] = mapped_column(Boolean, default=False)
    has_exception_handler: Mapped[bool] = mapped_column(Boolean, default=False)
    has_unsupported: Mapped[list | None] = mapped_column(JSON)     # list[str]
    unresolved_deps: Mapped[list | None] = mapped_column(JSON)     # list[str]

    # Deep extraction detail (A4)
    cursor_nesting_depth: Mapped[int | None] = mapped_column(Integer)
    dynamic_sql_patterns_found: Mapped[list | None] = mapped_column(JSON)   # list[str]
    unsupported_occurrences: Mapped[dict | None] = mapped_column(JSON)      # {str: int}

    # Construct coverage (set by Planning Agent)
    dialect_functions_in_body: Mapped[list | None] = mapped_column(JSON)   # list[str]
    unmapped_count: Mapped[int] = mapped_column(Integer, default=0)

    # Relationships
    run: Mapped[MigrationRun] = relationship("MigrationRun", back_populates="proc_entries")
    calls_made: Mapped[list[ProcCall]] = relationship(
        "ProcCall", foreign_keys="ProcCall.caller_run_proc_id",
        back_populates="caller_entry", cascade="all, delete-orphan"
    )
    table_refs: Mapped[list[ProcTableRef]] = relationship(
        "ProcTableRef", back_populates="proc_entry", cascade="all, delete-orphan"
    )


class ProcCall(Base):
    """
    Directed call edge: caller proc → callee proc.
    Derived from dependency_graph.json edges.
    Used by Orchestrator _deps_clear() at dispatch time.
    """
    __tablename__ = "proc_calls"
    __table_args__ = (
        Index("ix_proc_calls_run_caller", "run_id", "caller_name"),
        Index("ix_proc_calls_run_callee", "run_id", "callee_name"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("migration_runs.run_id", ondelete="CASCADE"), nullable=False
    )
    # Denormalised names for fast lookup without joins
    caller_name: Mapped[str] = mapped_column(String(256), nullable=False)
    callee_name: Mapped[str] = mapped_column(String(256), nullable=False)

    # FK back to proc_entries for the caller (callee may be external)
    caller_run_proc_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("proc_entries.id", ondelete="CASCADE")
    )
    is_external: Mapped[bool] = mapped_column(Boolean, default=False,
        comment="True when callee is not in the manifest (unresolved dep)")

    caller_entry: Mapped[ProcEntry | None] = relationship(
        "ProcEntry", foreign_keys=[caller_run_proc_id], back_populates="calls_made"
    )


class ProcTableRef(Base):
    """
    Table reference by a proc (reads and writes).
    Normalises the tables_read / tables_written lists from ProcEntry.
    Used for write-conflict detection and validation scoping.
    """
    __tablename__ = "proc_table_refs"
    __table_args__ = (
        Index("ix_proc_table_refs_run_proc", "run_id", "proc_name"),
        Index("ix_proc_table_refs_run_table", "run_id", "table_name"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("migration_runs.run_id", ondelete="CASCADE"), nullable=False
    )
    proc_entry_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("proc_entries.id", ondelete="CASCADE"), nullable=False
    )
    proc_name: Mapped[str] = mapped_column(String(256), nullable=False)
    table_name: Mapped[str] = mapped_column(String(256), nullable=False)
    ref_type: Mapped[str] = mapped_column(
        String(8), nullable=False,
        comment="'read' | 'write'"
    )

    proc_entry: Mapped[ProcEntry] = relationship("ProcEntry", back_populates="table_refs")


# ---------------------------------------------------------------------------
# table_registry
# ---------------------------------------------------------------------------

class TableRegistry(Base):
    """
    Every unique table referenced across all procs, resolved against Trino (A2).
    """
    __tablename__ = "table_registry"
    __table_args__ = (
        UniqueConstraint("run_id", "source_name", name="uq_table_registry_run_source"),
        Index("ix_table_registry_run", "run_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("migration_runs.run_id", ondelete="CASCADE"), nullable=False
    )
    source_name: Mapped[str] = mapped_column(String(256), nullable=False)
    trino_fqn: Mapped[str] = mapped_column(String(512), default="")
    status: Mapped[str] = mapped_column(
        String(32), nullable=False,
        comment="EXISTS_IN_TRINO | MISSING | TEMP_TABLE | SCHEMA_MISMATCH"
    )
    is_temp: Mapped[bool] = mapped_column(Boolean, default=False)
    row_count: Mapped[int | None] = mapped_column(Integer,
        comment="Populated during Validation V1")

    run: Mapped[MigrationRun] = relationship("MigrationRun", back_populates="table_registry")
    columns: Mapped[list[TableColumn]] = relationship(
        "TableColumn", back_populates="table", cascade="all, delete-orphan"
    )


class TableColumn(Base):
    """Column metadata for a table in the registry."""
    __tablename__ = "table_columns"
    __table_args__ = (
        Index("ix_table_columns_table_id", "table_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    table_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("table_registry.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    type: Mapped[str] = mapped_column(String(128), default="")
    nullable: Mapped[str] = mapped_column(String(8), default="")
    comment: Mapped[str] = mapped_column(Text, default="")

    table: Mapped[TableRegistry] = relationship("TableRegistry", back_populates="columns")


# ---------------------------------------------------------------------------
# complexity_scores
# ---------------------------------------------------------------------------

class ComplexityScore(Base):
    """
    Per-proc complexity score from A5 LLM call.
    Informs strategy selection at P1.
    """
    __tablename__ = "complexity_scores"
    __table_args__ = (
        UniqueConstraint("run_id", "proc_name", name="uq_complexity_run_proc"),
        Index("ix_complexity_run", "run_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("migration_runs.run_id", ondelete="CASCADE"), nullable=False
    )
    proc_name: Mapped[str] = mapped_column(String(256), nullable=False)
    score: Mapped[str] = mapped_column(
        String(16), nullable=False,
        comment="LOW | MEDIUM | HIGH | NEEDS_MANUAL"
    )
    rationale: Mapped[str] = mapped_column(Text, default="")
    priority_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    shared_utility_candidate: Mapped[bool] = mapped_column(Boolean, default=False)

    run: Mapped[MigrationRun] = relationship("MigrationRun", back_populates="complexity_scores")


# ---------------------------------------------------------------------------
# proc_states  (THE WRITE HOTSPOT)
# ---------------------------------------------------------------------------

class ProcState(Base):
    """
    Live Orchestrator state machine state for a single proc.
    Written after EVERY state transition (~6 per proc × N procs per run).

    This is the most frequently updated table — indexed heavily.
    Replaces the monolithic checkpoint.json which serialised all procs at once.
    """
    __tablename__ = "proc_states"
    __table_args__ = (
        UniqueConstraint("run_id", "proc_name", name="uq_proc_states_run_proc"),
        Index("ix_proc_states_run", "run_id"),
        Index("ix_proc_states_run_status", "run_id", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("migration_runs.run_id", ondelete="CASCADE"), nullable=False
    )
    proc_name: Mapped[str] = mapped_column(String(256), nullable=False)

    # State machine state
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="PENDING",
        comment="PENDING|DISPATCHED|CHUNK_CONVERTING|CHUNK_DONE|ALL_CHUNKS_DONE|"
                "VALIDATING|VALIDATED|PARTIAL|FAILED|FROZEN|SKIPPED"
    )

    # Progress tracking
    chunks_done: Mapped[list | None] = mapped_column(JSON)       # list[str]
    current_chunk: Mapped[str | None] = mapped_column(String(64))
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    replan_count: Mapped[int] = mapped_column(Integer, default=0)

    # Validation outcome (set when proc reaches terminal state)
    validation_outcome: Mapped[str | None] = mapped_column(
        String(16), comment="PASS | PARTIAL | FAIL"
    )

    # Timestamps
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow
    )

    # Relationships
    run: Mapped[MigrationRun] = relationship("MigrationRun", back_populates="proc_states")
    errors: Mapped[list[ProcError]] = relationship(
        "ProcError", back_populates="proc_state", cascade="all, delete-orphan"
    )


class ProcError(Base):
    """
    Append-only error trail for a proc.
    One row per error event — never updated, only inserted.
    Enriched with ErrorClassifier fields from core/error_handling.py.
    """
    __tablename__ = "proc_errors"
    __table_args__ = (
        Index("ix_proc_errors_run_proc", "run_id", "proc_name"),
        Index("ix_proc_errors_signature", "run_id", "proc_name", "signature"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    proc_state_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("proc_states.id", ondelete="CASCADE"), nullable=False
    )
    # Denormalised for direct lookup without join
    run_id: Mapped[str] = mapped_column(String(128), nullable=False)
    proc_name: Mapped[str] = mapped_column(String(256), nullable=False)

    error_type: Mapped[str] = mapped_column(String(64), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    agent: Mapped[str] = mapped_column(String(64), default="")
    chunk_id: Mapped[str] = mapped_column(String(64), default="")
    attempt: Mapped[int] = mapped_column(Integer, default=1)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)

    # ErrorClassifier enrichment
    signature: Mapped[str] = mapped_column(String(256), default="",
        comment="Normalised error signature for dedup (line nums stripped)")
    occurrence: Mapped[int] = mapped_column(Integer, default=1,
        comment="How many times this signature has been seen in this run")
    error_class: Mapped[str] = mapped_column(String(32), default="",
        comment="unsolvable | transient | retryable | repeated")

    proc_state: Mapped[ProcState] = relationship("ProcState", back_populates="errors")


# ---------------------------------------------------------------------------
# proc_plans + proc_chunks
# ---------------------------------------------------------------------------

class ProcPlan(Base):
    """
    Planning Agent output (P4) for a single proc.
    Contains strategy, loop guards, and all chunk boundaries.
    """
    __tablename__ = "proc_plans"
    __table_args__ = (
        UniqueConstraint("run_id", "proc_name", name="uq_proc_plans_run_proc"),
        Index("ix_proc_plans_run", "run_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("migration_runs.run_id", ondelete="CASCADE"), nullable=False
    )
    proc_name: Mapped[str] = mapped_column(String(256), nullable=False)
    source_file: Mapped[str] = mapped_column(Text, default="")
    strategy: Mapped[str] = mapped_column(
        String(32), nullable=False,
        comment="TRINO_SQL | PYSPARK_DF | PYSPARK_PIPELINE | MANUAL_SKELETON"
    )
    strategy_rationale: Mapped[str] = mapped_column(Text, default="")
    shared_utility: Mapped[bool] = mapped_column(Boolean, default=False)
    sample_data: Mapped[dict | None] = mapped_column(JSON)

    # Calls + tables written (for cross-proc context and write-conflict detection)
    calls: Mapped[list | None] = mapped_column(JSON)            # list[str]
    tables_written: Mapped[list | None] = mapped_column(JSON)   # list[str]
    unresolved_deps: Mapped[list | None] = mapped_column(JSON)  # list[str]

    # Loop guards (baked in at P4, kept here for replay)
    loop_guard_max_chunk_retry: Mapped[int] = mapped_column(Integer, default=2)
    loop_guard_frozen_after: Mapped[int] = mapped_column(Integer, default=3)

    run: Mapped[MigrationRun] = relationship("MigrationRun", back_populates="proc_plans")
    chunks: Mapped[list[ProcChunk]] = relationship(
        "ProcChunk", back_populates="plan", cascade="all, delete-orphan",
        order_by="ProcChunk.chunk_index"
    )


class ProcChunk(Base):
    """
    One chunk of a proc — line boundaries, schema context, construct hints.
    One row per (proc, chunk) per run.
    """
    __tablename__ = "proc_chunks"
    __table_args__ = (
        UniqueConstraint("plan_id", "chunk_id", name="uq_proc_chunks_plan_chunk"),
        Index("ix_proc_chunks_plan", "plan_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    plan_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("proc_plans.id", ondelete="CASCADE"), nullable=False
    )
    chunk_id: Mapped[str] = mapped_column(String(64), nullable=False)
    chunk_index: Mapped[int] = mapped_column(Integer, default=0)
    start_line: Mapped[int] = mapped_column(Integer, default=0)
    end_line: Mapped[int] = mapped_column(Integer, default=0)
    line_count: Mapped[int] = mapped_column(Integer, default=0)

    # Rich context injected into C2 prompt — kept as JSON blobs
    # (read as a unit, never filtered column-by-column)
    tables: Mapped[list | None] = mapped_column(JSON)            # list[str]
    state_vars: Mapped[dict | None] = mapped_column(JSON)        # {str: str}
    schema_context: Mapped[dict | None] = mapped_column(JSON)    # {table: {columns, types}}
    construct_hints: Mapped[dict | None] = mapped_column(JSON)   # {fn: mapping}

    plan: Mapped[ProcPlan] = relationship("ProcPlan", back_populates="chunks")


# ---------------------------------------------------------------------------
# chunk_results  (Conversion Agent per-chunk output)
# ---------------------------------------------------------------------------

class ChunkResult(Base):
    """
    Result of converting one chunk (C2–C4).
    Written once per (proc, chunk) attempt — not updated, latest retry overwrites.
    """
    __tablename__ = "chunk_results"
    __table_args__ = (
        UniqueConstraint("run_id", "proc_name", "chunk_id", name="uq_chunk_results_run_proc_chunk"),
        Index("ix_chunk_results_run_proc", "run_id", "proc_name"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("migration_runs.run_id", ondelete="CASCADE"), nullable=False
    )
    proc_name: Mapped[str] = mapped_column(String(256), nullable=False)
    chunk_id: Mapped[str] = mapped_column(String(64), nullable=False)

    status: Mapped[str] = mapped_column(
        String(32), nullable=False,
        comment="SUCCESS | FAIL | CONVERSION_FAILED"
    )
    converted_code: Mapped[str | None] = mapped_column(Text)
    errors: Mapped[list | None] = mapped_column(JSON)           # list[str]
    todo_count: Mapped[int] = mapped_column(Integer, default=0)
    todo_items: Mapped[list | None] = mapped_column(JSON)       # list[{line, comment, original_fn}]
    updated_state_vars: Mapped[dict | None] = mapped_column(JSON)  # {str: str}
    self_correction_attempts: Mapped[int] = mapped_column(Integer, default=0)
    duration_s: Mapped[float] = mapped_column(Float, default=0.0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)

    run: Mapped[MigrationRun] = relationship("MigrationRun", back_populates="chunk_results")


# ---------------------------------------------------------------------------
# conversion_logs  (Conversion Agent assembled log)
# ---------------------------------------------------------------------------

class ConversionLog(Base):
    """
    Log of the full conversion process for a proc (all chunks assembled).
    Includes all TODO items, warnings, and self-correction count.
    """
    __tablename__ = "conversion_logs"
    __table_args__ = (
        UniqueConstraint("run_id", "proc_name", name="uq_conversion_logs_run_proc"),
        Index("ix_conversion_logs_run", "run_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("migration_runs.run_id", ondelete="CASCADE"), nullable=False
    )
    proc_name: Mapped[str] = mapped_column(String(256), nullable=False)
    strategy: Mapped[str] = mapped_column(String(32), default="")
    todos: Mapped[list | None] = mapped_column(JSON)      # list[TodoItem]
    warnings: Mapped[list | None] = mapped_column(JSON)   # list[str]
    total_self_corrections: Mapped[int] = mapped_column(Integer, default=0)
    assembled: Mapped[bool] = mapped_column(Boolean, default=False)
    assembly_error: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)

    run: Mapped[MigrationRun] = relationship("MigrationRun", back_populates="conversion_logs")


# ---------------------------------------------------------------------------
# converted_code
# ---------------------------------------------------------------------------

class ConvertedCode(Base):
    """
    Final assembled converted code for a proc (.py or .sql).

    Stored in DB instead of only on the filesystem because:
      1. Orchestrator._load_callee_signature() needs it to inject callee
         function signatures into caller conversion prompts.
      2. Streamlit UI code preview needs it without filesystem access.
      3. FeedbackBundle chunk_codes field needs it at freeze time.

    Large TEXT column — SQLite handles up to 1GB per TEXT cell.
    For Postgres: use TEXT (unlimited).
    """
    __tablename__ = "converted_code"
    __table_args__ = (
        UniqueConstraint("run_id", "proc_name", name="uq_converted_code_run_proc"),
        Index("ix_converted_code_run", "run_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("migration_runs.run_id", ondelete="CASCADE"), nullable=False
    )
    proc_name: Mapped[str] = mapped_column(String(256), nullable=False)
    language: Mapped[str] = mapped_column(
        String(8), nullable=False, comment="'python' | 'sql'"
    )
    code: Mapped[str] = mapped_column(Text, nullable=False)
    written_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)

    # v6: Provenance metadata (Method 1 — genealogy-based lineage tracking)
    # Ref: "From Spark to Fire" (Xie et al., March 2026)
    # Stores confidence score, lineage tags, and validation provenance
    # so callers can assess the reliability of callee signatures.
    provenance_json: Mapped[dict | None] = mapped_column(
        JSON,
        comment="ArtifactProvenance dict: confidence, lineage_tags, validation_status"
    )

    run: Mapped[MigrationRun] = relationship("MigrationRun", back_populates="converted_codes")


# ---------------------------------------------------------------------------
# validation_results + column_diffs + todo_items
# ---------------------------------------------------------------------------

class ValidationResult(Base):
    """
    Full validation result for a proc (V5 output).
    """
    __tablename__ = "validation_results"
    __table_args__ = (
        UniqueConstraint("run_id", "proc_name", name="uq_validation_results_run_proc"),
        Index("ix_validation_results_run", "run_id"),
        Index("ix_validation_results_outcome", "run_id", "outcome"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("migration_runs.run_id", ondelete="CASCADE"), nullable=False
    )
    proc_name: Mapped[str] = mapped_column(String(256), nullable=False)

    outcome: Mapped[str] = mapped_column(
        String(16), nullable=False, comment="PASS | PARTIAL | FAIL"
    )
    validation_level: Mapped[int] = mapped_column(
        Integer, nullable=False, comment="1=STATIC_ONLY 2=SCHEMA_PLUS_EXEC 3=FULL_RECONCILIATION"
    )

    # Row count comparison
    sandbox_row_count: Mapped[int | None] = mapped_column(Integer)
    trino_row_count: Mapped[int | None] = mapped_column(Integer)
    row_deviation_pct: Mapped[float | None] = mapped_column(Float)

    # Execution result (JSON blob — not broken into columns because
    # it's only ever read as a unit for UI display)
    execution_result: Mapped[dict | None] = mapped_column(JSON)

    warnings: Mapped[list | None] = mapped_column(JSON)     # list[str]
    fail_reason: Mapped[str] = mapped_column(Text, default="")
    validated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)

    run: Mapped[MigrationRun] = relationship("MigrationRun", back_populates="validation_results")
    column_diffs: Mapped[list[ColumnDiff]] = relationship(
        "ColumnDiff", back_populates="validation_result", cascade="all, delete-orphan"
    )
    todo_items: Mapped[list[TodoItem]] = relationship(
        "TodoItem", back_populates="validation_result", cascade="all, delete-orphan"
    )


class ColumnDiff(Base):
    """
    Column-level semantic diff result (LEVEL_3 validation only — V4).
    One row per column compared between sandbox output and Trino.
    """
    __tablename__ = "column_diffs"
    __table_args__ = (
        Index("ix_column_diffs_validation", "validation_result_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    validation_result_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("validation_results.id", ondelete="CASCADE"), nullable=False
    )
    column: Mapped[str] = mapped_column(String(256), nullable=False)
    status: Mapped[str] = mapped_column(
        String(32), nullable=False,
        comment="MATCH|RENAMED|TYPE_WIDENED|TYPE_CHANGED|MISSING_IN_TRINO|EXTRA_IN_TRINO"
    )
    severity: Mapped[str] = mapped_column(String(8), default="LOW",
        comment="LOW | MEDIUM | HIGH")
    detail: Mapped[str] = mapped_column(Text, default="")

    validation_result: Mapped[ValidationResult] = relationship(
        "ValidationResult", back_populates="column_diffs"
    )


class TodoItem(Base):
    """
    An UNMAPPED construct left as a TODO in converted code.
    Linked to validation_result for UI display and manual review tracking.
    """
    __tablename__ = "todo_items"
    __table_args__ = (
        Index("ix_todo_items_validation", "validation_result_id"),
        Index("ix_todo_items_run_proc", "run_id", "proc_name"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    validation_result_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("validation_results.id", ondelete="CASCADE"), nullable=False
    )
    # Denormalised for direct lookup without join
    run_id: Mapped[str] = mapped_column(String(128), nullable=False)
    proc_name: Mapped[str] = mapped_column(String(256), nullable=False)

    line: Mapped[int] = mapped_column(Integer, nullable=False)
    comment: Mapped[str] = mapped_column(Text, nullable=False)
    original_fn: Mapped[str] = mapped_column(String(256), default="")
    construct_type: Mapped[str] = mapped_column(String(64), default="UNMAPPED_CONSTRUCT")

    validation_result: Mapped[ValidationResult] = relationship(
        "ValidationResult", back_populates="todo_items"
    )


# ---------------------------------------------------------------------------
# feedback_bundles + feedback_resolutions
# ---------------------------------------------------------------------------

class FeedbackBundle(Base):
    """
    Complete developer feedback bundle written when a proc is FROZEN.
    Read by Streamlit UI and polled by Orchestrator.
    """
    __tablename__ = "feedback_bundles"
    __table_args__ = (
        UniqueConstraint("run_id", "proc_name", name="uq_feedback_bundles_run_proc"),
        Index("ix_feedback_bundles_run", "run_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("migration_runs.run_id", ondelete="CASCADE"), nullable=False
    )
    proc_name: Mapped[str] = mapped_column(String(256), nullable=False)
    frozen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    replan_count: Mapped[int] = mapped_column(Integer, default=0)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)

    # Context blobs (read as a unit by UI — not queried column-by-column)
    manifest_entry: Mapped[dict | None] = mapped_column(JSON)
    complexity_entry: Mapped[dict | None] = mapped_column(JSON)
    chunk_codes: Mapped[dict | None] = mapped_column(JSON)        # {chunk_id: code}
    validation_reports: Mapped[dict | None] = mapped_column(JSON) # {chunk_id: val_result}
    error_history: Mapped[list | None] = mapped_column(JSON)      # list[ErrorEntry]
    manual_review_items: Mapped[list | None] = mapped_column(JSON) # list[TodoItem]
    available_actions: Mapped[list | None] = mapped_column(JSON)   # list[str]
    bundle_version: Mapped[str] = mapped_column(String(8), default="1")

    run: Mapped[MigrationRun] = relationship("MigrationRun", back_populates="feedback_bundles")
    resolution: Mapped[FeedbackResolution | None] = relationship(
        "FeedbackResolution", back_populates="bundle", uselist=False, cascade="all, delete-orphan"
    )


class FeedbackResolution(Base):
    """
    Human's resolution for a FROZEN proc.
    Written by Streamlit UI, polled by Orchestrator.
    """
    __tablename__ = "feedback_resolutions"
    __table_args__ = (
        UniqueConstraint("run_id", "proc_name", name="uq_feedback_resolutions_run_proc"),
        Index("ix_feedback_resolutions_run", "run_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("migration_runs.run_id", ondelete="CASCADE"), nullable=False
    )
    proc_name: Mapped[str] = mapped_column(String(256), nullable=False)
    bundle_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("feedback_bundles.id", ondelete="SET NULL")
    )

    action: Mapped[str] = mapped_column(
        String(64), nullable=False,
        comment="manual_rewrite_provided | mark_skip | override_pass | replan_with_notes"
    )
    notes: Mapped[str] = mapped_column(Text, default="")
    corrected_code: Mapped[str | None] = mapped_column(Text)
    resolved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    resolved_by: Mapped[str] = mapped_column(String(128), default="human")

    bundle: Mapped[FeedbackBundle | None] = relationship(
        "FeedbackBundle", back_populates="resolution"
    )
    run: Mapped[MigrationRun] = relationship("MigrationRun", back_populates="feedback_resolutions")


# ---------------------------------------------------------------------------
# write_conflicts
# ---------------------------------------------------------------------------

class WriteConflict(Base):
    """
    Tables written by more than one proc in this run.
    Detected at P4 plan assembly. Used by UI to warn developer.
    """
    __tablename__ = "write_conflicts"
    __table_args__ = (
        UniqueConstraint("run_id", "table_name", name="uq_write_conflicts_run_table"),
        Index("ix_write_conflicts_run", "run_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("migration_runs.run_id", ondelete="CASCADE"), nullable=False
    )
    table_name: Mapped[str] = mapped_column(String(256), nullable=False)
    writers: Mapped[list] = mapped_column(JSON, nullable=False)   # list[str] proc names
    writer_count: Mapped[int] = mapped_column(Integer, nullable=False)

    run: Mapped[MigrationRun] = relationship("MigrationRun", back_populates="write_conflicts")


class WriteSemanticsRecord(Base):
    """
    Per-proc per-table write intent classification and directive.
    Populated by WriteSemanticsAnalyzer during P4 plan assembly.

    v6-fix (Gap 11): Previously only in write_semantics.json on filesystem.
    DB column enables queries like "show all procs with UNKNOWN intent"
    and joins with validation_results to correlate write mode errors.
    """
    __tablename__ = "write_semantics"
    __table_args__ = (
        UniqueConstraint("run_id", "proc_name", "table_name",
                         name="uq_write_sem_run_proc_table"),
        Index("ix_write_sem_run", "run_id"),
        Index("ix_write_sem_intent", "run_id", "intent"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("migration_runs.run_id", ondelete="CASCADE"),
        nullable=False,
    )
    proc_name:    Mapped[str]       = mapped_column(String(256), nullable=False)
    table_name:   Mapped[str]       = mapped_column(String(256), nullable=False)
    intent:       Mapped[str]       = mapped_column(String(32), nullable=False)   # WriteIntent value
    confidence:   Mapped[float]     = mapped_column(Float, nullable=False, default=0.0)
    evidence:     Mapped[list]      = mapped_column(JSON, default=list)
    pyspark_directive: Mapped[str]  = mapped_column(Text, default="")
    trino_directive:   Mapped[str]  = mapped_column(Text, default="")
    order_index:  Mapped[int]       = mapped_column(Integer, default=-1)
    is_first:     Mapped[bool]      = mapped_column(Boolean, default=False)
    is_last:      Mapped[bool]      = mapped_column(Boolean, default=False)

    run: Mapped[MigrationRun] = relationship("MigrationRun")


# ---------------------------------------------------------------------------
# Engine + session factory
# ---------------------------------------------------------------------------

def get_engine(url: str | None = None) -> Engine:
    """
    Create a SQLAlchemy engine.

    Default: SQLite at {ARTIFACT_STORE_PATH}/sql_migration.db
    Production: pass a Postgres URL via DATABASE_URL env var or url arg.

    SQLite pragmas applied:
      - WAL mode: allows concurrent readers + one writer (safe for
        the checkpoint write hotspot while UI reads simultaneously)
      - foreign_keys: enforce referential integrity
      - journal_mode=WAL: less lock contention than DELETE journal
    """
    if url is None:
        url = os.environ.get("DATABASE_URL")

    if url is None:
        from sql_migration.core.config_loader import get_config
        store_root = get_config().paths.artifact_store
        db_path = os.path.join(store_root, "sql_migration.db")
        url = f"sqlite:///{db_path}"

    engine = create_engine(
        url,
        echo=False,
        # Connection pool: NullPool for SQLite (one connection per thread),
        # default pool for Postgres
        pool_pre_ping=True,
    )

    # SQLite-specific pragmas
    if url.startswith("sqlite"):
        @event.listens_for(engine, "connect")
        def _set_sqlite_pragmas(connection, _record):
            cursor = connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA synchronous=NORMAL")
            # busy_timeout: wait up to 5s for locks instead of failing immediately.
            # Prevents "database is locked" when Streamlit UI polls while CLI runs.
            cursor.execute("PRAGMA busy_timeout=5000")
            cursor.close()

    return engine


@contextmanager
def get_session(engine: Engine) -> Generator[Session, None, None]:
    """
    Context manager for database sessions.

    Usage:
        engine = get_engine()
        with get_session(engine) as session:
            session.add(some_object)
            session.commit()
    """
    session = Session(engine)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db(engine: Engine | None = None) -> Engine:
    """
    Create all tables if they don't exist.
    Safe to call on every startup — uses CREATE TABLE IF NOT EXISTS.

    Returns the engine for immediate use.
    """
    if engine is None:
        engine = get_engine()
    Base.metadata.create_all(engine)
    return engine
