"""
write_semantics.py
==================
Write Semantics Analyzer.

Runs as a sub-step of Planning Agent (between P1 and P4) when write
conflicts are detected.  Produces a WriteSemantics artifact that is
injected into every C2 conversion prompt for affected procs.

Design rationale (informed by Horizon VLDB 2025 + DataBrain 50K query study):

  The #1 correctness failure in multi-proc → same-table migrations is
  write mode confusion.  When 23 Oracle procs all INSERT/MERGE into one
  output table, the source system ran them sequentially via an external
  scheduler (e.g., Autosys, TWS, cron).  In PySpark, each proc is a
  standalone .py file — if all use mode("overwrite"), only the last
  survives.

  The LLM has NO WAY to know this from a single 200-line chunk.  It
  sees INSERT INTO table and must decide: overwrite? append? merge?
  Without explicit instructions, it WILL hallucinate a plausible-looking
  mode("overwrite") because that's the most common PySpark pattern in
  its training data.

  This module:
  1. Extracts DML patterns per proc from the manifest (SANDBOX, no LLM)
  2. For shared-write tables, classifies each proc's write intent:
     TRUNCATE_LOAD, APPEND, MERGE_UPSERT, DELETE_INSERT, CONDITIONAL
  3. Assigns an explicit write_mode per proc per table
  4. If ambiguous (e.g., dynamic SQL builds the DML), flags for LLM
     classification with ONLY the DML-relevant lines (not full code)
  5. Produces write_semantics.json consumed by P4 plan assembly
  6. Injects write_mode + ordering_constraint into every DispatchTask

Anti-hallucination principle:
  "Never let the LLM decide write semantics by inference.
   Tell it the exact mode. Make it a hard constraint, not a suggestion."

Artifact written:
  artifacts/02_planning/write_semantics.json

Integration:
  Called by PlanningAgent._p4_assemble_plan() when plan.write_conflicts()
  returns non-empty.  Output injected into ChunkInfo.construct_hints
  and DispatchTask as write_directives.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from sql_migration.core.artifact_store import ArtifactStore
from sql_migration.core.logger import get_logger
from sql_migration.core.sandbox import Sandbox

log = get_logger("write_semantics")


# ---------------------------------------------------------------------------
# Write intent classification
# ---------------------------------------------------------------------------

class WriteIntent(str, Enum):
    """Classified write intent for a proc's DML to a specific table."""
    TRUNCATE_LOAD   = "TRUNCATE_LOAD"     # DELETE ALL / TRUNCATE then INSERT
    APPEND          = "APPEND"            # INSERT without prior DELETE
    MERGE_UPSERT    = "MERGE_UPSERT"      # MERGE INTO / INSERT ON CONFLICT
    DELETE_INSERT   = "DELETE_INSERT"      # DELETE WHERE <condition> then INSERT
    CONDITIONAL     = "CONDITIONAL"        # Dynamic SQL / IF-ELSE paths
    UNKNOWN         = "UNKNOWN"            # Could not determine


# Map WriteIntent → PySpark write mode directive
_PYSPARK_MODE_MAP = {
    WriteIntent.TRUNCATE_LOAD: 'mode("overwrite")',
    WriteIntent.APPEND:        'mode("append")',
    WriteIntent.MERGE_UPSERT:  "# MERGE: use foreachBatch or DeltaLake merge — see directive below",
    WriteIntent.DELETE_INSERT:  'mode("overwrite").option("replaceWhere", "<CONDITION>")',
    WriteIntent.CONDITIONAL:   "# CONDITIONAL WRITE: see directive below",
    WriteIntent.UNKNOWN:       'mode("append")  # DEFAULT — write intent could not be determined',
}

# Map WriteIntent → Trino SQL directive
_TRINO_MODE_MAP = {
    WriteIntent.TRUNCATE_LOAD: "DELETE FROM {table}; INSERT INTO {table} ...",
    WriteIntent.APPEND:        "INSERT INTO {table} ...",
    WriteIntent.MERGE_UPSERT:  "MERGE INTO {table} USING ... WHEN MATCHED THEN UPDATE ...",
    WriteIntent.DELETE_INSERT:  "DELETE FROM {table} WHERE <condition>; INSERT INTO {table} ...",
    WriteIntent.CONDITIONAL:   "-- CONDITIONAL WRITE: use IF/CASE logic",
    WriteIntent.UNKNOWN:       "INSERT INTO {table} ...  -- DEFAULT: write intent unclear",
}


# ---------------------------------------------------------------------------
# DML pattern detection (regex-based, no LLM)
# ---------------------------------------------------------------------------

# Patterns that indicate TRUNCATE_LOAD
_TRUNCATE_PATTERNS = [
    r"\bTRUNCATE\s+TABLE\s+",
    r"\bDELETE\s+FROM\s+\S+\s*;",         # DELETE with no WHERE = full delete
    r"\bDELETE\s+FROM\s+\S+\s*$",
    r"\bDELETE\s+\S+\s*;",                 # Short DELETE syntax (Oracle)
]

# Patterns that indicate MERGE_UPSERT
_MERGE_PATTERNS = [
    r"\bMERGE\s+INTO\b",
    r"\bINSERT\s+.*?\bON\s+CONFLICT\b",    # Postgres style
    r"\bINSERT\s+.*?\bON\s+DUPLICATE\b",   # MySQL style
    r"\bREPLACE\s+INTO\b",
]

# Patterns that indicate DELETE_INSERT (conditional DELETE before INSERT)
_DELETE_INSERT_PATTERNS = [
    r"\bDELETE\s+FROM\s+\S+\s+WHERE\b",     # DELETE with WHERE clause
]

# Patterns that indicate dynamic/conditional DML
_CONDITIONAL_PATTERNS = [
    r"\bEXECUTE\s+IMMEDIATE\b",
    r"\bIF\b.{0,50}\b(?:INSERT|UPDATE|DELETE|MERGE)\b",
    r"\bCASE\b.{0,80}\b(?:INSERT|UPDATE|DELETE)\b",
]


@dataclass
class ProcWriteProfile:
    """Write profile for one proc against one shared table."""
    proc_name:    str
    table_name:   str
    intent:       WriteIntent = WriteIntent.UNKNOWN
    confidence:   float = 0.0        # 0.0-1.0
    evidence:     list[str] = field(default_factory=list)
    # Ordering info
    order_index:  int = -1           # Position in execution order (-1 = unknown)
    is_first:     bool = False       # True = this proc runs first (can overwrite)
    is_last:      bool = False       # True = this proc runs last (final state)

    # Directive injected into conversion prompt
    pyspark_directive: str = ""
    trino_directive:   str = ""


@dataclass
class WriteSemantics:
    """Complete write semantics for all shared-write tables."""
    tables: dict[str, list[ProcWriteProfile]] = field(default_factory=dict)
    # {table_name: [ProcWriteProfile, ...]} — ordered by execution order
    analysis_timestamp: str = ""

    def to_dict(self) -> dict:
        return {
            "tables": {
                tbl: [
                    {
                        "proc_name": p.proc_name,
                        "intent": p.intent.value,
                        "confidence": p.confidence,
                        "evidence": p.evidence,
                        "order_index": p.order_index,
                        "is_first": p.is_first,
                        "is_last": p.is_last,
                        "pyspark_directive": p.pyspark_directive,
                        "trino_directive": p.trino_directive,
                    }
                    for p in profiles
                ]
                for tbl, profiles in self.tables.items()
            },
            "analysis_timestamp": self.analysis_timestamp,
        }

    def get_directive_for_proc(
        self, proc_name: str, strategy: str,
    ) -> dict[str, str]:
        """
        Get write directives for a specific proc.
        Returns {table_name: directive_text} to inject into C2 prompt.
        """
        directives: dict[str, str] = {}
        for tbl, profiles in self.tables.items():
            for p in profiles:
                if p.proc_name == proc_name:
                    if "PYSPARK" in strategy:
                        directives[tbl] = p.pyspark_directive
                    else:
                        directives[tbl] = p.trino_directive
        return directives


class WriteSemanticsAnalyzer:
    """
    Analyze write patterns across procs that share output tables.

    The analyzer operates in two phases:
      Phase 1 (SANDBOX): Regex-based DML pattern detection on proc bodies.
                         Deterministic, no LLM, handles 80%+ of cases.
      Phase 2 (LLM):     Only for CONDITIONAL/UNKNOWN intents where regex
                         could not classify.  Sends ONLY the DML-relevant
                         lines (10-30 lines max), never full proc code.

    Both phases produce the same output: WriteSemantics with explicit
    directives per proc per table.
    """

    def __init__(
        self,
        store:   ArtifactStore,
        sandbox: Sandbox,
        llm=None,
        default_write_mode: str = "append",
        low_confidence_threshold: float = 0.5,
    ) -> None:
        self.store   = store
        self.sandbox = sandbox
        # Optional LLM client for Phase 2 classification of UNKNOWN/CONDITIONAL
        # intents. When None, UNKNOWN procs get the safe default.
        self._llm = llm
        self._default_write_mode = default_write_mode
        self._low_confidence_threshold = low_confidence_threshold

    def analyze(
        self,
        write_conflicts: dict[str, list[str]],
        conversion_order: list[str],
    ) -> WriteSemantics:
        """
        Analyze write semantics for all shared-write tables.

        Args:
            write_conflicts: {table_name: [proc1, proc2, ...]} from Plan.write_conflicts()
            conversion_order: Topologically sorted proc names from Analysis.

        Returns:
            WriteSemantics with explicit directives per proc.
        """
        log.info("write_semantics_start",
                 shared_tables=len(write_conflicts),
                 total_writers=sum(len(v) for v in write_conflicts.values()))

        # Load manifest for proc metadata
        manifest = self.store.read("analysis", "manifest.json")
        procs_by_name = {p["name"]: p for p in manifest.get("procs", [])}

        semantics = WriteSemantics(
            analysis_timestamp=datetime.now(timezone.utc).isoformat(),
        )

        for table_name, writer_procs in write_conflicts.items():
            profiles = self._analyze_table(
                table_name=table_name,
                writer_procs=writer_procs,
                procs_by_name=procs_by_name,
                conversion_order=conversion_order,
            )
            semantics.tables[table_name] = profiles

        # Persist to filesystem
        self.store.write(
            "planning",
            "write_semantics.json",
            semantics.to_dict(),
        )

        # v6-fix (Gap 11): Also persist to DB for SQL queryability
        if hasattr(self.store, '_db') and self.store._db:
            try:
                all_profiles = []
                for table_name, profiles in semantics.tables.items():
                    for p in profiles:
                        all_profiles.append({
                            "proc_name":        p.proc_name,
                            "table_name":       p.table_name,
                            "intent":           p.intent.value if hasattr(p.intent, 'value') else str(p.intent),
                            "confidence":       p.confidence,
                            "evidence":         p.evidence,
                            "pyspark_directive": p.pyspark_directive,
                            "trino_directive":   p.trino_directive,
                            "order_index":      p.order_index,
                            "is_first":         p.is_first,
                            "is_last":          p.is_last,
                        })
                self.store._db.upsert_write_semantics(all_profiles)
            except Exception as e:
                log.debug("write_semantics_db_persist_failed", error=str(e)[:100])

        log.info("write_semantics_complete",
                 tables=len(semantics.tables),
                 total_profiles=sum(
                     len(p) for p in semantics.tables.values()
                 ))

        return semantics

    def _analyze_table(
        self,
        table_name:       str,
        writer_procs:     list[str],
        procs_by_name:    dict[str, dict],
        conversion_order: list[str],
    ) -> list[ProcWriteProfile]:
        """Analyze write intent for all procs writing to one table."""

        # Determine execution ordering from topological sort
        order_map = {name: i for i, name in enumerate(conversion_order)}

        profiles: list[ProcWriteProfile] = []

        for proc_name in writer_procs:
            proc = procs_by_name.get(proc_name, {})
            if not proc:
                profiles.append(ProcWriteProfile(
                    proc_name=proc_name,
                    table_name=table_name,
                    intent=WriteIntent.UNKNOWN,
                ))
                continue

            # Phase 1: Sandbox-based DML pattern analysis
            intent, confidence, evidence = self._classify_write_intent(
                proc_name=proc_name,
                source_file=proc.get("source_file", ""),
                start_line=proc.get("start_line", 0),
                end_line=proc.get("end_line", 0),
                table_name=table_name,
                has_dynamic_sql=proc.get("has_dynamic_sql", False),
            )

            # Phase 2: LLM-assisted classification for UNKNOWN/CONDITIONAL
            # Only the DML-relevant lines (evidence) are sent — never full proc.
            if intent in (WriteIntent.UNKNOWN, WriteIntent.CONDITIONAL) and self._llm:
                llm_intent = self._llm_classify_intent(
                    proc_name=proc_name,
                    table_name=table_name,
                    evidence_lines=evidence,
                )
                if llm_intent and llm_intent != WriteIntent.UNKNOWN:
                    log.info("write_intent_llm_classified",
                             proc=proc_name, table=table_name,
                             phase1_intent=intent.value,
                             phase2_intent=llm_intent.value)
                    intent = llm_intent
                    confidence = 0.6  # LLM classification — moderate confidence

            profile = ProcWriteProfile(
                proc_name=proc_name,
                table_name=table_name,
                intent=intent,
                confidence=confidence,
                evidence=evidence,
                order_index=order_map.get(proc_name, -1),
            )
            profiles.append(profile)

        # Sort by execution order
        profiles.sort(key=lambda p: p.order_index if p.order_index >= 0 else 9999)

        # Assign first/last flags
        if profiles:
            profiles[0].is_first = True
            profiles[-1].is_last = True

        # Generate directives based on intent + ordering
        self._generate_directives(table_name, profiles)

        return profiles

    def _classify_write_intent(
        self,
        proc_name:      str,
        source_file:    str,
        start_line:     int,
        end_line:       int,
        table_name:     str,
        has_dynamic_sql: bool,
    ) -> tuple[WriteIntent, float, list[str]]:
        """
        Classify write intent using sandbox regex analysis.
        Returns (intent, confidence, evidence_lines).
        """
        # Run DML extraction in sandbox
        result = self.sandbox.run(
            script="extraction/extract_structure.py",
            args={
                "mode":         "extract_chunk",
                "source_file":  source_file,
                "start_line":   start_line,
                "end_line":     end_line,
                "schema_context": {},
            },
        )

        if not result.success:
            return WriteIntent.UNKNOWN, 0.0, ["sandbox extraction failed"]

        try:
            chunk_code = result.stdout_json.get("chunk_code", "")
        except Exception:
            return WriteIntent.UNKNOWN, 0.0, ["could not parse sandbox output"]

        if not chunk_code:
            return WriteIntent.UNKNOWN, 0.0, ["empty chunk code"]

        # Focus on lines that reference this specific table
        table_upper = table_name.upper()
        relevant_lines = [
            line for line in chunk_code.splitlines()
            if table_upper in line.upper()
            or any(kw in line.upper() for kw in
                   ["INSERT", "DELETE", "MERGE", "TRUNCATE", "UPDATE"])
        ]
        relevant_text = "\n".join(relevant_lines)
        evidence = relevant_lines[:5]  # Keep max 5 evidence lines

        # Check patterns in priority order
        if has_dynamic_sql and any(
            re.search(p, chunk_code, re.IGNORECASE)
            for p in _CONDITIONAL_PATTERNS
        ):
            return WriteIntent.CONDITIONAL, 0.4, evidence

        if any(re.search(p, relevant_text, re.IGNORECASE) for p in _MERGE_PATTERNS):
            return WriteIntent.MERGE_UPSERT, 0.9, evidence

        if any(re.search(p, relevant_text, re.IGNORECASE) for p in _TRUNCATE_PATTERNS):
            # Check if there's also an INSERT after the TRUNCATE/DELETE
            if re.search(r"\bINSERT\b", relevant_text, re.IGNORECASE):
                return WriteIntent.TRUNCATE_LOAD, 0.9, evidence
            return WriteIntent.TRUNCATE_LOAD, 0.7, evidence

        if any(re.search(p, relevant_text, re.IGNORECASE) for p in _DELETE_INSERT_PATTERNS):
            if re.search(r"\bINSERT\b", relevant_text, re.IGNORECASE):
                return WriteIntent.DELETE_INSERT, 0.8, evidence
            return WriteIntent.DELETE_INSERT, 0.6, evidence

        if re.search(r"\bINSERT\b", relevant_text, re.IGNORECASE):
            return WriteIntent.APPEND, 0.7, evidence

        return WriteIntent.UNKNOWN, 0.2, evidence

    def _llm_classify_intent(
        self,
        proc_name:      str,
        table_name:     str,
        evidence_lines: list[str],
    ) -> WriteIntent | None:
        """
        Phase 2: LLM-assisted classification for UNKNOWN/CONDITIONAL intents.

        Sends ONLY the DML-relevant lines (5-10 lines max) to the LLM with
        a targeted prompt. This is a focused, cheap call — not the full proc.

        Returns:
            WriteIntent if successfully classified, None on failure.
            On failure, the caller keeps the Phase 1 result (UNKNOWN → append).
        """
        if not evidence_lines:
            return None

        # Build the minimal context: just the DML lines + table name
        dml_context = "\n".join(evidence_lines[:10])

        prompt_messages = [
            {"role": "system", "content": (
                "You classify SQL DML intent for a specific table. "
                "Output ONLY one of: TRUNCATE_LOAD, APPEND, MERGE_UPSERT, "
                "DELETE_INSERT, UNKNOWN. No explanation."
            )},
            {"role": "user", "content": (
                f"Table: {table_name}\n"
                f"Proc: {proc_name}\n\n"
                f"DML lines:\n{dml_context}\n\n"
                f"What is the write intent for table '{table_name}'? "
                f"TRUNCATE_LOAD = deletes all rows then inserts. "
                f"APPEND = only inserts, never deletes. "
                f"MERGE_UPSERT = insert or update based on key match. "
                f"DELETE_INSERT = conditional delete then insert. "
                f"UNKNOWN = cannot determine.\n"
                f"Answer with ONE word only:"
            )},
        ]

        try:
            response = self._llm.call(
                messages=prompt_messages,
                expect_json=False,
                call_id=f"ws_{proc_name}_{table_name}",
            )
            # Parse the response — should be a single word
            raw = response.strip().upper().replace('"', '').replace("'", "")

            intent_map = {
                "TRUNCATE_LOAD": WriteIntent.TRUNCATE_LOAD,
                "APPEND":        WriteIntent.APPEND,
                "MERGE_UPSERT":  WriteIntent.MERGE_UPSERT,
                "DELETE_INSERT":  WriteIntent.DELETE_INSERT,
                "UNKNOWN":       WriteIntent.UNKNOWN,
            }
            return intent_map.get(raw)

        except Exception as e:
            log.debug("write_intent_llm_failed",
                      proc=proc_name, table=table_name,
                      error=str(e)[:100])
            return None

    def _generate_directives(
        self,
        table_name:  str,
        profiles:    list[ProcWriteProfile],
    ) -> None:
        """
        Generate explicit write directives for each proc based on
        classified intent + execution ordering.

        The directive is a HARD CONSTRAINT injected into the C2 prompt —
        the LLM MUST follow it, not decide write mode on its own.
        """
        n = len(profiles)
        has_truncate = any(
            p.intent == WriteIntent.TRUNCATE_LOAD for p in profiles
        )

        for i, p in enumerate(profiles):
            # Look up the canonical PySpark mode string for this intent
            mode_str = _PYSPARK_MODE_MAP.get(p.intent, _PYSPARK_MODE_MAP[WriteIntent.UNKNOWN])

            # Build PySpark directive
            if p.intent == WriteIntent.TRUNCATE_LOAD:
                if p.is_first or n == 1:
                    p.pyspark_directive = (
                        f"WRITE DIRECTIVE for '{table_name}' (MANDATORY):\n"
                        f"  This proc performs TRUNCATE+LOAD.\n"
                        f"  Use: df.write.{mode_str}.saveAsTable(\"{table_name}\")\n"
                        f"  This proc runs FIRST — overwrite is correct here."
                    )
                else:
                    # Not first but wants to truncate — likely a mistake or
                    # the ordering is different from topological.
                    # Default to append to preserve earlier proc's data.
                    append_mode = _PYSPARK_MODE_MAP[WriteIntent.APPEND]
                    p.pyspark_directive = (
                        f"WRITE DIRECTIVE for '{table_name}' (MANDATORY):\n"
                        f"  ⚠️ This proc's source DML truncates the table, but "
                        f"{profiles[0].proc_name} already wrote to it.\n"
                        f"  Use: df.write.{append_mode}.saveAsTable(\"{table_name}\")\n"
                        f"  # TODO: REVIEW — source system may have different execution order.\n"
                        f"  # If this proc should run first, reorder in pipeline orchestration."
                    )

            elif p.intent == WriteIntent.APPEND:
                p.pyspark_directive = (
                    f"WRITE DIRECTIVE for '{table_name}' (MANDATORY):\n"
                    f"  This proc APPENDS rows.\n"
                    f"  Use: df.write.{mode_str}.saveAsTable(\"{table_name}\")\n"
                    f"  Do NOT use mode(\"overwrite\") — other procs also write to this table."
                )

            elif p.intent == WriteIntent.MERGE_UPSERT:
                p.pyspark_directive = (
                    f"WRITE DIRECTIVE for '{table_name}' (MANDATORY):\n"
                    f"  This proc performs MERGE/UPSERT.\n"
                    f"  Implement as:\n"
                    f"    existing = spark.table(\"{table_name}\")\n"
                    f"    merged = existing.alias(\"t\").join(\n"
                    f"        new_data.alias(\"s\"), on=<join_keys>, how=\"full\"\n"
                    f"    ).select(...)\n"
                    f"    merged.write.mode(\"overwrite\").saveAsTable(\"{table_name}\")\n"
                    f"  The overwrite here is correct because the merge preserves existing rows."
                )

            elif p.intent == WriteIntent.DELETE_INSERT:
                p.pyspark_directive = (
                    f"WRITE DIRECTIVE for '{table_name}' (MANDATORY):\n"
                    f"  This proc performs conditional DELETE then INSERT.\n"
                    f"  Implement as:\n"
                    f"    existing = spark.table(\"{table_name}\").filter(~<delete_condition>)\n"
                    f"    result = existing.union(new_rows)\n"
                    f"    result.write.mode(\"overwrite\").saveAsTable(\"{table_name}\")\n"
                    f"  Do NOT delete and insert separately — use DataFrame union approach."
                )

            elif p.intent == WriteIntent.CONDITIONAL:
                append_mode = _PYSPARK_MODE_MAP[WriteIntent.APPEND]
                p.pyspark_directive = (
                    f"WRITE DIRECTIVE for '{table_name}' (MANDATORY):\n"
                    f"  ⚠️ This proc uses dynamic/conditional DML — write mode varies.\n"
                    f"  Default to: df.write.{append_mode}.saveAsTable(\"{table_name}\")\n"
                    f"  # TODO: REVIEW — dynamic SQL determines write mode at runtime.\n"
                    f"  # A developer must verify the correct mode for each code path."
                )

            else:  # UNKNOWN
                # Safe default: append (never overwrites other procs' data)
                p.pyspark_directive = (
                    f"WRITE DIRECTIVE for '{table_name}' (MANDATORY):\n"
                    f"  Write intent could not be determined.\n"
                    f"  Use: df.write.{mode_str}.saveAsTable(\"{table_name}\")\n"
                    f"  # TODO: REVIEW — developer must verify write mode.\n"
                    f"  Do NOT use mode(\"overwrite\") — {n} procs share this table."
                )

            # Build Trino SQL directive
            p.trino_directive = (
                f"WRITE DIRECTIVE for '{table_name}' (MANDATORY):\n"
                f"  Intent: {p.intent.value}\n"
                f"  {_TRINO_MODE_MAP.get(p.intent, 'INSERT INTO ...').format(table=table_name)}\n"
                f"  This is proc {i+1} of {n} writing to this table."
            )

        log.debug(
            "write_directives_generated",
            table=table_name,
            profiles=[
                {"proc": p.proc_name, "intent": p.intent.value,
                 "confidence": p.confidence, "is_first": p.is_first}
                for p in profiles
            ],
        )
