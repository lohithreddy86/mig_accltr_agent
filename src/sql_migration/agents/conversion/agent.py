"""
agent.py  (Conversion Agent)
=============================
Converts one chunk at a time via an agentic tool-calling loop:

  C1  SANDBOX  — Extract chunk code by line boundaries from the correct
                 source file (supports multi-proc modules via source_ranges)
  C2–C4  LLM+TOOLS — Agentic conversion loop. The LLM receives source SQL,
                 construct mappings, schema context, callee signatures, UDF
                 implementations, and write directives. It iteratively converts,
                 tests (validate_sql / run_pyspark / query_trino), and self-
                 corrects within a budget-limited tool-calling loop managed by
                 ToolExecutor. Submits final code via submit_result tool.
  C5  SANDBOX  — Assemble all chunks for a proc + run module integration test
                 (multi-chunk only) → write final proc file + callee interface

Supports both TRINO_SQL and PYSPARK strategies with strategy-specific tools.
Maintains per-proc conversation history, chunk codes, and inter-chunk context
summaries so later chunks see what earlier chunks defined.

Called by Orchestrator via run_chunk(task: DispatchTask).
Returns ChunkConversionResult.
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from sql_migration.core.artifact_store import ArtifactStore
from sql_migration.core.error_handling import ErrorClass, ErrorClassifier, ErrorTracker
from sql_migration.core.config_loader import get_config
from sql_migration.core.llm_client import LLMClient
from sql_migration.core.logger import get_logger
from sql_migration.core.sandbox import Sandbox
from sql_migration.models import (
    ChunkConversionResult,
    ConversionLog,
    DispatchTask,
)
from sql_migration.models.common import ConversionStrategy, TodoItem

log = get_logger("conversion")


class ConversionAgent:
    """
    Conversion Agent — converts one chunk at a time via agentic tool-calling.

    One instance is created per pipeline run and reused for all chunks.
    The C2–C4 loop (convert → test → self-correct) runs inside ToolExecutor
    with budget and turn limits. Per-proc state (histories, chunk codes,
    inter-chunk summaries, error trackers) is accumulated across chunks
    and reset on replan.
    """

    def __init__(self, store: ArtifactStore, sandbox: Sandbox, mcp=None) -> None:
        self.store   = store
        self.sandbox = sandbox
        self.mcp     = mcp
        self.llm     = LLMClient(agent="conversion")
        self.cfg     = get_config().conversion
        self.global_cfg = get_config()

        # Load dialect functions for remnant detection (respects config toggle)
        dialect_fns, _ = self._load_adapter_mappings()
        if not self.cfg.sandbox_validation.run_remnant_check:
            dialect_fns = []  # Disabled — skip remnant detection at submit

        # Tool executor for agentic C2-C4 loop
        from sql_migration.core.tool_executor import ToolExecutor
        self._tool_executor = ToolExecutor(
            sandbox=sandbox, mcp=mcp, store=store,
            dialect_functions=dialect_fns,
        )

        # v10 Task 3: Load query mode flag + type_mappings + column_analysis
        try:
            dp = self.store.read("analysis", "dialect_profile.json")
            self._is_query_mode = dp.get("source_type") == "sql_queries"
            self._dialect_display = dp.get("dialect_display", "")
        except Exception:
            self._is_query_mode = False
            self._dialect_display = ""

        # Source dialect info for conversion prompts
        try:
            rs = self.store.read("analysis", "readme_signals.json")
            self._source_engine = rs.get("source_engine", "")    # e.g. "DB2 11.5"
            self._declared_dialect = rs.get("declared_dialect", "")  # e.g. "DB2 SQL"
        except Exception:
            self._source_engine = ""
            self._declared_dialect = ""

        # Dialect ID for dialect-specific prompt routing (e.g., Oracle→PySpark guidance)
        try:
            dp = self.store.read("analysis", "dialect_profile.json")
            self._dialect_id = dp.get("dialect_id", "")
        except Exception:
            self._dialect_id = ""

        # Build a human-readable source label for prompts
        if self._source_engine and self._declared_dialect:
            self._source_label = f"{self._source_engine} ({self._declared_dialect})"
        elif self._source_engine:
            self._source_label = self._source_engine
        elif self._declared_dialect:
            self._source_label = self._declared_dialect
        elif self._dialect_display:
            self._source_label = self._dialect_display
        else:
            self._source_label = "source SQL"

        try:
            cm = self.store.read("analysis", "construct_map.json")
            self._type_mappings: dict[str, str] = cm.get("type_mappings", {})
        except Exception:
            self._type_mappings = {}

        try:
            ca = self.store.read("analysis", "column_analysis.json")
            self._column_analysis: list[dict] = ca.get("entries", [])
        except Exception:
            self._column_analysis = []

        # Full-registry fallback for target-catalog write rewriting. The
        # Conversion Agent is constructed BEFORE Analysis writes
        # table_registry.json, so this map starts empty and is populated
        # lazily on first access via _get_registry_target_map().
        self._registry_target_map: dict[str, str] | None = None

        # Per-proc conversation history for multi-chunk procs
        # Key: proc_name → LLM history list
        # v10: Currently unused — inter-chunk continuity uses state_vars +
        # completion_summary instead of full conversation history. Kept
        # commented for future use if full history replay is needed.
        # self._histories: dict[str, list[dict]] = {}

        # Accumulated chunk codes per proc (for assembly in C5)
        # Key: proc_name → {chunk_id: converted_code}
        self._chunk_codes: dict[str, dict[str, str]] = {}
        # Used by deterministic-accept paths; was missing → AttributeError
        # forced those paths to fall through to the full agentic loop.
        self._chunks: dict[str, dict[str, str]] = {}

        # Inter-chunk completion summaries (for next chunk's LLM context)
        # Key: proc_name → accumulated summary string
        self._chunk_summaries: dict[str, str] = {}

        # Per-proc error trackers for the C3/C4 self-correction loop
        # Key: proc_name → ErrorTracker (reset on replan)
        self._error_trackers: dict[str, ErrorTracker] = {}

    # =========================================================================
    # Step progress (for Streamlit UI live sub-step display)
    # =========================================================================

    def _write_step_progress(self, proc: str, chunk: str,
                              step: str, detail: str = "",
                              status: str = "RUNNING", **extra) -> None:
        """Write step progress with history so the UI can show a timeline."""
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        try:
            # Read existing progress to accumulate history
            filename = f"conversion_progress_{proc}_{chunk}.json"
            try:
                existing = self.store.read("conversion", filename)
                steps = existing.get("steps", [])
            except Exception:
                steps = []

            # Mark previous RUNNING step as DONE
            for s in steps:
                if s.get("status") == "RUNNING":
                    s["status"] = "DONE"
                    s["ended_at"] = now

            # Append new step
            entry = {"step": step, "detail": detail, "status": status,
                     "started_at": now, **extra}
            steps.append(entry)

            self.store.write("conversion", filename, {
                "proc": proc, "chunk": chunk,
                "step": step, "detail": detail,
                "steps": steps,
                "updated_at": now,
            })
        except Exception:
            pass  # Progress is advisory — never block conversion

    # =========================================================================
    # Public entrypoint — called by Orchestrator per chunk
    # =========================================================================

    def run_chunk(self, task: DispatchTask) -> ChunkConversionResult:
        """
        Convert one chunk. Returns ChunkConversionResult with status SUCCESS or FAIL.
        On success: accumulates chunk code and triggers assembly if it's the last chunk.
        """
        start = time.monotonic()
        proc  = task.proc_name
        chunk = task.chunk_id
        log.info("conversion_chunk_start",
                 proc=proc, chunk=chunk,
                 chunk_index=task.chunk_index,
                 total_chunks=task.total_chunks,
                 strategy=task.strategy.value,
                 retry=task.retry_number)

        # ── C1: Extract chunk code ────────────────────────────────────────────
        self._write_step_progress(proc, chunk, "C1", "Extracting source chunk...")
        chunk_code = self._c1_extract_chunk(task)
        if not chunk_code:
            return ChunkConversionResult(
                proc_name=proc, chunk_id=chunk,
                status="FAIL",
                errors=["C1: Could not extract chunk code from source file"],
                duration_s=time.monotonic() - start,
            )

        # ── Query mode: deterministic conversion pipeline ────────────────────
        if self._is_query_mode:
            return self._convert_query_deterministic(task, chunk_code, start)

        # ── PySpark proc mode: deterministic first-pass (if available) ───────
        # SCOPE GUARD: Only for PySpark strategies. TRINO_SQL is NEVER touched.
        if task.strategy.value in ("PYSPARK_DF", "PYSPARK_PIPELINE"):
            det_result = self._try_deterministic_first_pass(task, chunk_code, start)
            if det_result is not None:
                return det_result
            # If deterministic first-pass returns None → fall through to agentic

        # ── Proc mode: Agentic conversion with tool-calling ──────────────────
        self._write_step_progress(proc, chunk, "C2", "Agentic conversion...")

        # Get prior chunk summary for inter-chunk context
        prior_summary = self._chunk_summaries.get(proc, "")

        agentic_result = self._c2_c4_agentic(task, chunk_code, prior_summary)

        if not agentic_result.get("success"):
            errors = [agentic_result.get("error", "Agentic conversion failed")]
            status = agentic_result.get("status", "CONVERSION_FAILED")

            # Include attempt details for the FROZEN feedback bundle
            attempt_details = agentic_result.get("attempts", [])
            if attempt_details:
                for a in attempt_details[:3]:
                    if a.get("error"):
                        errors.append(f"Attempt {a['attempt_number']}: {a['error'][:150]}")

            log.warning("chunk_conversion_failed",
                        proc=proc, chunk=chunk,
                        status=status,
                        errors=errors[:3],
                        tool_calls=agentic_result.get("tool_calls", 0),
                        budget=agentic_result.get("budget_info", ""),
                        best_code_lines=len(agentic_result.get("best_code", "").splitlines()))
            return ChunkConversionResult(
                proc_name=proc, chunk_id=chunk,
                status="CONVERSION_FAILED",
                converted_code=agentic_result.get("best_code", ""),  # Best effort
                errors=errors,
                self_correction_attempts=agentic_result.get("tool_calls", 0),
                duration_s=time.monotonic() - start,
            )

        final_code = agentic_result["converted_code"]
        test_code = agentic_result.get("test_code", "")
        corrections = agentic_result.get("tool_calls", 0)

        # Target-catalog rewrite on the agentic output too — guarantees
        # writes land in the target catalog regardless of which path
        # produced the code. Runs even when per-chunk target_context is
        # empty so the run-wide registry fallback can catch tables A2
        # mis-classified (e.g. staging tables flagged TEMP_TABLE).
        _tgt_ctx_post = getattr(task, "target_context", None) or {}
        if final_code:
            _rewritten = self._rewrite_write_targets(final_code, _tgt_ctx_post)
            if _rewritten != final_code:
                log.info("agentic_target_rewrite",
                         proc=proc, chunk=chunk,
                         chunk_targets=len(_tgt_ctx_post),
                         registry_targets=len(self._get_registry_target_map()))
                final_code = _rewritten

        # ── Semantic Review with Tier-3 resume-on-retry ──────────────────────
        # Review gate: fast_review (deterministic) → semantic_review (LLM).
        # On failure: targeted_repair (single LLM call, ~10 s) replaces the
        # previous "re-run the full agentic loop" path (~90-120 s).
        # Loop exits early when the same review signature recurs (unsolvable).
        max_review_rounds = 2
        seen_signatures: set[str] = set()
        strategy_val = task.strategy.value if hasattr(task.strategy, 'value') else str(task.strategy)
        for review_round in range(1, max_review_rounds + 1):
            self._write_step_progress(proc, chunk, "REVIEW",
                                       f"Semantic review round {review_round}...")
            # Tier-2 fast-review gate: skip the ~12 s LLM semantic review when
            # deterministic heuristics confirm statement coverage + no silent
            # NULL / Oracle remnants. Also enforces target-catalog write FQNs
            # when task.target_context is populated.
            tgt_ctx = getattr(task, "target_context", None) or {}
            allowed_write_fqns = {
                spec.get("target_fqn", "")
                for spec in tgt_ctx.values()
                if isinstance(spec, dict) and spec.get("target_fqn")
            }
            fast = self._fast_review(chunk_code, final_code,
                                      allowed_write_fqns=allowed_write_fqns)
            if not fast["issues_found"]:
                log.info("fast_review_passed", proc=proc, chunk=chunk,
                         round=review_round)
                review = {"issues_found": False, "feedback": ""}
            else:
                log.info("fast_review_flagged", proc=proc, chunk=chunk,
                         feedback=fast["feedback"][:200])
                review = self._semantic_review(
                    chunk_code, final_code, strategy=strategy_val)

            if not review["issues_found"]:
                break  # Clean — accept the conversion

            sig = self._review_signature(review["feedback"])
            log.warning("semantic_review_round",
                        proc=proc, round=review_round, signature=sig,
                        feedback=review["feedback"][:200])

            if sig in seen_signatures:
                # Same issue after a repair attempt → unsolvable. Stop.
                log.error("semantic_review_same_signature",
                          proc=proc, signature=sig,
                          msg="Repair did not change the reviewer's verdict — marking PARTIAL.")
                return ChunkConversionResult(
                    proc_name=proc, chunk_id=chunk,
                    status="CONVERSION_FAILED",
                    converted_code=final_code,  # emit best-so-far (may still be useful)
                    errors=[f"Semantic review repeated same issue after repair: "
                            f"{review['feedback'][:300]}"],
                    self_correction_attempts=corrections,
                    duration_s=time.monotonic() - start,
                )
            seen_signatures.add(sig)

            if review_round == max_review_rounds:
                log.error("semantic_review_exhausted",
                          proc=proc, rounds=max_review_rounds,
                          feedback=review["feedback"][:300])
                return ChunkConversionResult(
                    proc_name=proc, chunk_id=chunk,
                    status="CONVERSION_FAILED",
                    converted_code=final_code,  # best-so-far — still useful for manual review
                    errors=[
                        f"Semantic review failed after {max_review_rounds} rounds: "
                        f"{review['feedback'][:300]}"
                    ],
                    self_correction_attempts=corrections,
                    duration_s=time.monotonic() - start,
                )

            # Tier-3: single-shot targeted repair instead of re-running the
            # full agentic loop. ~10 s vs ~90-120 s.
            self._write_step_progress(proc, chunk, "REPAIR",
                                       f"Targeted repair (round {review_round})...")
            repaired = self._targeted_repair(
                source_code=chunk_code,
                converted_code=final_code,
                feedback=review["feedback"],
                strategy=strategy_val,
            )
            if repaired is None:
                log.error("targeted_repair_failed", proc=proc, round=review_round)
                return ChunkConversionResult(
                    proc_name=proc, chunk_id=chunk,
                    status="CONVERSION_FAILED",
                    converted_code=final_code,
                    errors=[f"Targeted repair failed after review round {review_round}"],
                    self_correction_attempts=corrections,
                    duration_s=time.monotonic() - start,
                )
            final_code = repaired
            corrections += 1
            log.info("targeted_repair_applied", proc=proc, round=review_round,
                     new_lines=len(final_code.split("\n")))

        # ── Count TODOs ───────────────────────────────────────────────────────
        todo_count, todo_items = self._count_todos(final_code)

        # ── Proc mode: full inter-chunk handling ──────────────────────────────
        # Store test artifact if produced
        if test_code:
            self.store.write_code("conversion", f"test_{proc}_{chunk}.py", test_code)
            log.info("c2_test_generated", proc=proc, chunk=chunk,
                     test_lines=len(test_code.splitlines()))

        # Wire test for C5 integration testing (multi-chunk procs)
        if test_code:
            test_code = self._validate_test_quality(test_code, task)
        self._current_test = test_code if test_code else getattr(self, '_current_test', None)

        # Store inter-chunk context for next chunk
        completion_summary = agentic_result.get("completion_summary", "")
        if completion_summary:
            existing = self._chunk_summaries.get(proc, "")
            self._chunk_summaries[proc] = (
                (existing + "\n" + completion_summary).strip()
            )

        # State vars from converted code (for orchestrator)
        live_vars = agentic_result.get("state_vars", {})

        # Accumulate chunk for later assembly
        if proc not in self._chunk_codes:
            self._chunk_codes[proc] = {}
        self._chunk_codes[proc][chunk] = final_code

        # ── C5: Assemble if this is the last chunk ────────────────────────────
        result = ChunkConversionResult(
            proc_name=proc,
            chunk_id=chunk,
            status="SUCCESS",
            converted_code=final_code,
            todo_count=todo_count,
            todo_items=[TodoItem(**t) if isinstance(t, dict) else t for t in todo_items],
            self_correction_attempts=corrections,
            updated_state_vars=live_vars if live_vars else task.state_vars,  # FIX 6: live vars from converted code
            duration_s=time.monotonic() - start,
        )

        if task.chunk_index == task.total_chunks - 1:
            # Last chunk — assemble all chunks together
            self._write_step_progress(proc, chunk, "C5", "Assembling all chunks...")
            assembly_result = self._c5_assemble(task)
            if assembly_result["status"] != "PASS":
                log.warning("assembly_failed",
                            proc=proc,
                            error=assembly_result.get("error", ""))
                result.status = "CONVERSION_FAILED"
                result.errors = [f"C5 ASSEMBLY: {assembly_result.get('error', '')}"]
            else:
                log.info("proc_assembled",
                         proc=proc,
                         path=assembly_result.get("output_path", ""))
                # Write conversion log
                self._write_conversion_log(task, result, assembly_result)

        log.info("conversion_chunk_done",
                 proc=proc, chunk=chunk,
                 status=result.status,
                 todos=todo_count,
                 corrections=corrections,
                 duration_s=round(result.duration_s, 2))

        return result

    # =========================================================================
    # C0: Deterministic first-pass for PySpark (converter-as-preprocessor)
    # =========================================================================

    def _try_deterministic_first_pass(self, task, chunk_code: str,
                                       start_time: float) -> "ChunkConversionResult | None":
        """Attempt deterministic conversion for PySpark strategies.

        SCOPE GUARD: This method is ONLY called for PYSPARK_DF and
        PYSPARK_PIPELINE strategies. TRINO_SQL never reaches here.

        Returns:
            ChunkConversionResult if deterministic conversion succeeds and
            confidence is high enough. Returns None to fall through to
            the agentic C2-C4 loop.
        """
        try:
            from sql_migration.deterministic.preprocessor import (
                OraclePreprocessor, is_converter_available,
            )
            from sql_migration.deterministic.interchange import ConversionRoute
        except ImportError:
            # Deterministic converter not available — fall through to agentic
            return None

        if not is_converter_available():
            return None

        proc = task.proc_name
        chunk = task.chunk_id

        self._write_step_progress(proc, chunk, "C0",
                                   "Deterministic first-pass conversion...")
        log.info("deterministic_first_pass_start", proc=proc, chunk=chunk)

        try:
            preprocessor = OraclePreprocessor()

            # Detect status table from source for accurate classification
            status_table = None
            try:
                dp = self.store.read("analysis", "dialect_profile.json")
                # Status table might be stored in dialect profile or readme signals
            except Exception:
                pass

            artifact = preprocessor.run_full_conversion(
                proc_source=chunk_code,
                proc_name=proc,
                status_table=status_table,
            )

            log.info("deterministic_first_pass_result",
                     proc=proc, chunk=chunk,
                     confidence=artifact.confidence_score,
                     tier=artifact.confidence_tier,
                     route=artifact.route.value,
                     todo_count=artifact.todo_count)

            # Store the artifact for downstream use
            try:
                self.store.write("conversion",
                                 f"{proc}/deterministic_result.json",
                                 artifact.to_dict())
            except Exception:
                pass  # Artifact storage is advisory

            # Route decision
            # if artifact.route == ConversionRoute.DETERMINISTIC_ACCEPT:
            #     # High confidence, zero TODOs — accept without LLM
            #     log.info("deterministic_accepted", proc=proc, chunk=chunk,
            #              confidence=artifact.confidence_score)

            #     # Accumulate chunk code (same as agentic path)
            #     if proc not in self._chunks:
            #         self._chunks[proc] = {}
            #     self._chunks[proc][chunk] = artifact.generated_pyspark

            #     return ChunkConversionResult(
            #         proc_name=proc, chunk_id=chunk,
            #         status="SUCCESS",
            #         converted_code=artifact.generated_pyspark,
            #         conversion_log={"method": "deterministic",
            #                         "confidence": artifact.confidence_score,
            #                         "tier": artifact.confidence_tier,
            #                         "conversion_path": artifact.conversion_path},
            #         duration_s=time.monotonic() - start_time,
            #     )
            if artifact.route == ConversionRoute.DETERMINISTIC_ACCEPT:
                # Target-catalog rewrite: rewrite every INSERT/UPDATE/DELETE/
                # MERGE/CTAS/saveAsTable/insertInto/writeTo target in the
                # deterministic output. Done BEFORE review so the allow-list
                # check sees the corrected code. Runs even when per-chunk
                # target_context is empty so the run-wide registry fallback
                # catches tables A2 mis-classified.
                tgt_ctx = getattr(task, "target_context", None) or {}
                rewritten = self._rewrite_write_targets(
                    artifact.generated_pyspark, tgt_ctx)
                if rewritten != artifact.generated_pyspark:
                    log.info("deterministic_target_rewrite",
                             proc=proc, chunk=chunk,
                             chunk_targets=len(tgt_ctx),
                             registry_targets=len(self._get_registry_target_map()))
                    artifact.generated_pyspark = rewritten

                # Skip the LLM semantic review for simple, high-confidence
                # chunks that the deterministic converter handles perfectly.
                # Saves one LLM round-trip (~10-20 s) per chunk. The review
                # is still run for complex chunks where the converter is more
                # likely to produce silent semantic bugs (correlated UPDATEs,
                # SELECT...INTO, large multi-hundred-line statements).
                has_correlated_update = "UPDATE" in chunk_code.upper() and "SELECT" in chunk_code.upper() and "SET" in chunk_code.upper() and "(" in chunk_code
                has_select_into = "INTO" in chunk_code.upper() and "SELECT" in chunk_code.upper() and "FROM" in chunk_code.upper()
                source_lines = len(chunk_code.split("\n"))
                skip_review = (
                    artifact.confidence_score >= 98
                    and artifact.todo_count <= 1
                    and source_lines < 150
                    and not has_correlated_update
                    and not has_select_into
                )

                if skip_review:
                    log.info("deterministic_accepted_no_review", proc=proc,
                             chunk=chunk, confidence=artifact.confidence_score,
                             lines=source_lines)
                    if proc not in self._chunks:
                        self._chunks[proc] = {}
                    self._chunks[proc][chunk] = artifact.generated_pyspark
                    self._write_deterministic_proc_file(task, artifact.generated_pyspark)
                    return ChunkConversionResult(
                        proc_name=proc, chunk_id=chunk, status="SUCCESS",
                        converted_code=artifact.generated_pyspark,
                        conversion_log={"method": "deterministic",
                                        "confidence": artifact.confidence_score,
                                        "review_skipped": True},
                        duration_s=time.monotonic() - start_time,
                    )

                log.info("deterministic_pre_review", proc=proc, chunk=chunk,
                         confidence=artifact.confidence_score)

                # Tier-2 fast-review gate: skip the LLM call when deterministic
                # heuristics already confirm coverage + no Oracle remnants.
                # Also enforces target-catalog write allow-list.
                tgt_ctx = getattr(task, "target_context", None) or {}
                allowed_write_fqns = {
                    spec.get("target_fqn", "")
                    for spec in tgt_ctx.values()
                    if isinstance(spec, dict) and spec.get("target_fqn")
                }
                # Include registry-fallback targets so rewrites backed by the
                # run-wide registry pass the allow-list check.
                allowed_write_fqns.update(self._get_registry_target_map().values())
                fast = self._fast_review(chunk_code, artifact.generated_pyspark,
                                          allowed_write_fqns=allowed_write_fqns)
                if not fast["issues_found"]:
                    log.info("fast_review_passed", proc=proc, chunk=chunk)
                    review = {"issues_found": False, "feedback": ""}
                else:
                    log.info("fast_review_flagged", proc=proc, chunk=chunk,
                             feedback=fast["feedback"][:200])
                    review = self._semantic_review(
                        chunk_code, artifact.generated_pyspark,
                        strategy=task.strategy.value if hasattr(task.strategy, 'value') else str(task.strategy))

                if review["issues_found"]:
                    # Semantic review caught problems — fall through to agentic
                    log.warning("deterministic_review_rejected",
                                proc=proc, chunk=chunk,
                                feedback=review["feedback"][:300])
                    self._deterministic_artifacts = getattr(
                        self, '_deterministic_artifacts', {})
                    self._deterministic_artifacts[f"{proc}/{chunk}"] = artifact
                    return None  # Fall through to C2-C4 agentic loop

                # Review passed — accept
                log.info("deterministic_accepted", proc=proc, chunk=chunk,
                         confidence=artifact.confidence_score)

                if proc not in self._chunks:
                    self._chunks[proc] = {}
                self._chunks[proc][chunk] = artifact.generated_pyspark
                self._write_deterministic_proc_file(task, artifact.generated_pyspark)

                return ChunkConversionResult(
                    proc_name=proc, chunk_id=chunk,
                    status="SUCCESS",
                    converted_code=artifact.generated_pyspark,
                    conversion_log={"method": "deterministic",
                                    "confidence": artifact.confidence_score,
                                    "tier": artifact.confidence_tier,
                                    "conversion_path": artifact.conversion_path,
                                    "semantic_review": "passed"},
                    duration_s=time.monotonic() - start_time,
                )

            elif artifact.route == ConversionRoute.DETERMINISTIC_THEN_REPAIR:
                # Target-catalog rewrite on the partial deterministic output
                # so the agentic repair loop sees already-routed writes and
                # focuses on the genuine TODOs. Runs even when per-chunk
                # target_context is empty — the run-wide registry fallback
                # still applies.
                tgt_ctx = getattr(task, "target_context", None) or {}
                if artifact.generated_pyspark:
                    rewritten = self._rewrite_write_targets(
                        artifact.generated_pyspark, tgt_ctx)
                    if rewritten != artifact.generated_pyspark:
                        log.info("deterministic_target_rewrite",
                                 proc=proc, chunk=chunk, path="repair",
                                 chunk_targets=len(tgt_ctx),
                                 registry_targets=len(self._get_registry_target_map()))
                        artifact.generated_pyspark = rewritten

                # Medium confidence — let agentic loop fix TODOs only
                # We inject the converter's output as context for the agent
                log.info("deterministic_needs_repair", proc=proc, chunk=chunk,
                         confidence=artifact.confidence_score,
                         todo_count=artifact.todo_count)
                # Store the artifact so _c2_c4_agentic can use it
                self._deterministic_artifacts = getattr(self, '_deterministic_artifacts', {})
                self._deterministic_artifacts[f"{proc}/{chunk}"] = artifact
                return None  # Fall through to agentic with enhanced context

            else:
                # Low confidence or parse failure — fall through to full agentic
                log.info("deterministic_fallthrough", proc=proc, chunk=chunk,
                         confidence=artifact.confidence_score,
                         reason=artifact.conversion_path)
                # Still store preprocessed SQL for the agentic prompt
                self._deterministic_artifacts = getattr(self, '_deterministic_artifacts', {})
                self._deterministic_artifacts[f"{proc}/{chunk}"] = artifact
                return None

        except Exception as e:
            # Any error in the deterministic path → graceful fallback to agentic
            log.warning("deterministic_first_pass_error", proc=proc, chunk=chunk,
                        error=str(e)[:200])
            return None

    # =========================================================================
    # C2-C4 Agentic conversion (tool-calling loop)
    # =========================================================================

    def _c2_c4_agentic(self, task: DispatchTask, chunk_code: str,
                        prior_chunk_summary: str = "",
                        review_feedback: str = "") -> dict:
        """
        Run agentic tool-calling conversion for one chunk.

        Returns dict with common output envelope:
            success, converted_code, test_code, tool_calls, error,
            status, attempts, completion_summary, state_vars, best_code,
            budget_info, confidence
        """
        import json
        from sql_migration.core.tool_executor import get_conversion_tools

        strategy_val = task.strategy.value if hasattr(task.strategy, 'value') else task.strategy
        target = "Trino SQL" if strategy_val == "TRINO_SQL" else "PySpark Python"
        tools = get_conversion_tools(strategy_val)

        if self._is_query_mode:
            # ── Query mode: simple, focused prompt ────────────────────────
            system_prompt = f"""You are converting a {self._source_label} query to Trino SQL for a Starburst Trino lakehouse.

You have tools:
  - validate_sql(sql): Check Trino SQL syntax via sqlglot parser.
  - query_trino(sql): Run read-only SQL against the live Trino lakehouse.
    Use this to verify table schemas (DESCRIBE) or execute your converted query.
  - submit_result(converted_code): Submit your final Trino SQL.

IMPORTANT CONTEXT:
- The source SQL is written in {self._declared_dialect or self._source_engine or 'the source dialect'}. The target is Trino SQL.
- query_trino and DESCRIBE operate against the TARGET Trino lakehouse — not the source database.
- Column types from TABLE SCHEMA below are Trino types. The source may use different types — use DATA TYPE MAPPINGS to translate.

WORKFLOW:
1. Read the source query and convert all dialect-specific functions using the construct mappings.
2. Apply data type mappings — ensure column types match Trino types.
3. Use validate_sql to check syntax. If errors, fix and re-validate.
4. Use query_trino(your_sql + " LIMIT 1") to execute against live Trino and verify columns and schema are correct.
   If it fails, read the error message carefully and fix your SQL before submitting. Do NOT send multiple statements in one call.
5. Call submit_result with the final Trino SQL.

RULES:
- Output ONLY pure Trino SQL. No PySpark. No Python.
- Use construct mappings EXACTLY as provided. Do not invent alternatives.
- Use schema_context column names exactly — do not guess.
- When uncertain about the Trino equivalent of a {self._declared_dialect or 'source'} function, or unsure whether a Trino function exists or its exact syntax, search the web for official Trino documentation before guessing or marking as UNMAPPED.
- For UNMAPPED functions: emit a comment. Format: -- TODO: UNMAPPED — original: FUNC_NAME(args)
- If the source references a system dummy table (e.g. FROM DUAL), remove it — Trino supports SELECT expressions without a FROM clause.
"""

        else:
            # ── Proc mode: full agentic prompt ────────────────────────────
            if strategy_val == "TRINO_SQL":
                tool_instructions = (
                    "You have tools to validate and test your SQL:\n"
                    "  - validate_sql(sql): Check Trino SQL syntax via sqlglot parser.\n"
                    "  - query_trino(sql): Run read-only SQL against the live Trino lakehouse.\n"
                    "    Use this to verify table schemas (DESCRIBE) and test SELECT queries.\n"
                    "  - submit_result(converted_code, test_code, state_vars): Submit your final SQL.\n\n"
                    "Do NOT write PySpark/Python code. Output must be pure Trino SQL."
                )
                verify_step = "3. Use validate_sql to check syntax and query_trino to test against live data."
            else:
                tool_instructions = (
                    "You have tools to execute and test your code:\n"
                    "  - run_pyspark(code): Execute PySpark/Python in an isolated sandbox.\n"
                    "  - query_trino(sql): Run read-only SQL against the live Trino lakehouse.\n"
                    "    Use this to verify table schemas (DESCRIBE) and check column names.\n"
                    "  - submit_result(converted_code, test_code, state_vars): Submit your final code.\n\n"
                    "Do NOT submit raw SQL. Output must be PySpark Python code."
                )
                verify_step = "3. Use run_pyspark to execute your code and verify it works."

            system_prompt = f"""You are converting {self._source_label} source code to {target} for a Starburst Trino lakehouse migration.

{tool_instructions}

IMPORTANT CONTEXT:
- The source SQL is written in {self._declared_dialect or self._source_engine or 'the source dialect'}. The target is {target}.
- query_trino and DESCRIBE operate against the TARGET Trino lakehouse — not the source database.
- Column types from TABLE SCHEMA below are Trino types. The source may use different types — use DATA TYPE MAPPINGS to translate.

WORKFLOW:
1. Read the source SQL chunk and understand what it does.
2. Write the converted {target} code.
{verify_step}
4. If errors occur, fix them and re-test. If you get the same error twice,
   try a FUNDAMENTALLY DIFFERENT approach instead of small tweaks.
5. When the code passes, call submit_result with the final code.
   Include state_vars: a dict of variable/DataFrame names you defined in this chunk,
   e.g. {{"df_accounts": "DataFrame", "rate_tiers": "DataFrame", "total_count": "int"}}.
6. If you run out of budget, call submit_result with your BEST code so far.

RULES:
- Use construct mappings EXACTLY as provided. Do not invent alternatives.
- When uncertain about the Trino equivalent of a {self._declared_dialect or 'source'} function, or unsure whether a Trino function exists or its exact syntax, search the web for official Trino documentation before guessing or marking as UNMAPPED.
- For UNMAPPED functions: emit a TODO comment. Format: # TODO: UNMAPPED — original: FUNC_NAME(args)
- Use schema_context column names exactly — do not guess column names.
  If uncertain, use query_trino("DESCRIBE table_name") to verify.
- Do NOT re-declare variables listed in prior_chunk_variables.
- If the source references a system dummy table (e.g. FROM DUAL), remove it — Trino supports SELECT expressions without a FROM clause.
- This is chunk {task.chunk_index + 1} of {task.total_chunks} for '{task.proc_name}'.
"""

        callee_block = ""
        if task.callee_signatures:
            lines = ["CALLEE PROC SIGNATURES (already converted — call these):"]
            for name, sig in task.callee_signatures.items():
                first_line = sig.splitlines()[0] if sig else name
                lines.append(f"  {name} → {first_line}")
            callee_block = "\n".join(lines)

        udf_block = ""
        if task.udf_implementations:
            udf_lines = [
                "PRE-WRITTEN UDF IMPLEMENTATIONS (already written — use these EXACTLY, do NOT rewrite):"
            ]
            for udf in task.udf_implementations:
                udf_lines.append(f"\n# --- {udf.get('name', 'unknown')} ---")
                udf_lines.append(udf.get("code", ""))
            udf_block = "\n".join(udf_lines)

        write_block = ""
        if task.write_directives:
            w_lines = ["WRITE DIRECTIVES (MANDATORY):"]
            for tbl, directive in task.write_directives.items():
                w_lines.append(f"  {directive}")
            write_block = "\n".join(w_lines)

        warnings_block = ""
        if task.external_warnings:
            w_lines = ["⚠️ EXTERNAL DEPENDENCY WARNINGS:"]
            for w in task.external_warnings:
                w_lines.append(f"  - {w}")
            warnings_block = "\n".join(w_lines)

        semantic_block = f"\n{task.semantic_context}\n" if task.semantic_context else ""

        # Oracle→PySpark conversion guidance (only when Oracle dialect + PySpark strategy)
        oracle_pyspark_block = ""
        dialect_id = getattr(self, '_dialect_id', '') or ''
        if (dialect_id.lower() == "oracle"
                and strategy_val in ("PYSPARK_DF", "PYSPARK_PIPELINE")):
            oracle_pyspark_block = """
ORACLE → PYSPARK CONVERSION RULES:
- COMMIT/ROLLBACK: emit as comments (# COMMIT — Spark lakehouse has no explicit transactions)
- EXCEPTION WHEN OTHERS: convert to try/except Exception as e: with logging
- EXECUTE IMMEDIATE 'truncate table X': convert to spark.sql("TRUNCATE TABLE X")
- @Dbloans / @db_link references: emit TODO comment — # TODO: Replace @Dbloans with direct Trino table ref
- DBMS_STATS.GATHER_TABLE_STATS: emit spark.sql("ANALYZE TABLE schema.table COMPUTE STATISTICS")
- DBMS_SCHEDULER.CREATE_JOB / RUN_JOB: emit TODO comment — # TODO: Use Airflow/external scheduler
- DBMS_OUTPUT.PUT_LINE: convert to logger.info(...)
- Oracle hints (/*+ APPEND */, /*+ PARALLEL */): strip — Spark optimizes differently
- Status table INSERT/UPDATE (Lac_Mis_Archive_Status etc.): PRESERVE as spark.sql("INSERT/UPDATE ...") — business-critical logging
- ROWNUM < N: use ROW_NUMBER() OVER() with .limit(N) or WHERE rn <= N
- Package-qualified function calls (Pkg.proc()): call the function directly within the same module
- Each procedure should become a Python function: def proc_name(spark: SparkSession):
- Wrap DML in spark.sql("...") — Spark SQL supports INSERT/UPDATE/DELETE/TRUNCATE on lakehouse tables
- Import: from pyspark.sql import SparkSession, functions as F
"""

        # v10 Task 3: Type mappings and column-level analysis
        type_map_block = ""
        if self._type_mappings:
            type_map_block = "DATA TYPE MAPPINGS (source → Trino):\n" + "\n".join(
                f"  {k} → {v}" for k, v in sorted(self._type_mappings.items())
            ) + "\n"

        col_analysis_block = ""
        if self._column_analysis:
            # Filter to columns relevant to this chunk's tables
            chunk_tables = set(task.schema_context.keys()) if task.schema_context else set()
            relevant = [
                cf for cf in self._column_analysis
                if not chunk_tables or cf.get("table", "").lower() in {t.lower() for t in chunk_tables}
            ]
            if relevant:
                col_lines = ["COLUMN ANALYSIS (source functions applied to columns):"]
                for cf in relevant[:20]:  # Cap to avoid prompt bloat
                    col_lines.append(
                        f"  {cf.get('table','?')}.{cf.get('column','?')} "
                        f"({cf.get('source_type','?')}): "
                        f"{cf.get('expression','?')} → {cf.get('trino_equivalent','?')}"
                    )
                col_analysis_block = "\n".join(col_lines) + "\n"

        # Target-catalog routing block: explicit write-FQN allow-list from
        # Planning P3. Only emitted for chunks with at least one write-role
        # table (role ∈ {target, both}) when output_catalog is set.
        target_ctx = getattr(task, "target_context", None) or {}
        target_block = ""
        if target_ctx:
            lines = ["\nTARGET TABLE LOCATIONS (all writes MUST use these exact FQNs):"]
            for src_name, spec in target_ctx.items():
                if not isinstance(spec, dict):
                    continue
                tgt = spec.get("target_fqn", "")
                if not tgt:
                    continue
                role = spec.get("role", "target")
                status = spec.get("target_status", "")
                lines.append(f"  {src_name} → {tgt}  (role={role}, status={status})")
            lines.append(
                "Every INSERT INTO / UPDATE / DELETE FROM / MERGE INTO / CREATE TABLE AS / "
                "saveAsTable / insertInto / writeTo MUST reference one of the FQNs above — "
                "not the source FQN, not a bare table name, not an @dblink reference. "
                "READ statements keep using the TABLE SCHEMA FQNs."
            )
            target_block = "\n".join(lines) + "\n"

        # Query mode: simpler user content — no callee/udf/write/state blocks
        if self._is_query_mode:
            user_content = f"""Convert this SQL query to Trino SQL.

{semantic_block}CONSTRUCT MAPPINGS (source dialect → Trino SQL):
{json.dumps(task.construct_hints, indent=2) if task.construct_hints else "  (none)"}

{type_map_block}TABLE SCHEMA (exact Trino column names and types):
{json.dumps(task.schema_context, indent=2) if task.schema_context else "  (no tables)"}

{target_block}{col_analysis_block}
--- SOURCE QUERY (lines {task.start_line}–{task.end_line}) ---
{chunk_code}
--- END SOURCE QUERY ---

Convert, validate with validate_sql, execute with query_trino(sql + " LIMIT 1"), then call submit_result."""

        else:
            user_content = f"""Convert this SQL chunk to {target}.

{semantic_block}CONSTRUCT MAPPINGS (source dialect → {target}):
{json.dumps(task.construct_hints, indent=2) if task.construct_hints else "  (none)"}

{type_map_block}TABLE SCHEMA (exact Trino column names and types):
{json.dumps(task.schema_context, indent=2) if task.schema_context else "  (no tables)"}

{target_block}{col_analysis_block}PRIOR CHUNK VARIABLES (already exist — do NOT re-declare):
{json.dumps(task.state_vars, indent=2) if task.state_vars else "  (first chunk)"}

{callee_block}
{udf_block}
{write_block}
{warnings_block}
{oracle_pyspark_block}{f"REPLAN NOTES: {task.replan_notes}" if task.replan_notes else ""}

--- SOURCE SQL (lines {task.start_line}–{task.end_line}) ---
{chunk_code}
--- END SOURCE SQL ---

Convert, test with your tools, and call submit_result when done."""

        # Inject semantic review feedback if this is a re-conversion
        if review_feedback:
            user_content += (
                f"\n\n⚠️ SEMANTIC REVIEW FEEDBACK (from a prior conversion attempt):\n"
                f"{review_feedback}\n\n"
                f"Your previous conversion had the issues above. "
                f"Fix ALL identified issues. Do NOT replace source expressions with NULL or placeholder values. "
                f"If you cannot find an exact Trino equivalent, search the web for official Trino documentation. "
                f"Only mark as TODO if genuinely impossible to convert."
            )

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ]

        # Run the agentic loop
        ag = self.global_cfg.agentic
        if self._is_query_mode:
            # Query mode: smaller budget, no prior_chunk_summary,
            # only TRINO_SQL tools (validate_sql + query_trino + submit_result)
            budget = self.cfg.query_mode_budget
            max_turns = self.cfg.query_mode_budget
            loop_result = self._tool_executor.run_agentic_loop(
                llm_client=self.llm,
                messages=messages,
                tools=tools,
                max_tool_calls=budget,
                max_turns=max_turns,
                call_id=f"c2_{task.proc_name}_{task.chunk_id}",
                prior_chunk_summary="",
                tool_costs=ag.tool_costs,
                expected_strategy="TRINO_SQL",
            )
        else:
            corrections = self.cfg.max_chunk_self_corrections
            budget = corrections * ag.conversion_budget_multiplier + ag.conversion_budget_base
            max_turns = corrections * ag.conversion_turns_multiplier + ag.conversion_turns_base
            loop_result = self._tool_executor.run_agentic_loop(
                llm_client=self.llm,
                messages=messages,
                tools=tools,
                max_tool_calls=budget,
                max_turns=max_turns,
                call_id=f"c2_{task.proc_name}_{task.chunk_id}",
                prior_chunk_summary=prior_chunk_summary,
                tool_costs=ag.tool_costs,
                expected_strategy=strategy_val,
            )

        # Build common result envelope
        if loop_result.success and loop_result.terminal_data:
            data = loop_result.terminal_data
            return {
                "success": True,
                "converted_code": data.get("converted_code", ""),
                "test_code": data.get("test_code", ""),
                "tool_calls": loop_result.tool_calls_made,
                "error": "",
                "status": loop_result.status,
                "attempts": [
                    {"attempt_number": a.attempt_number, "error": a.error[:200],
                     "success": a.success, "code_lines": a.code_lines}
                    for a in loop_result.attempts
                ],
                "completion_summary": loop_result.completion_summary,
                "state_vars": loop_result.state_vars,
                "best_code": loop_result.best_code,
                "budget_info": f"{loop_result.budget_used}/{budget}",
                "confidence": loop_result.confidence,
            }
        else:
            return {
                "success": False,
                "converted_code": "",
                "test_code": "",
                "tool_calls": loop_result.tool_calls_made,
                "error": loop_result.error,
                "status": loop_result.status,
                "attempts": [
                    {"attempt_number": a.attempt_number, "error": a.error[:200],
                     "success": a.success, "code_lines": a.code_lines}
                    for a in loop_result.attempts
                ],
                "completion_summary": loop_result.completion_summary,
                "state_vars": loop_result.state_vars,
                "best_code": loop_result.best_code,
                "budget_info": f"{loop_result.budget_used}/{budget}",
                "confidence": loop_result.confidence,
            }

    # =========================================================================
    # Step C1 — Chunk extraction
    # =========================================================================

    def _c1_extract_chunk(self, task: DispatchTask) -> str:
        """
        Extract the raw source lines for this chunk using line boundaries.
        Applies light preprocessing: inline table annotations.

        v7: For multi-proc modules, determines the correct source file for
        this chunk by matching start_line/end_line against source_ranges.
        If the chunk spans a single source proc, extracts from that proc's file.
        """
        # v7: Find the correct source file for this chunk.
        # In module mode, different chunks may come from different source files.
        source_file = task.source_file  # default: primary source file
        if task.source_ranges:
            for sr in task.source_ranges:
                if (sr.get("start_line", -1) <= task.start_line
                        and task.end_line <= sr.get("end_line", -1)):
                    source_file = sr.get("source_file", source_file)
                    break

        # Merge target-catalog routing info into schema_context so the
        # extractor can emit role-aware FQN annotations (read vs write).
        # Each entry may gain `target_fqn` + `role` keys.
        merged_schema_ctx = dict(task.schema_context or {})
        for src_name, spec in (getattr(task, "target_context", None) or {}).items():
            if not isinstance(spec, dict) or not spec.get("target_fqn"):
                continue
            base = dict(merged_schema_ctx.get(src_name, {}))
            base["target_fqn"] = spec.get("target_fqn", "")
            base["role"]       = spec.get("role", "target")
            # Keep the original trino_fqn for read references.
            if "trino_fqn" not in base and spec.get("source_fqn"):
                base["trino_fqn"] = spec["source_fqn"]
            merged_schema_ctx[src_name] = base

        result = self.sandbox.run(
            script="extraction/extract_structure.py",  # Reuse existing script
            args={
                "mode":         "extract_chunk",
                "source_file":  source_file,
                "start_line":   task.start_line,
                "end_line":     task.end_line,
                "schema_context": merged_schema_ctx,
            },
        )

        if result.success:
            try:
                data = result.stdout_json
                return data.get("chunk_code", "")
            except Exception:
                pass

        # Fallback: read file directly if sandbox unavailable
        try:
            path = Path(source_file)
            if path.exists():
                lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
                chunk = lines[task.start_line:task.end_line + 1]
                return "\n".join(chunk)
        except Exception as e:
            log.error("c1_extract_failed",
                      proc=task.proc_name, error=str(e))

        return ""

    def _c3_5_run_test(self, task: DispatchTask, production_code: str) -> list[str]:
        """
        Execute the LLM-generated test script in the PySpark sandbox.

        The test is self-contained: creates synthetic data, injects the
        production code, and prints TEST_PASS or TEST_FAIL: {reason}.

        Returns list of error strings (empty on TEST_PASS).
        """
        test_code = self._current_test
        if not test_code:
            return []

        result = self.sandbox.run(
            script="conversion/validate_conversion.py",
            args={
                "mode":             "exec_test",
                "converted_code":   production_code,
                "test_code":        test_code,
                "strategy":         task.strategy.value,
            },
        )

        if not result.success:
            # Sandbox execution itself failed (timeout, OOM, etc.)
            stderr = result.stderr[:300] if result.stderr else "unknown"
            return [f"TEST_EXEC: Sandbox test execution failed: {stderr}"]

        try:
            data = result.stdout_json
            test_result = data.get("test_result", "")
            runtime_error = data.get("runtime_error")

            if runtime_error:
                return [f"TEST_RUNTIME: {runtime_error}"]

            if "TEST_FAIL" in test_result:
                # Extract the failure reason
                reason = test_result.split("TEST_FAIL:", 1)[-1].strip() \
                         if "TEST_FAIL:" in test_result else test_result
                return [f"TEST_FAIL: {reason}"]

            if "TEST_PASS" in test_result:
                log.info("c3_5_test_passed",
                         proc=task.proc_name, chunk=task.chunk_id)
                return []

            # Ambiguous output — test didn't print expected markers
            return [f"TEST_AMBIGUOUS: Test did not print TEST_PASS or TEST_FAIL. Output: {test_result[:200]}"]

        except Exception as e:
            return [f"TEST_EXEC: Could not parse test output: {e}"]

    # =========================================================================
    # Step C5 — Assembly + integration check
    # =========================================================================

    def _c5_assemble(self, task: DispatchTask) -> dict:
        """
        Assemble all chunks for a proc into the final output file.
        Only called when the last chunk has successfully passed C3.

        v10-fix (Bug 1): On crash-resume, _chunk_codes only has chunks
        processed in THIS run. Earlier chunks (completed before crash) are
        on disk as result_{proc}_{chunk}.json. We reload any missing chunks
        from those files before assembly.
        """
        proc = task.proc_name
        chunks_data = dict(self._chunk_codes.get(proc, {}))

        # Reload missing chunks from persisted result files (crash-resume)
        plan_data = self.store.read("planning", "plan.json")
        plan_chunks = plan_data["procs"][proc]["chunks"]
        missing_reloaded = 0
        for pc in plan_chunks:
            cid = pc["chunk_id"]
            if cid in chunks_data and chunks_data[cid]:
                continue  # Already in memory
            # Try to reload from disk
            result_file = f"result_{proc}_{cid}.json"
            if self.store.exists("conversion", result_file):
                try:
                    result_data = self.store.read("conversion", result_file)
                    code = result_data.get("converted_code", "")
                    if code:
                        chunks_data[cid] = code
                        missing_reloaded += 1
                        log.info("c5_chunk_reloaded_from_disk",
                                 proc=proc, chunk=cid,
                                 code_lines=len(code.splitlines()))
                except Exception as e:
                    log.warning("c5_chunk_reload_failed",
                                proc=proc, chunk=cid, error=str(e)[:100])

        if missing_reloaded:
            log.info("c5_resume_recovery",
                     proc=proc, reloaded=missing_reloaded,
                     total=len(plan_chunks))
            # Update in-memory cache so _write_conversion_log also sees them
            self._chunk_codes[proc] = chunks_data

        if not chunks_data:
            return {"status": "INTEGRATION_FAIL", "error": "No chunk codes found"}

        # Determine output path and extension
        ext = ".sql" if task.strategy == ConversionStrategy.TRINO_SQL else ".py"
        output_path = str(self.store.path("conversion", f"proc_{proc}{ext}"))

        # Build chunk list in order
        plan_data = self.store.read("planning", "plan.json")
        plan_chunks = plan_data["procs"][proc]["chunks"]
        ordered_chunks = []
        for pc in plan_chunks:
            cid  = pc["chunk_id"]
            code = chunks_data.get(cid, "")
            ordered_chunks.append({"chunk_id": cid, "converted_code": code})

        result = self.sandbox.run(
            script="conversion/assemble_proc.py",
            args={
                "proc_name":   proc,
                "strategy":    task.strategy.value,
                "chunks":      ordered_chunks,
                "output_path": output_path,
            },
        )

        if not result.success:
            return {
                "status": "INTEGRATION_FAIL",
                "error":  f"Assembly script failed: {result.stderr[:300]}",
            }

        try:
            data = result.stdout_json
            # If sandbox wrote the file inside container, write it from here too
            if data.get("status") == "PASS" and data.get("assembled_code"):
                assembled_code = data["assembled_code"]
                self.store.write_code(
                    "conversion",
                    f"proc_{proc}{ext}",
                    assembled_code,
                )

                # ── v7: Run module integration test against assembled code ────
                # The last chunk's C2 call produced a module-level integration
                # test (if multi-chunk). Run it against the ASSEMBLED code to
                # catch cross-chunk variable mismatches before V3.
                if (self.cfg.sandbox_validation.run_test_execution
                        and hasattr(self, '_current_test')
                        and self._current_test
                        and task.total_chunks > 1):
                    test_errors = self._c3_5_run_test(task, assembled_code)
                    if test_errors:
                        log.warning("c5_integration_test_failed",
                                    proc=proc,
                                    errors=test_errors[:2])
                        return {
                            "status": "INTEGRATION_FAIL",
                            "error": f"Module integration test failed: {'; '.join(test_errors[:2])}",
                        }
                    log.info("c5_integration_test_passed", proc=proc)

                # v6-fix (Gap 4): Extract and persist structured callee interface.
                # This replaces the fragile 4-line def extraction in the Orchestrator
                # with a machine-readable interface that captures params, return type,
                # tables read/written, and strategy.
                try:
                    from sql_migration.models.pipeline import extract_callee_interface
                    # Get tables from the plan entry
                    plan_data = self.store.read("planning", "plan.json")
                    proc_plan = plan_data.get("procs", {}).get(proc, {})
                    tables_read = []
                    tables_written = proc_plan.get("tables_written", [])
                    for chunk_info in proc_plan.get("chunks", []):
                        tables_read.extend(chunk_info.get("tables", []))
                    tables_read = sorted(set(tables_read))

                    interface = extract_callee_interface(
                        proc_name=proc,
                        assembled_code=assembled_code,
                        strategy=task.strategy.value,
                        tables_read=tables_read,
                        tables_written=tables_written,
                    )
                    self.store.write(
                        "conversion",
                        f"callee_interface_{proc}.json",
                        interface.model_dump(),
                    )
                except Exception as e:
                    log.debug("callee_interface_extraction_failed",
                              proc=proc, error=str(e)[:100])

            return data
        except Exception as e:
            return {"status": "INTEGRATION_FAIL", "error": str(e)}

    # =========================================================================
    # Conversion log
    # =========================================================================

    def _write_conversion_log(
        self,
        task:            DispatchTask,
        chunk_result:    ChunkConversionResult,
        assembly_result: dict,
    ) -> None:
        """Write conversion_log_{proc}.json to the artifact store."""
        proc = task.proc_name

        # Collect all todos across all chunks
        all_todos: list[dict] = []
        for chunk_id, code in self._chunk_codes.get(proc, {}).items():
            _, todos = self._count_todos(code)
            all_todos.extend(todos)

        log_entry = ConversionLog(
            proc_name=proc,
            strategy=task.strategy,
            chunks=[chunk_result],
            todos=[TodoItem(**t) if isinstance(t, dict) else t for t in all_todos],
            warnings=assembly_result.get("warnings", []),
            total_self_corrections=chunk_result.self_correction_attempts,
            assembled=assembly_result.get("status") == "PASS",
            assembly_error=assembly_result.get("error", ""),
        )
        self.store.write(
            "conversion",
            f"conversion_log_{proc}.json",
            log_entry.model_dump(),
        )

    # =========================================================================
    # Helpers
    # =========================================================================

    def _validate_test_quality(self, test_code: str, task: DispatchTask) -> str:
        """
        Validate LLM-generated test quality. Discard trivial tests that
        would give false confidence in C5 integration testing.

        A test is considered trivial if it:
          - Has no assertions (assert, assertEqual, assertTrue, etc.)
          - Has no data creation (DataFrame, createDataFrame, spark.sql)
          - Catches all exceptions and prints TEST_PASS regardless

        Returns:
            test_code if quality passes, empty string if trivial.
        """
        import re
        lines = test_code.strip().splitlines()
        if len(lines) < 3:
            log.debug("test_quality_too_short",
                      proc=task.proc_name, lines=len(lines))
            return ""

        has_assertion = bool(re.search(
            r'\bassert\b|assertEqual|assertTrue|assertFalse|assertRaises|'
            r'TEST_FAIL|raise\s',
            test_code,
        ))
        has_data = bool(re.search(
            r'createDataFrame|DataFrame|spark\.sql|\.collect\(\)|'
            r'\.show\(\)|\.count\(\)',
            test_code,
        ))
        # Catch-all pattern: except Exception ... TEST_PASS with no real logic
        has_catch_all = bool(re.search(
            r'except\s*(?:Exception|\w*Error).*?TEST_PASS',
            test_code, re.DOTALL,
        ))

        if has_catch_all and not has_assertion:
            log.info("test_quality_catch_all_rejected",
                     proc=task.proc_name, chunk=task.chunk_id)
            return ""

        if not has_assertion and not has_data:
            log.info("test_quality_no_assertions_or_data",
                     proc=task.proc_name, chunk=task.chunk_id)
            return ""

        return test_code

    def _count_todos(self, code: str) -> tuple[int, list[dict]]:
        """Count TODO markers in converted code."""
        import re
        todos = []
        pattern = re.compile(r'(?:#|--)\s*TODO\s*:?\s*UNMAPPED.*', re.IGNORECASE)
        for i, line in enumerate(code.splitlines(), start=1):
            m = pattern.search(line)
            if m:
                fn_match = re.search(r'original:\s*(\w+)', line, re.IGNORECASE)
                todos.append({
                    "line":           i,
                    "comment":        line.strip(),
                    "original_fn":    fn_match.group(1) if fn_match else "",
                    "construct_type": "UNMAPPED_CONSTRUCT",
                })
        return len(todos), todos

    # =========================================================================
    # Deterministic Query Conversion Pipeline (TRINO_SQL query mode only)
    # =========================================================================

    def _convert_query_deterministic(
        self,
        task: DispatchTask,
        chunk_code: str,
        start: float,
    ) -> ChunkConversionResult:
        """
        Deterministic conversion pipeline for TRINO_SQL query mode.

        Step 1: LLM converts + tests via query_trino (self-correction loop)
        Step 2: Semantic review (source vs converted comparison)
                If issues → re-enter Step 1 with feedback (max 2 cycles)
        Step 3: Accept → write file

        On failure: returns CONVERSION_FAILED with empty code.
        Other queries continue unaffected.
        """
        proc = task.proc_name
        chunk = task.chunk_id
        max_review_cycles = 2
        total_tool_calls = 0
        review_feedback = ""

        for cycle in range(1, max_review_cycles + 1):
            # ── Step 1: Convert + Test ────────────────────────────────────
            self._write_step_progress(proc, chunk, "C2",
                f"Converting + testing (cycle {cycle})...")

            step1 = self._step1_convert_and_test(
                task, chunk_code, review_feedback=review_feedback)
            total_tool_calls += step1.get("tool_calls", 0)

            if not step1.get("success") or not step1.get("converted_code"):
                log.warning("step1_conversion_failed",
                            proc=proc, cycle=cycle,
                            error=step1.get("error", "No converted code"))
                return ChunkConversionResult(
                    proc_name=proc, chunk_id=chunk,
                    status="CONVERSION_FAILED",
                    converted_code="",
                    errors=[step1.get("error", "Conversion failed — no valid SQL produced")],
                    self_correction_attempts=total_tool_calls,
                    duration_s=time.monotonic() - start,
                )

            converted_code = step1["converted_code"]

            # ── Step 2: Semantic Review ───────────────────────────────────
            self._write_step_progress(proc, chunk, "REVIEW",
                f"Semantic review (cycle {cycle})...")

            review = self._semantic_review(
                chunk_code, converted_code,
                strategy=task.strategy.value if hasattr(task.strategy, 'value') else str(task.strategy))

            # Persist review result for debugging/download
            self.store.write("conversion", f"semantic_review_{proc}.txt", (
                f"=== Semantic Review — {proc} (cycle {cycle}) ===\n"
                f"Result: {'ISSUES FOUND' if review['issues_found'] else 'PASSED'}\n\n"
                f"--- Source SQL ---\n{chunk_code}\n\n"
                f"--- Converted SQL ---\n{converted_code}\n\n"
                f"--- Review Feedback ---\n{review.get('feedback', 'No issues found.')}\n"
            ), wrap=False)

            if not review["issues_found"]:
                # Clean — accept
                log.info("query_conversion_accepted",
                         proc=proc, cycle=cycle,
                         tool_calls=total_tool_calls)
                break

            log.warning("semantic_review_issues",
                        proc=proc, cycle=cycle,
                        feedback=review["feedback"][:200])

            if cycle == max_review_cycles:
                # Exhausted — fail this query
                log.error("semantic_review_exhausted",
                          proc=proc, cycles=max_review_cycles,
                          feedback=review["feedback"][:300])
                return ChunkConversionResult(
                    proc_name=proc, chunk_id=chunk,
                    status="CONVERSION_FAILED",
                    converted_code="",
                    errors=[
                        f"Semantic review failed after {max_review_cycles} cycles: "
                        f"{review['feedback'][:300]}"
                    ],
                    self_correction_attempts=total_tool_calls,
                    duration_s=time.monotonic() - start,
                )

            # Feed issues back for next cycle
            review_feedback = review["feedback"]

        # ── Step 3: Accept → write file ───────────────────────────────────
        self._write_step_progress(proc, chunk, "ACCEPT",
            f"Conversion complete ({total_tool_calls} tool calls)", status="DONE")
        todo_count, todo_items = self._count_todos(converted_code)
        self.store.write_code("conversion", f"proc_{proc}.sql", converted_code)
        log.info("query_converted",
                 query=proc, lines=len(converted_code.splitlines()),
                 corrections=total_tool_calls, todos=todo_count)
        return ChunkConversionResult(
            proc_name=proc,
            chunk_id=chunk,
            status="SUCCESS",
            converted_code=converted_code,
            todo_count=todo_count,
            todo_items=[TodoItem(**t) if isinstance(t, dict) else t for t in todo_items],
            self_correction_attempts=total_tool_calls,
            duration_s=time.monotonic() - start,
        )

    def _step1_convert_and_test(
        self,
        task: DispatchTask,
        chunk_code: str,
        review_feedback: str = "",
    ) -> dict:
        """
        Step 1: LLM converts source SQL to Trino and tests via query_trino.

        The LLM has ONE tool: query_trino. It converts, tests its SQL against
        live Trino, self-corrects on errors. When done it returns the final SQL
        as text (no more tool calls).

        Returns: {"success": bool, "converted_code": str, "tool_calls": int, "error": str}
        """
        import json

        # ── Build context blocks ──────────────────────────────────────────
        semantic_block = f"\n{task.semantic_context}\n" if task.semantic_context else ""

        type_map_block = ""
        if self._type_mappings:
            type_map_block = "DATA TYPE MAPPINGS (source → Trino):\n" + "\n".join(
                f"  {k} → {v}" for k, v in sorted(self._type_mappings.items())
            ) + "\n"

        col_analysis_block = ""
        if self._column_analysis:
            chunk_tables = set(task.schema_context.keys()) if task.schema_context else set()
            relevant = [
                cf for cf in self._column_analysis
                if not chunk_tables or cf.get("table", "").lower() in {t.lower() for t in chunk_tables}
            ]
            if relevant:
                col_lines = ["COLUMN ANALYSIS (source functions applied to columns):"]
                for cf in relevant[:20]:
                    col_lines.append(
                        f"  {cf.get('table','?')}.{cf.get('column','?')} "
                        f"({cf.get('source_type','?')}): "
                        f"{cf.get('expression','?')} → {cf.get('trino_equivalent','?')}"
                    )
                col_analysis_block = "\n".join(col_lines) + "\n"

        # ── System prompt ─────────────────────────────────────────────────
        system_prompt = f"""You are converting a {self._source_label} query to Trino SQL for a Starburst Trino lakehouse.

You have ONE tool:
  - query_trino(sql): Execute read-only SQL against the live Trino lakehouse.
    Use this to DESCRIBE tables, check column names, and TEST your converted query.

IMPORTANT CONTEXT:
- The source SQL is written in {self._declared_dialect or self._source_engine or 'the source dialect'}. The target is Trino SQL.
- query_trino operates against the TARGET Trino lakehouse — not the source database.
- Column types from TABLE SCHEMA below are Trino types. The source may use different types — use DATA TYPE MAPPINGS to translate.

WORKFLOW:
1. Read the source query and convert ALL dialect-specific functions using the construct mappings.
2. Ensure EVERY source column expression has a proper Trino equivalent. Do NOT replace expressions with NULL.
3. Call query_trino(your_converted_sql + ' LIMIT 1') to test against live Trino.
4. If query_trino returns an error, read the error carefully, fix your SQL, and test again.
5. When the query executes successfully, return your FINAL Trino SQL inside a ```sql code block.

RULES:
- Output ONLY pure Trino SQL. No PySpark. No Python.
- Use construct mappings EXACTLY as provided. Do not invent alternatives.
- Use schema_context column names exactly — do not guess.
- NEVER replace a source expression with NULL. If you cannot convert a function, search the web for the Trino equivalent.
- When uncertain about the Trino equivalent of a {self._declared_dialect or 'source'} function, or unsure whether a Trino function exists or its exact syntax, search the web for official Trino documentation before guessing or marking as UNMAPPED.
- For truly UNMAPPED functions: emit a comment. Format: -- TODO: UNMAPPED — original: FUNC_NAME(args)
- Do NOT send multiple statements in one query_trino call (no semicolons).
- If the source references a system dummy table (e.g. FROM DUAL), remove it — Trino supports SELECT expressions without a FROM clause.

When you are done testing and satisfied, return ONLY the final SQL in a ```sql code block. No other text after the code block."""

        # ── User content ──────────────────────────────────────────────────
        user_content = f"""Convert this SQL query to Trino SQL.

{semantic_block}CONSTRUCT MAPPINGS (source dialect → Trino SQL):
{json.dumps(task.construct_hints, indent=2) if task.construct_hints else "  (none)"}

{type_map_block}TABLE SCHEMA (exact Trino column names and types):
{json.dumps(task.schema_context, indent=2) if task.schema_context else "  (no tables)"}

{col_analysis_block}
--- SOURCE QUERY (lines {task.start_line}–{task.end_line}) ---
{chunk_code}
--- END SOURCE QUERY ---"""

        if review_feedback:
            user_content += (
                f"\n\n⚠️ SEMANTIC REVIEW FEEDBACK (your previous conversion had these issues):\n"
                f"{review_feedback}\n\n"
                f"Fix ALL identified issues. Do NOT replace source expressions with NULL. "
                f"If you cannot find an exact Trino equivalent, search the web for official Trino documentation."
            )

        user_content += "\n\nConvert, test with query_trino, then return your final SQL in a ```sql block."

        # ── Tool definition (query_trino only) ────────────────────────────
        tools = [{
            "type": "function",
            "function": {
                "name": "query_trino",
                "description": (
                    "Execute a read-only SQL query against the live Trino lakehouse. "
                    "Use to DESCRIBE tables, check column names, test converted queries. "
                    "DML is blocked. Returns rows/columns or an error message."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "sql": {
                            "type": "string",
                            "description": "Read-only SQL query to execute",
                        },
                    },
                    "required": ["sql"],
                },
            },
        }]

        # ── Mini agentic loop ─────────────────────────────────────────────
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ]

        budget = self.cfg.query_mode_budget or 8
        tool_calls_made = 0
        call_id = f"dq_{task.proc_name}"

        for turn in range(budget + 2):  # +2 for final text turns
            try:
                response = self.llm.call_with_tools(
                    messages=messages, tools=tools, call_id=f"{call_id}_t{turn}")
            except Exception as e:
                log.error("step1_llm_error", proc=task.proc_name, error=str(e)[:200])
                return {"success": False, "converted_code": "", "tool_calls": tool_calls_made,
                        "error": f"LLM call failed: {str(e)[:200]}"}

            message = response.choices[0].message

            if not message.tool_calls:
                # LLM returned text — extract SQL
                text = message.content or ""
                sql = self._extract_sql_from_response(text)
                if sql:
                    return {"success": True, "converted_code": sql,
                            "tool_calls": tool_calls_made, "error": ""}
                else:
                    # No SQL found in response — might be an explanation
                    log.warning("step1_no_sql_in_response",
                                proc=task.proc_name, text=text[:200])
                    # Ask LLM to provide the SQL
                    messages.append({"role": "assistant", "content": text})
                    messages.append({"role": "user",
                                     "content": "Please return your converted Trino SQL inside a ```sql code block."})
                    continue

            # Process tool calls
            for tc in message.tool_calls:
                if tc.function.name != "query_trino":
                    continue

                tool_calls_made += 1
                try:
                    args = json.loads(tc.function.arguments) if tc.function.arguments else {}
                except json.JSONDecodeError:
                    args = {}

                sql_arg = args.get("sql", "")
                log.debug("step1_query_trino", proc=task.proc_name, sql=sql_arg[:100])

                # Execute via MCP (raw — errors flow back to LLM)
                try:
                    result = self.mcp.run_query_raw(sql_arg)
                    result_str = json.dumps(result, default=str)
                    if len(result_str) > 8000:
                        result_str = result_str[:8000] + "\n... (truncated)"
                except Exception as e:
                    result_str = json.dumps({"error": str(e)[:500]})

                # Append to conversation
                messages.append({
                    "role": "assistant", "content": None,
                    "tool_calls": [{
                        "id": tc.id, "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments or "{}",
                        },
                    }],
                })
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result_str,
                })

            if tool_calls_made >= budget:
                # Budget exhausted — ask for final answer
                messages.append({
                    "role": "user",
                    "content": "Budget exhausted. Return your BEST Trino SQL now in a ```sql code block.",
                })

        # Fell through — no valid SQL after all turns
        return {"success": False, "converted_code": "", "tool_calls": tool_calls_made,
                "error": "Conversion exhausted budget without producing valid SQL"}

    def _extract_sql_from_response(self, text: str) -> str:
        """Extract SQL from LLM response. Looks for ```sql blocks, falls back to raw text."""
        import re

        # Try ```sql ... ``` blocks
        pattern = re.compile(r'```sql\s*\n(.*?)```', re.DOTALL | re.IGNORECASE)
        matches = pattern.findall(text)
        if matches:
            # Return the last (most refined) SQL block
            return matches[-1].strip()

        # Try generic ``` ... ``` blocks
        pattern = re.compile(r'```\s*\n(.*?)```', re.DOTALL)
        matches = pattern.findall(text)
        if matches:
            sql = matches[-1].strip()
            # Verify it looks like SQL
            if any(kw in sql.upper() for kw in ("SELECT", "WITH", "INSERT", "CREATE")):
                return sql

        # Fallback: if the response looks like raw SQL (starts with SELECT/WITH)
        stripped = text.strip()
        if stripped.upper().startswith(("SELECT", "WITH")):
            return stripped

        return ""

    def _get_registry_target_map(self) -> dict[str, str]:
        """Lazy-load the run-wide target_fqn map from table_registry.json.

        Loaded on first access because the registry is written by the
        Analysis Agent *after* this Conversion Agent is instantiated.
        Empty map if the registry doesn't exist or output_catalog wasn't
        configured (no entries will have role=target/both).
        """
        if self._registry_target_map is not None:
            return self._registry_target_map
        rm: dict[str, str] = {}
        try:
            reg = self.store.read("analysis", "table_registry.json")
            for src_name, entry in (reg.get("entries") or {}).items():
                if not isinstance(entry, dict):
                    continue
                if entry.get("role") not in ("target", "both"):
                    continue
                tgt = entry.get("target_fqn") or ""
                if not tgt:
                    continue
                rm[src_name.lower()] = tgt
                bare = src_name.split(".")[-1].lower()
                if bare and bare not in rm:
                    rm[bare] = tgt
        except Exception:
            pass
        self._registry_target_map = rm
        return rm

    def _rewrite_write_targets(self, code: str, target_context: dict) -> str:
        """Rewrite INSERT/UPDATE/DELETE/MERGE/CTAS targets + PySpark writer
        calls in `code` so they point at the target-catalog FQN listed in
        `target_context` (instead of the source FQN that the deterministic
        converter emits verbatim).

        Mapping key = the source table name as found in target_context
        (e.g. `Mis.Lac_Mis_Archive` → `lz_lakehouse.lm_target_schema.lac_mis_archive`).
        Falls back to the full run-wide registry map so A2 misclassifications
        (e.g. staging table tagged TEMP_TABLE, dropped from chunk.tables)
        don't leak through as un-routed writes.

        The rewrite is done per write-verb occurrence only — READ references
        are left alone. `@dblink` suffixes are tolerated.
        """
        if not code:
            return code

        # Build a bare-name → target_fqn lookup.  Start from the chunk's
        # per-chunk target_context, then fill in any remaining registry
        # targets as a safety net.
        name_map: dict[str, str] = {}
        for src_name, spec in (target_context or {}).items():
            if not isinstance(spec, dict):
                continue
            tgt = spec.get("target_fqn", "")
            if not tgt:
                continue
            name_map[src_name.lower()] = tgt
            bare = src_name.split(".")[-1].lower()
            if bare and bare not in name_map:
                name_map[bare] = tgt

        for key, tgt in self._get_registry_target_map().items():
            name_map.setdefault(key, tgt)

        if not name_map:
            return code

        def _replace_target(match: "re.Match[str]") -> str:
            verb = match.group(1)
            raw_name = match.group(2)
            # Strip a trailing @dblink + compare case-insensitively against
            # both the full name and the bare tail.
            stripped = re.sub(r"@[\w$]+$", "", raw_name).strip().rstrip(";,)")
            tgt = (
                name_map.get(stripped.lower())
                or name_map.get(stripped.split(".")[-1].lower())
            )
            if not tgt:
                return match.group(0)
            # Preserve original whitespace after the verb.
            whitespace = match.group(0).split(raw_name, 1)[0]
            return f"{whitespace}{tgt}"

        # SQL write verbs: INSERT INTO / UPDATE / DELETE FROM / MERGE INTO
        code = re.sub(
            r"(INSERT\s+INTO|UPDATE|DELETE\s+FROM|MERGE\s+INTO)\s+([\w\.]+(?:@[\w$]+)?)",
            _replace_target,
            code,
            flags=re.IGNORECASE,
        )

        # CREATE TABLE … AS  (mirror the _replace_target / _replace_df_target
        # pattern above so the raw regex stays OUT of the f-string expression
        # — Python 3.10/3.11 reject backslashes inside f-string braces).
        def _replace_ctas_target(m: "re.Match[str]") -> str:
            head, raw_name, tail = m.group(1), m.group(2), m.group(3)
            stripped = re.sub(r"@[\w$]+$", "", raw_name)
            tgt = (
                name_map.get(stripped.lower())
                or name_map.get(stripped.split(".")[-1].lower())
                or raw_name
            )
            return f"{head} {tgt}{tail}"

        code = re.sub(
            r"(CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?)\s+([\w\.]+(?:@[\w$]+)?)(\s+AS\b)",
            _replace_ctas_target,
            code,
            flags=re.IGNORECASE,
        )

        # PySpark DataFrame writers: .saveAsTable("X") / .insertInto("X") / .writeTo("X")
        def _replace_df_target(match: "re.Match[str]") -> str:
            method, raw_name = match.group(1), match.group(2)
            stripped = re.sub(r"@[\w$]+$", "", raw_name).strip()
            tgt = (
                name_map.get(stripped.lower())
                or name_map.get(stripped.split(".")[-1].lower())
            )
            if not tgt:
                return match.group(0)
            return f'.{method}("{tgt}")'

        code = re.sub(
            r'\.(saveAsTable|insertInto|writeTo)\(\s*["\']([\w\.]+(?:@[\w$]+)?)["\']\s*\)',
            _replace_df_target,
            code,
        )

        return code

    def _write_deterministic_proc_file(self, task, converted_code: str) -> None:
        """Write the `proc_{name}.{py|sql}` file expected by the Validation
        Agent for single-attempt deterministic-accept paths.

        The normal C5 assembly step writes this file, but the deterministic-
        accept paths return SUCCESS *before* C5 runs. Without this hook the
        validation agent reports `"No converted code found"` and every
        deterministic-accepted proc ends up FROZEN.
        """
        try:
            ext = ".sql" if task.strategy.value == "TRINO_SQL" else ".py"
            filename = f"proc_{task.proc_name}{ext}"
            self.store.write_code("conversion", filename, converted_code)
            log.info("deterministic_proc_file_written",
                     proc=task.proc_name,
                     filename=filename,
                     lines=len(converted_code.splitlines()))
        except Exception as e:
            log.warning("deterministic_proc_file_write_failed",
                        proc=task.proc_name, error=str(e)[:200])

    def _targeted_repair(self, source_code: str, converted_code: str,
                         feedback: str, strategy: str) -> str | None:
        """Tier-3 resume-on-retry: single-shot LLM repair.

        Replaces the expensive "re-run the full agentic loop with feedback"
        path (~90-120 s, 5-8 tool calls) with one focused non-tool-use call
        (~10 s). The LLM gets source + current converted + specific review
        issues and returns the corrected code.

        Returns the repaired code or None if the repair failed/invalid.
        """
        target_label = "PySpark Python" if "PYSPARK" in strategy.upper() else "Trino SQL"
        prompt = f"""You are fixing a specific set of issues in an auto-generated {target_label} migration of Oracle PL/SQL. Do NOT rewrite the whole program — apply the minimum change required to resolve EACH listed issue while preserving everything else.

--- SOURCE Oracle PL/SQL ---
{source_code}
--- END SOURCE ---

--- CURRENT CONVERSION ({target_label}) ---
{converted_code}
--- END CURRENT ---

--- REVIEW ISSUES TO FIX ---
{feedback}
--- END ISSUES ---

Output ONLY the corrected {target_label} program — no markdown fences, no prose, no explanation, just the runnable code. Preserve all @track_status decorators, function signatures, imports, step_no tracking, and any existing spark.sql blocks that are already correct. Fix only what the review flagged."""
        try:
            result = self.llm.call(
                messages=[{"role": "user", "content": prompt}],
                expect_json=False,
                call_id="targeted_repair",
            )
            if not result:
                return None
            repaired = result.strip() if isinstance(result, str) else str(result)
            # Strip any stray markdown fences
            repaired = re.sub(r"^```(?:python|sql)?\s*\n?", "", repaired)
            repaired = re.sub(r"\n?```\s*$", "", repaired)
            if len(repaired) < 50:  # suspiciously small, likely empty
                return None
            return repaired
        except Exception as e:
            log.warning("targeted_repair_failed", error=str(e)[:200])
            return None

    @staticmethod
    def _review_signature(feedback: str) -> str:
        """Compact signature for a review feedback to detect "same issue recurring"."""
        # First 80 chars of lowercased feedback with collapsed whitespace.
        import hashlib
        cleaned = re.sub(r"\s+", " ", feedback.lower()).strip()
        return hashlib.md5(cleaned[:200].encode()).hexdigest()[:10]

    def _fast_review(self, source_code: str, converted_code: str,
                     allowed_write_fqns: set[str] | list[str] | None = None) -> dict:
        """Tier-2: deterministic pre-review — no LLM call.

        Cheap statement-count + silent-NULL heuristics. If this fast check
        passes, we skip the full ~12 s LLM semantic review (which often adds
        no value on chunks that are clearly well-formed).

        When `allowed_write_fqns` is non-empty, also enforces that every
        INSERT/UPDATE/DELETE/MERGE/CTAS/saveAsTable/insertInto/writeTo
        target in the converted code is one of the allow-listed FQNs
        (target-catalog routing). Violations are added to the feedback.

        Returns the same shape as `_semantic_review` so callers can chain:
            review = self._fast_review(...)
            if review["issues_found"]:
                review = self._semantic_review(...)  # fall through
        """
        import re as _re

        def _count(patterns: list[str], text: str) -> int:
            total = 0
            for p in patterns:
                total += len(_re.findall(p, text, _re.IGNORECASE))
            return total

        # Subtract status-tracking statements from source counts — the
        # @track_status decorator handles them, so the converted code
        # intentionally omits them.
        src_inserts_all = _count([r'\bInsert\s+(?:/\*.*?\*/\s*)?Into\b'], source_code)
        src_updates_all = _count([r'\bUpdate\s+[\w.]+'], source_code)
        status_inserts  = _count([r"\bInsert\s+Into\s+\w*\.?\w*Status\w*\b"],
                                  source_code)
        status_updates  = _count([r"\bUpdate\s+\w*\.?\w*Status\w*\s+Set\s+Status",
                                   r"\bSet\s+Status\s*=\s*'[YN]'"], source_code)
        src_inserts = max(0, src_inserts_all - status_inserts)
        src_updates = max(0, src_updates_all - status_updates)
        src_deletes = _count([r'\bDelete\s+From\b'], source_code)
        # Only count Execute Immediate TRUNCATE (avoid double-counting with
        # the bare `\bTruncate Table\b` which also matches the same lines).
        src_truncs  = _count([r"Execute\s+Immediate\s+['\"]truncate"],
                              source_code)

        # Converted code: count spark.sql(...) blocks containing key verbs
        # plus raw MERGE / INSERT statements.
        conv_inserts = _count([r'INSERT\s+INTO'], converted_code)
        conv_updates = _count([r'\bMERGE\s+INTO\b', r'\bUPDATE\s+[\w.]+\s+SET'], converted_code)
        conv_deletes = _count([r'\bDELETE\s+FROM\b'], converted_code)
        conv_truncs  = _count([r'TRUNCATE\s+TABLE'], converted_code)

        # Statement-fidelity issues
        issues = []

        def _ratio(src, conv):
            if src == 0:
                return 1.0
            return conv / src

        if _ratio(src_inserts, conv_inserts) < 0.8:
            issues.append(
                f"INSERT coverage low: {conv_inserts} of {src_inserts} "
                f"source INSERTs present in converted code.")
        if _ratio(src_updates, conv_updates) < 0.8:
            issues.append(
                f"UPDATE coverage low: {conv_updates} MERGE/UPDATE statements "
                f"vs {src_updates} source UPDATEs.")
        if _ratio(src_deletes, conv_deletes) < 0.8 and src_deletes > 0:
            issues.append(
                f"DELETE coverage low: {conv_deletes} of {src_deletes}.")
        if _ratio(src_truncs, conv_truncs) < 0.8 and src_truncs > 0:
            issues.append(
                f"TRUNCATE coverage low: {conv_truncs} of {src_truncs}.")

        # Silent-NULL smell: explicit `NULL AS col_name` in converted code
        # when source had a real expression. Rough heuristic.
        null_smell = _count([r"\bNone\s+AS\s+\w+", r"\bNULL\s+AS\s+\w+"], converted_code)
        if null_smell >= 3:
            issues.append(
                f"Silent-NULL smell: {null_smell} `NULL AS col` replacements "
                f"in converted code.")

        # Untranslated Oracle constructs still present
        remnants = _count([r"\bNVL\s*\(", r"\bDECODE\s*\(",
                            r"\bSYSDATE\b", r"\bROWNUM\b"], converted_code)
        if remnants > 0:
            issues.append(
                f"Untranslated Oracle remnants: {remnants} occurrences of "
                f"NVL/DECODE/SYSDATE/ROWNUM in converted code.")

        # Target-catalog write-FQN allow-list (only when caller provided a
        # non-empty list of allowed write targets from task.target_context).
        if allowed_write_fqns:
            try:
                from sql_migration.core.trino_sql_validator import validate_write_targets
                write_violations = validate_write_targets(
                    converted_code, allowed_write_fqns)
                if write_violations:
                    issues.extend(write_violations[:5])
            except Exception as exc:
                log.debug("fast_review_write_check_skipped", error=str(exc)[:100])

        if issues:
            return {
                "issues_found": True,
                "feedback": " | ".join(issues)[:800],
            }
        return {"issues_found": False, "feedback": ""}

    def _semantic_review(self, source_code: str, converted_code: str,
                         strategy: str = "Trino SQL") -> dict:
        """
        Compare source SQL and converted SQL side-by-side via a separate LLM call.
        Catches silent hallucinations where the LLM replaces complex expressions
        with NULL, drops columns, simplifies JOINs, or removes business logic.

        Returns:
            {"issues_found": bool, "feedback": str}
        """
        source_label = self._declared_dialect or self._source_engine or "source"
        # Use actual strategy so the review doesn't mislabel PySpark output
        # as Trino SQL (was causing false "target language mismatch" rejections).
        target_label = "PySpark Python" if "PYSPARK" in strategy.upper() else "Trino SQL"
        review_prompt = f"""You are a senior SQL migration reviewer. Your job is to perform an in-depth analysis comparing a {source_label} source query against its converted {target_label} equivalent.

Analyze EVERY expression, column, function call, JOIN condition, WHERE clause, GROUP BY, HAVING, ORDER BY, and subquery in the source — and verify it has a correct equivalent in the converted output.

Check specifically for:
1. **Silent NULL replacements**: Source has a real expression (e.g. SUBSTRING, CAST, REGEXP_EXTRACT, DECODE, NVL) but converted uses NULL AS column_name or a placeholder.
2. **Dropped columns**: Source SELECT has N columns, converted has fewer.
3. **Simplified expressions**: Source has complex CASE WHEN logic, converted has a simpler version that changes behavior.
4. **Missing WHERE/JOIN conditions**: Source has filtering or join conditions that are absent in converted.
5. **Function mismatches**: Source function converted to wrong equivalent (e.g. wrong argument order, wrong function name).
6. **Type changes**: Source casts to one type, converted casts to a different type that changes semantics.

IMPORTANT — do NOT flag any of the following as issues (they are expected migration behaviors):
- Removal of Oracle database links (@Dbloans, @dbloans, etc.). The target tables in Trino are equivalent.
- Replacement of SYSDATE/SYSTIMESTAMP with CURRENT_TIMESTAMP/CURRENT_DATE.
- Removal of Oracle hints (/*+ APPEND */, /*+ parallel(N) */).
- Conversion of NVL→COALESCE, DECODE→CASE, TO_NUMBER→CAST, TO_DATE→DATE.
- Wrapping SQL in spark.sql() calls (expected for PySpark target).
- COMMIT / ROLLBACK removal (Spark writes are atomic).
- DBMS_STATS.GATHER_TABLE_STATS converted to ANALYZE TABLE.
- Variable name case changes (e.g., i_Yyyymm → i_yyyymm). Python is case-sensitive but the converter intentionally lowercases Oracle PascalCase variables.
- Status tracking INSERT/UPDATE statements (Name, Dwld_Date, Status) handled by @track_status decorator instead of explicit SQL. This is by design.
- Exception handling blocks (WHEN OTHERS THEN Rollback; v_Err := SQLERRM; UPDATE status SET Status='N') replaced by @track_status decorator. The decorator catches exceptions, logs SQLERRM, and updates status automatically.
- v_Step_No decimal values truncated to integers (step tracking is ordinal).
- Missing EXCEPTION/WHEN OTHERS blocks in converted code — these are handled by the @track_status decorator wrapper, not explicit try/except.

--- SOURCE ({source_label}) ---
{source_code}
--- END SOURCE ---

--- CONVERTED ({target_label}) ---
{converted_code}
--- END CONVERTED ---

If you find ANY issues, respond with:
ISSUES FOUND
Then list each issue clearly:
- Issue 1: [source expression] was converted to [converted expression] — [what's wrong and what the correct conversion should be]
- Issue 2: ...

If the conversion is correct and preserves all business logic, respond with:
NO ISSUES

Be thorough. A silent NULL replacement is a critical bug that produces wrong business results."""

        try:
            result = self.llm.call(
                messages=[{"role": "user", "content": review_prompt}],
                expect_json=False,
                call_id="semantic_review",
            )
            review_text = result.strip() if isinstance(result, str) else str(result)

            if "ISSUES FOUND" in review_text.upper():
                # Extract everything after "ISSUES FOUND"
                idx = review_text.upper().index("ISSUES FOUND")
                feedback = review_text[idx + len("ISSUES FOUND"):].strip()
                log.warning("semantic_review_issues",
                            feedback=feedback[:300])
                return {"issues_found": True, "feedback": feedback}

            log.info("semantic_review_passed")
            return {"issues_found": False, "feedback": ""}

        except Exception as e:
            log.warning("semantic_review_failed", error=str(e)[:200])
            # On failure, don't block — proceed with the conversion as-is
            return {"issues_found": False, "feedback": ""}

    def _load_adapter_mappings(self) -> tuple[list[str], dict]:
        """Load dialect_functions and trino_mappings from persisted adapter."""
        try:
            profile_data = self.store.read("analysis", "dialect_profile.json")
            adapter_path = Path(self.global_cfg.paths.adapters_dir) / \
                           Path(profile_data.get("adapter_path", "generic.json")).name
            if not adapter_path.exists():
                adapter_path = Path(self.global_cfg.paths.adapters_dir) / "generic.json"
            import json
            adapter = json.loads(adapter_path.read_text(encoding="utf-8"))
            return (
                adapter.get("dialect_functions", []),
                {k: v for k, v in adapter.get("trino_mappings", {}).items() if v},
            )
        except Exception:
            return [], {}

    def reset_proc_state(self, proc_name: str) -> None:
        """
        Clear accumulated state for a proc (called on replan).
        Resets the error tracker so the new replan starts with a clean
        occurrence count — a replan may use a different strategy and the
        old error signatures should not carry over.
        """
        # self._histories.pop(proc_name, None)  # v10: _histories unused, see __init__
        self._chunk_codes.pop(proc_name, None)
        self._chunk_summaries.pop(proc_name, None)
        if proc_name in self._error_trackers:
            self._error_trackers[proc_name].reset()
        log.debug("proc_state_reset",
                  proc=proc_name,
                  cleared=["chunk_codes", "chunk_summaries", "error_tracker"])
