"""
global_error_aggregator.py
==========================
Run-level error aggregation with circuit breaker.

Ref: DeepMind "Towards a Science of Scaling Agent Systems" (Dec 2025)
  — unstructured multi-agent networks amplify errors 17.2×.
  — circuit breakers that halt workflows on budget/error thresholds
    are the primary mitigation for cascade amplification.

Ref: "From Spark to Fire" (Xie et al., March 2026, arXiv 2603.04474)
  — cascade amplification: a single bad shared artifact causes the
    same error signature across all downstream consumers.

Problem solved:
  In v5, each proc has its own per-proc ErrorTracker. If the adapter
  maps NVL→COALESCE incorrectly, proc_1 burns 9 LLM calls to freeze,
  then proc_2 independently does the same, then proc_3, etc.
  For 23 procs: 207 wasted LLM calls, all from one bad adapter entry.

  The GlobalErrorAggregator watches ACROSS procs. When the same
  normalised error signature appears in 3+ different procs, it:
    1. Triggers a CIRCUIT_BREAKER_OPEN state
    2. Pauses all pending dispatches
    3. Surfaces the systematic root cause with affected procs
    4. Optionally traces the error back to a shared artifact (adapter,
       callee signature, schema) via ProvenanceTracker

Design decisions:
  - Runs IN the Orchestrator's dispatch loop (not a separate process).
  - Records errors as they arrive from _handle_chunk_result() and
    _run_validation_for_proc().
  - Circuit breaker check happens BEFORE each proc dispatch in the loop.
  - When open, the Orchestrator skips all PENDING procs and logs
    a SYSTEMATIC_ERROR_DETECTED event.
  - Does NOT auto-fix: it surfaces the issue for the developer.
    The developer can: fix the adapter, re-run, or mark procs as skip.
  - Resets when the developer acknowledges via a resolution file.

Integration:
  - Orchestrator.__init__: create GlobalErrorAggregator
  - Orchestrator._handle_chunk_result: feed errors via record_proc_error()
  - Orchestrator._run_validation_for_proc: feed validation failures
  - Orchestrator dispatch loop: check is_circuit_open() before dispatch
  - main.py: pass aggregator to Orchestrator
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from sql_migration.core.error_handling import ErrorClassifier
from sql_migration.core.logger import get_logger

if TYPE_CHECKING:
    from sql_migration.core.artifact_store import ArtifactStore
    from sql_migration.core.provenance import ProvenanceTracker

log = get_logger("global_error_aggregator")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Number of distinct procs that must exhibit the same error signature
# before the circuit breaker trips.
DEFAULT_CROSS_PROC_THRESHOLD = 3

# Maximum number of procs that can fail before forcing a pipeline-wide pause,
# regardless of whether a single signature dominates.
DEFAULT_MAX_FAILED_PROCS = 5

# Known error patterns that implicate specific shared artifacts.
# If a signature matches one of these, the aggregator can trace it
# back to the artifact for provenance annotation.
ADAPTER_ERROR_MARKERS = (
    "remnant:",
    "not translated",
    "unmapped",
    "dialect function",
)
SCHEMA_ERROR_MARKERS = (
    "column",
    "does not exist",
    "schema",
    "missing_in_trino",
    "table not found",
)
CALLEE_ERROR_MARKERS = (
    "callee",
    "undefined function",
    "is not defined",
    "no attribute",
    "takes",
    "argument",
    "positional",
)
UNRESOLVED_DEP_MARKERS = (
    "unresolved_dep",
    "not found in any source",
    "udf definition unknown",
    "was not found in",
)


# ---------------------------------------------------------------------------
# Systematic error record
# ---------------------------------------------------------------------------

@dataclass
class SystematicError:
    """
    A cross-proc error pattern detected by the aggregator.
    Surfaces to the developer via the UI and CLI.
    """
    signature: str                    # Normalised error signature
    affected_procs: list[str]         # Proc names that exhibited this error
    occurrence_count: int             # Total occurrences across all procs
    first_seen_proc: str = ""         # First proc to exhibit this error
    suspected_source: str = ""        # "adapter" | "schema" | "callee" | "unknown"
    suspected_artifact_key: str = ""  # e.g. "NVL" or "ACCOUNT_MASTER" or "sp_calc"
    recommendation: str = ""          # Actionable fix suggestion
    detected_at: str = ""             # ISO timestamp

    def to_dict(self) -> dict:
        return {
            "signature": self.signature,
            "affected_procs": self.affected_procs,
            "occurrence_count": self.occurrence_count,
            "first_seen_proc": self.first_seen_proc,
            "suspected_source": self.suspected_source,
            "suspected_artifact_key": self.suspected_artifact_key,
            "recommendation": self.recommendation,
            "detected_at": self.detected_at,
        }


# ---------------------------------------------------------------------------
# Circuit breaker states
# ---------------------------------------------------------------------------

class CircuitState:
    CLOSED = "CLOSED"        # Normal operation — all dispatches proceed
    OPEN   = "OPEN"          # Systematic error detected — dispatches paused
    ACKNOWLEDGED = "ACKNOWLEDGED"  # Developer saw the error, pipeline resumes


# ---------------------------------------------------------------------------
# Global Error Aggregator
# ---------------------------------------------------------------------------

class GlobalErrorAggregator:
    """
    Run-level error aggregator that detects systematic cross-proc patterns
    and triggers a circuit breaker to prevent cascading waste.

    Usage in Orchestrator:
        aggregator = GlobalErrorAggregator(store, provenance_tracker)

        # In _handle_chunk_result / _run_validation_for_proc:
        aggregator.record_proc_error(proc_name, error_msg, "CHUNK_FAIL")

        # In dispatch loop, before dispatching a PENDING proc:
        if aggregator.is_circuit_open():
            log.warning("circuit_breaker_open", ...)
            continue  # Skip this proc
    """

    def __init__(
        self,
        store: ArtifactStore,
        provenance_tracker: ProvenanceTracker | None = None,
        cross_proc_threshold: int = DEFAULT_CROSS_PROC_THRESHOLD,
        max_failed_procs: int = DEFAULT_MAX_FAILED_PROCS,
    ) -> None:
        self.store = store
        self.provenance = provenance_tracker
        self.cross_proc_threshold = cross_proc_threshold
        self.max_failed_procs = max_failed_procs

        # State
        self._circuit_state: str = CircuitState.CLOSED
        self._systematic_errors: list[SystematicError] = []

        # Tracking: signature → set of proc names
        self._sig_to_procs: dict[str, set[str]] = defaultdict(set)
        # Tracking: signature → total occurrence count
        self._sig_counts: Counter[str] = Counter()
        # Tracking: signature → first proc that saw it
        self._sig_first_proc: dict[str, str] = {}
        # Tracking: set of procs that have reached a failure state
        self._failed_procs: set[str] = set()

    # =========================================================================
    # Public API
    # =========================================================================

    def record_proc_error(
        self,
        proc_name: str,
        error_msg: str,
        error_type: str = "CHUNK_FAIL",
    ) -> SystematicError | None:
        """
        Record an error from a proc and check for cross-proc patterns.

        Called by Orchestrator in _handle_chunk_result and
        _run_validation_for_proc.

        Returns:
            SystematicError if a new systematic pattern was detected
            (circuit breaker just tripped). None otherwise.
        """
        sig = ErrorClassifier.get_signature(error_msg)
        if not sig:
            return None

        # Update tracking
        self._sig_to_procs[sig].add(proc_name)
        self._sig_counts[sig] += 1
        if sig not in self._sig_first_proc:
            self._sig_first_proc[sig] = proc_name

        # Track failed procs
        if error_type in {"CHUNK_FAIL", "VALIDATION_FAIL", "FROZEN"}:
            self._failed_procs.add(proc_name)

        # Check if this signature now crosses the threshold
        affected = self._sig_to_procs[sig]
        if len(affected) >= self.cross_proc_threshold:
            # Check if we already reported this signature
            if not any(se.signature == sig for se in self._systematic_errors):
                return self._trip_circuit_breaker(sig, error_msg)

        # Check if too many procs have failed overall
        if len(self._failed_procs) >= self.max_failed_procs:
            dominant = self._get_dominant_signature()
            if dominant and not any(se.signature == dominant for se in self._systematic_errors):
                return self._trip_circuit_breaker(dominant, "")

        return None

    def record_proc_terminal(self, proc_name: str, status: str) -> None:
        """
        Record when a proc reaches a terminal failure state (FROZEN).
        Used for the max_failed_procs threshold check.
        """
        if status == "FROZEN":
            self._failed_procs.add(proc_name)

    def is_circuit_open(self) -> bool:
        """
        Check if the circuit breaker is currently open (pausing dispatches).

        Call this in the Orchestrator dispatch loop before dispatching
        any PENDING proc. If True, skip the dispatch and log.
        """
        return self._circuit_state == CircuitState.OPEN

    def get_systematic_errors(self) -> list[SystematicError]:
        """Get all detected systematic errors for UI/CLI display."""
        return list(self._systematic_errors)

    def acknowledge(self, reason: str = "") -> None:
        """
        Developer acknowledges the systematic error.
        Closes the circuit breaker and allows dispatches to resume.

        Called from Streamlit UI or CLI when the developer has reviewed
        the error and either fixed the root cause or chosen to continue.
        """
        if self._circuit_state == CircuitState.OPEN:
            self._circuit_state = CircuitState.ACKNOWLEDGED
            log.info("circuit_breaker_acknowledged",
                     errors=len(self._systematic_errors),
                     reason=reason)
            # Persist the acknowledgement so the summary reflects the new state
            self._persist_systematic_errors()

    def write_ack_signal(self, reason: str = "", reset_procs: bool = True) -> None:
        """
        Write a file-based acknowledgement signal for cross-process communication.

        The Orchestrator runs as a long-lived process. The CLI `resume` command
        and Streamlit UI run in separate processes. This file is the cross-process
        signal that the developer has fixed the root cause and wants to resume.

        The Orchestrator calls check_and_apply_ack() on each dispatch cycle
        to detect this file and transition the circuit breaker.

        Args:
            reason: Why the developer is acknowledging (logged for audit).
            reset_procs: If True, the Orchestrator should reset FROZEN procs
                         affected by the systematic error back to PENDING.
        """
        from datetime import datetime, timezone
        ack_data = {
            "acknowledged_at": datetime.now(timezone.utc).isoformat(),
            "reason": reason,
            "reset_procs": reset_procs,
            "systematic_errors_at_ack": len(self._systematic_errors),
        }
        try:
            self.store.write("orchestrator", "circuit_ack.json", ack_data)
            log.info("circuit_ack_signal_written", reason=reason)
        except Exception as e:
            log.error("circuit_ack_signal_write_failed", error=str(e)[:100])

    def check_and_apply_ack(self) -> bool:
        """
        Check for a file-based acknowledgement signal and apply it.

        Called by the Orchestrator in the dispatch loop. If circuit_ack.json
        exists, acknowledge the circuit breaker, remove the file, and return True.
        Returns False if no ack signal found or circuit is not open.
        """
        if self._circuit_state != CircuitState.OPEN:
            return False

        if not self.store.exists("orchestrator", "circuit_ack.json"):
            return False

        try:
            ack_data = self.store.read("orchestrator", "circuit_ack.json")
            reason = ack_data.get("reason", "file-based ack")
            self.acknowledge(reason)

            # Remove the ack file so it doesn't re-trigger
            ack_path = self.store.path("orchestrator", "circuit_ack.json")
            if ack_path.exists():
                ack_path.unlink()

            log.info("circuit_ack_applied_from_file",
                     reason=reason,
                     reset_procs=ack_data.get("reset_procs", False))
            return True
        except Exception as e:
            log.error("circuit_ack_apply_failed", error=str(e)[:100])
            return False

    def reset(self) -> None:
        """
        Full reset — used when the pipeline restarts with a fix.
        Clears all tracking state and closes the circuit breaker.
        """
        self._circuit_state = CircuitState.CLOSED
        self._systematic_errors.clear()
        self._sig_to_procs.clear()
        self._sig_counts.clear()
        self._sig_first_proc.clear()
        self._failed_procs.clear()

    def get_summary(self) -> dict:
        """Serialisable summary for artifact persistence and UI display."""
        return {
            "circuit_state": self._circuit_state,
            "total_unique_signatures": len(self._sig_counts),
            "total_failed_procs": len(self._failed_procs),
            "failed_proc_names": sorted(self._failed_procs),
            "systematic_errors": [se.to_dict() for se in self._systematic_errors],
            "top_signatures": [
                {
                    "signature": sig,
                    "count": count,
                    "affected_procs": sorted(self._sig_to_procs.get(sig, set())),
                }
                for sig, count in self._sig_counts.most_common(5)
            ],
        }

    # =========================================================================
    # Internal: Circuit breaker logic
    # =========================================================================

    def _trip_circuit_breaker(
        self,
        signature: str,
        raw_error: str,
    ) -> SystematicError:
        """
        Trip the circuit breaker for a detected systematic error.

        Determines the suspected source (adapter, schema, callee),
        generates a recommendation, and persists the finding.
        """
        affected_procs = sorted(self._sig_to_procs.get(signature, set()))
        count = self._sig_counts[signature]
        first_proc = self._sig_first_proc.get(signature, "")

        # Classify the suspected source
        suspected_source, artifact_key, recommendation = self._classify_error_source(
            signature, raw_error
        )

        systematic = SystematicError(
            signature=signature,
            affected_procs=affected_procs,
            occurrence_count=count,
            first_seen_proc=first_proc,
            suspected_source=suspected_source,
            suspected_artifact_key=artifact_key,
            recommendation=recommendation,
            detected_at=datetime.now(timezone.utc).isoformat(),
        )

        self._systematic_errors.append(systematic)
        self._circuit_state = CircuitState.OPEN

        log.warning(
            "circuit_breaker_tripped",
            signature=signature[:100],
            affected_procs=affected_procs,
            affected_count=len(affected_procs),
            total_occurrences=count,
            suspected_source=suspected_source,
            suspected_artifact=artifact_key,
            recommendation=recommendation[:200],
        )

        # Update provenance if tracker available
        if self.provenance and suspected_source and artifact_key:
            for proc in affected_procs:
                self.provenance.increment_downstream_errors(
                    artifact_type=self._source_to_artifact_type(suspected_source),
                    artifact_key=artifact_key,
                    failing_proc=proc,
                )

        # Persist the finding
        self._persist_systematic_errors()

        return systematic

    def _classify_error_source(
        self,
        signature: str,
        raw_error: str,
    ) -> tuple[str, str, str]:
        """
        Determine the most likely shared artifact that caused the error.

        Returns:
            (suspected_source, artifact_key, recommendation)
        """
        sig_lower = signature.lower()
        raw_lower = (raw_error or "").lower()
        combined = sig_lower + " " + raw_lower

        # 1. Adapter mapping errors (REMNANT: NVL not translated)
        if any(marker in combined for marker in ADAPTER_ERROR_MARKERS):
            # Try to extract the function name from the signature
            fn_name = self._extract_function_name(combined)
            return (
                "adapter",
                fn_name or "unknown_function",
                f"Systematic adapter mapping failure detected across {len(self._sig_to_procs.get(signature, set()))} procs. "
                f"{'Function: ' + fn_name + '. ' if fn_name else ''}"
                f"Action: Review adapters/{{dialect}}.json — the mapping for this function "
                f"may be incorrect or missing. Fix the adapter and re-run affected procs."
            )

        # 2. Schema/column errors
        if any(marker in combined for marker in SCHEMA_ERROR_MARKERS):
            col_or_table = self._extract_identifier(combined)
            return (
                "schema",
                col_or_table or "unknown_table_or_column",
                f"Systematic schema mismatch across {len(self._sig_to_procs.get(signature, set()))} procs. "
                f"{'Identifier: ' + col_or_table + '. ' if col_or_table else ''}"
                f"Action: Check table_registry and table_mapping in README. "
                f"The source→Trino table mapping may be incorrect or columns may have been renamed."
            )

        # 3. Unresolved external dependency (missing UDF — check BEFORE callee)
        if any(marker in combined for marker in UNRESOLVED_DEP_MARKERS):
            fn_name = self._extract_function_name(combined)
            affected_count = len(self._sig_to_procs.get(signature, set()))
            return (
                "missing_udf",
                fn_name or "unknown_udf",
                f"Missing UDF '{fn_name or '?'}' causing failures across {affected_count} procs. "
                f"Action: Upload the missing UDF source file and re-run, or resolve as "
                f"TODO via the Dependency Gate. In the Streamlit UI: navigate to "
                f"'Dependency Resolution' and upload the file. In CLI: re-run with "
                f"the UDF file appended to --sql."
            )

        # 4. Callee signature errors
        if any(marker in combined for marker in CALLEE_ERROR_MARKERS):
            callee_name = self._extract_function_name(combined)
            return (
                "callee",
                callee_name or "unknown_callee",
                f"Systematic callee signature mismatch across {len(self._sig_to_procs.get(signature, set()))} procs. "
                f"{'Callee: ' + callee_name + '. ' if callee_name else ''}"
                f"Action: The shared utility proc's converted signature may be wrong. "
                f"Review the callee's converted code and compare its parameter list "
                f"against the original source proc declaration."
            )

        # 4. Unknown pattern — still systematic
        return (
            "unknown",
            "",
            f"Systematic error pattern detected across {len(self._sig_to_procs.get(signature, set()))} procs "
            f"but the root cause could not be auto-classified. "
            f"Signature: '{signature[:80]}'. "
            f"Action: Review the error details for common factor — "
            f"likely a shared artifact (adapter, schema, or callee) is causing this."
        )

    # =========================================================================
    # Internal: helpers
    # =========================================================================

    def _get_dominant_signature(self) -> str | None:
        """Get the most frequent error signature across all failed procs."""
        if not self._sig_counts:
            return None
        sig, count = self._sig_counts.most_common(1)[0]
        if count >= 2:  # At least 2 occurrences to be "dominant"
            return sig
        return None

    @staticmethod
    def _extract_function_name(text: str) -> str:
        """Try to extract a SQL/Python function name from error text."""
        import re
        # Patterns: "NVL not translated", "REMNANT: DECODE", "function SYSDATE"
        patterns = [
            r"remnant[:\s]+(\w+)",
            r"(\w+)\s+not\s+translated",
            r"function\s+['\"]?(\w+)",
            r"unmapped.*?original[:\s]+(\w+)",
            r"'(\w+)'\s+is\s+not\s+defined",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                name = m.group(1)
                # Filter out common noise words
                if name.upper() not in {"THE", "A", "AN", "IS", "NOT", "AND", "OR",
                                         "IN", "AT", "TO", "LINE", "COL", "FILE"}:
                    return name
        return ""

    @staticmethod
    def _extract_identifier(text: str) -> str:
        """Try to extract a column or table name from error text."""
        import re
        patterns = [
            r"column\s+['\"]?(\w+)",
            r"table\s+['\"]?(\w+)",
            r"['\"](\w+)['\"]\s+does\s+not\s+exist",
            r"missing.*?columns?[:\s]+\[?['\"]?(\w+)",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                name = m.group(1)
                if name.upper() not in {"THE", "A", "AN", "IS", "NOT"}:
                    return name
        return ""

    @staticmethod
    def _source_to_artifact_type(source: str) -> str:
        """Map suspected source to provenance artifact_type."""
        return {
            "adapter": "adapter_mapping",
            "schema": "table_entry",
            "callee": "converted_code",
            "missing_udf": "external_dependency",
        }.get(source, "unknown")

    def _persist_systematic_errors(self) -> None:
        """Write systematic error findings to the artifact store."""
        try:
            self.store.write(
                "orchestrator",
                "systematic_errors.json",
                self.get_summary(),
            )
        except Exception:
            # Persistence is advisory — never block the pipeline
            pass
