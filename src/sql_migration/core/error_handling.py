"""
error_handling.py
=================
Centralised error classification, signature normalisation, and escalating
LLM guidance for use across all agents and the LLM client.

Three consumers:
  1. LLMClient._call_with_retry()     → skip retries on UNSOLVABLE errors
  2. ConversionAgent._c3_c4_loop()    → inject escalating guidance into C4 prompt
  3. OrchestratorAgent._handle_chunk  → detect repeated root causes across replans

Design decisions (ported from reference Databricks LLMClient, adapted for this system):

  UNSOLVABLE errors    — environmental issues (auth, network, quota).
                         Stop immediately, don't retry, surface to developer.
  TRANSIENT errors     — infrastructure hiccups (timeout, 5xx).
                         Retry with backoff, up to the configured limit.
  REPEATED errors      — same root error seen >= threshold across attempts.
                         Stop retrying, emit STOP_RETRYING guidance to LLM.
  CORRECTION_REFUSED   — LLM keeps producing code with the same remnant/syntax
                         error even after being shown the fix. Stop correcting,
                         escalate to Orchestrator as CONVERSION_FAILED.

Error signature normalisation removes noise that changes between runs:
  - Line numbers       (line 42 → line N)
  - Memory addresses   (0x7f3a... → 0xADDR)
  - File paths         (/home/user/xyz.py → /PATH/file.py)
  - Library versions   (1.4.2 → X.X.X)
  - Proc/chunk names   (sp_calc_interest → PROC_NAME)

This keeps the Counter keyed on the *root cause* of the error, not the
surface message, so "line 14: NVL not translated" and "line 27: NVL not
translated" both count as the same REMNANT error.
"""

from __future__ import annotations

import re
from collections import Counter
from enum import Enum
from typing import NamedTuple


# ---------------------------------------------------------------------------
# Error classification enum
# ---------------------------------------------------------------------------

class ErrorClass(Enum):
    UNSOLVABLE  = "unsolvable"   # Environmental — stop immediately
    TRANSIENT   = "transient"    # Infrastructure — retry with backoff
    RETRYABLE   = "retryable"    # Generic failure — one retry is reasonable
    REPEATED    = "repeated"     # Same signature seen >= threshold — stop retrying


# ---------------------------------------------------------------------------
# Pattern tables
# ---------------------------------------------------------------------------

# Errors that indicate a problem outside the agent's control.
# Retrying will never help. Surface directly to the developer.
UNSOLVABLE_PATTERNS: tuple[str, ...] = (
    # Authentication / authorisation
    "api_key", "api key", "apikey",
    "authentication", "unauthorized", "unauthenticated",
    "401", "403",
    "forbidden", "permission denied", "access denied",
    # Quota / rate (hard)
    "quota exceeded", "billing", "insufficient_quota",
    # Network infrastructure
    "connection refused", "connection reset", "connection aborted",
    "network unreachable", "network error",
    "name or service not known", "name resolution",
    "no route to host", "host unreachable",
    "eof occurred in violation of protocol",
    # SSL / TLS
    "ssl", "certificate verify failed", "certificate expired",
    # Missing resource (environmental)
    "podman: command not found", "docker: command not found",
    "file not found: /scripts",           # Sandbox image missing scripts mount
    "no space left on device",            # Disk full
    "killed",                             # OOM kill (structural, won't self-heal)
)

# Errors that are *likely* transient — worth retrying after a short wait.
TRANSIENT_PATTERNS: tuple[str, ...] = (
    "timeout", "timed out", "read timeout",
    "service unavailable", "503",
    "internal server error", "500",
    "bad gateway", "502",
    "gateway timeout", "504",
    "temporary failure",
    "try again",
    "connection pool",
    "resource temporarily unavailable",
    "engine overloaded",
    "model overloaded",
    "server busy",
)

# Repeated-error threshold: if the same error signature is seen this many
# times, switch from "fix it" guidance to "stop and escalate" guidance.
REPEATED_THRESHOLD = 3


# ---------------------------------------------------------------------------
# ErrorClassifier
# ---------------------------------------------------------------------------

class ErrorClassifier:
    """
    Classify an error message and build escalating LLM guidance.
    All methods are class-level so no instantiation is needed.
    """

    # ── Public API ────────────────────────────────────────────────────────────

    @classmethod
    def classify(cls, error_msg: str) -> ErrorClass:
        """
        Classify an error message into one of four categories.

        Args:
            error_msg: The raw error string (exception message, stderr, etc.)

        Returns:
            ErrorClass enum value

        Usage in _call_with_retry():
            if ErrorClassifier.classify(str(e)) == ErrorClass.UNSOLVABLE:
                raise  # Don't retry
        """
        lower = error_msg.lower()
        if any(pat in lower for pat in UNSOLVABLE_PATTERNS):
            return ErrorClass.UNSOLVABLE
        if any(pat in lower for pat in TRANSIENT_PATTERNS):
            return ErrorClass.TRANSIENT
        return ErrorClass.RETRYABLE

    @classmethod
    def is_unsolvable(cls, error_msg: str) -> bool:
        return cls.classify(error_msg) == ErrorClass.UNSOLVABLE

    @classmethod
    def is_transient(cls, error_msg: str) -> bool:
        return cls.classify(error_msg) == ErrorClass.TRANSIENT

    @classmethod
    def get_signature(cls, error_msg: str) -> str:
        """
        Normalise an error message into a stable signature for deduplication.

        Strips noise that changes between runs (line numbers, paths, versions)
        while preserving the root cause descriptor.

        Usage:
            sig = ErrorClassifier.get_signature(error_msg)
            counter[sig] += 1   # Same root cause regardless of line number
        """
        sig = error_msg.lower()

        # Numbers: line N, column N
        sig = re.sub(r'\bline\s+\d+', 'line N', sig)
        sig = re.sub(r'\bcol(?:umn)?\s+\d+', 'col N', sig)

        # Memory addresses
        sig = re.sub(r'0x[0-9a-f]+', '0xADDR', sig)

        # File paths → keep only the filename
        sig = re.sub(r'[/\\][^\s,:\'"]+', '/PATH/file', sig)

        # Version strings (e.g. 1.4.2, pyspark 3.5.0)
        sig = re.sub(r'\d+\.\d+\.\d+', 'X.X.X', sig)

        # Proc/chunk identifiers — strip _c0, _c1 chunk suffixes
        sig = re.sub(r'\b[a-z_]+_c\d+\b', 'PROC_CHUNK', sig)

        # Timestamps
        sig = re.sub(r'\d{4}-\d{2}-\d{2}[t ]\d{2}:\d{2}:\d{2}', 'TIMESTAMP', sig)

        # Trim whitespace and cap length (signatures only need to be
        # distinctive, not full reproductions of the error)
        return sig.strip()[:200]

    @classmethod
    def build_llm_guidance(
        cls,
        error_msg:   str,
        occurrence:  int,
        attempt:     int,
        max_attempts: int,
    ) -> str:
        """
        Build escalating guidance text to inject into LLM self-correction prompts.

        The guidance changes based on how many times the same root error has
        been seen and how close the agent is to its iteration limit.

        Args:
            error_msg:    Raw error string from sandbox/validation
            occurrence:   How many times this error *signature* has been seen
            attempt:      Current correction attempt number (1-based)
            max_attempts: Maximum corrections allowed (from config)

        Returns:
            Guidance string to prepend to the self-correction prompt.

        Severity ladder:
          1st occurrence, not near limit  → standard "fix this" message
          2nd occurrence                  → warning: "if this fails again, stop"
          3rd+ occurrence                 → STOP_RETRYING: ask for help
          unsolvable                      → ENVIRONMENTAL_ISSUE: explain to user
          near iteration limit            → WRAP_UP: don't start new approaches
        """
        is_unsolvable = cls.is_unsolvable(error_msg)

        # ── UNSOLVABLE: don't even try ────────────────────────────────────────
        if is_unsolvable:
            return (
                "⚠️ ENVIRONMENTAL ERROR — DO NOT ATTEMPT TO FIX IN CODE\n\n"
                "This error indicates a problem outside the code itself "
                "(authentication, network, missing system dependency):\n\n"
                f"  {error_msg}\n\n"
                "ACTION: Do NOT attempt further code changes. "
                "Explain clearly what external action is required "
                "(e.g. install a system package, set an environment variable, "
                "check network connectivity)."
            )

        # ── REPEATED 3+ times: stop retrying ─────────────────────────────────
        if occurrence >= REPEATED_THRESHOLD:
            return (
                f"🚨 REPEATED ERROR (seen {occurrence}×) — STOP RETRYING\n\n"
                f"Error: {error_msg}\n\n"
                "You have attempted to fix this error multiple times without success. "
                "STOP making further correction attempts.\n"
                "Instead:\n"
                "  1. Do NOT produce another version of the code.\n"
                "  2. Explain precisely what is causing the persistent failure.\n"
                "  3. List what alternatives or workarounds exist.\n"
                "  4. If a manual developer decision is needed, say so explicitly."
            )

        # ── Near iteration limit: wrap up ─────────────────────────────────────
        if attempt >= max_attempts:
            return (
                f"⚠️ LAST CORRECTION ATTEMPT ({attempt}/{max_attempts})\n\n"
                f"Error: {error_msg}\n\n"
                "This is your final attempt. "
                "If you cannot fix this with a targeted, minimal change, "
                "do NOT try a completely different approach — "
                "output the current code with a TODO comment explaining the issue."
            )

        # ── Second occurrence: warning ────────────────────────────────────────
        if occurrence == 2:
            return (
                f"⚠️ SECOND OCCURRENCE of this error type (attempt {attempt}/{max_attempts})\n\n"
                f"Error: {error_msg}\n\n"
                "You have seen this error before and your previous fix did not resolve it. "
                "If your next fix attempt fails again, STOP and escalate rather than retry. "
                "Make a very targeted, specific change this time."
            )

        # ── First occurrence: standard ────────────────────────────────────────
        return f"Error to fix (attempt {attempt}/{max_attempts}):\n{error_msg}"


# ---------------------------------------------------------------------------
# ErrorTracker
# ---------------------------------------------------------------------------

class ErrorRecord(NamedTuple):
    """Single error occurrence record."""
    raw_msg:   str
    signature: str
    error_class: ErrorClass


class ErrorTracker:
    """
    Track error occurrences within a single agent scope (one proc, one
    agentic loop, etc.).

    Usage — in ConversionAgent._c3_c4_validate_and_correct():
        tracker = ErrorTracker()
        for attempt in range(max_corrections + 1):
            errors = self._c3_validate(task, code)
            if not errors:
                break
            combined = "; ".join(errors)
            occurrence = tracker.record(combined)
            guidance = tracker.build_guidance(combined, attempt, max_corrections)
            corrected = self._c4_self_correct(..., guidance=guidance)

    Usage — in OrchestratorAgent, to detect repeated root causes:
        if tracker.dominant_signature and tracker.dominant_count >= 3:
            log.warning("repeated_root_cause", sig=tracker.dominant_signature)
    """

    def __init__(self) -> None:
        self._counts:  Counter[str]    = Counter()
        self._records: list[ErrorRecord] = []

    def record(self, error_msg: str) -> int:
        """
        Record an error occurrence.

        Returns:
            How many times this error *signature* has been seen (including now).
        """
        sig   = ErrorClassifier.get_signature(error_msg)
        eclass = ErrorClassifier.classify(error_msg)
        self._counts[sig] += 1
        self._records.append(ErrorRecord(error_msg, sig, eclass))
        return self._counts[sig]

    def occurrence_count(self, error_msg: str) -> int:
        """How many times has this error signature been seen?"""
        sig = ErrorClassifier.get_signature(error_msg)
        return self._counts[sig]

    def build_guidance(
        self,
        error_msg:   str,
        attempt:     int,
        max_attempts: int,
    ) -> str:
        """
        Record the error and return escalating LLM guidance.
        Combines record() + ErrorClassifier.build_llm_guidance() in one call.
        """
        occurrence = self.record(error_msg)
        return ErrorClassifier.build_llm_guidance(
            error_msg    = error_msg,
            occurrence   = occurrence,
            attempt      = attempt,
            max_attempts = max_attempts,
        )

    @property
    def dominant_signature(self) -> str | None:
        """The most frequently seen error signature (None if no errors recorded)."""
        if not self._counts:
            return None
        return self._counts.most_common(1)[0][0]

    @property
    def dominant_count(self) -> int:
        """How many times the dominant error signature has been seen."""
        if not self._counts:
            return 0
        return self._counts.most_common(1)[0][1]

    @property
    def has_unsolvable(self) -> bool:
        """True if any recorded error is UNSOLVABLE."""
        return any(r.error_class == ErrorClass.UNSOLVABLE for r in self._records)

    @property
    def total_errors(self) -> int:
        return len(self._records)

    def reset(self) -> None:
        """Clear all recorded errors (call when a proc is replanned)."""
        self._counts.clear()
        self._records.clear()

    def summary(self) -> dict:
        """Serialisable summary for artifact logging."""
        return {
            "total_errors":        self.total_errors,
            "dominant_signature":  self.dominant_signature,
            "dominant_count":      self.dominant_count,
            "has_unsolvable":      self.has_unsolvable,
            "signature_counts":    dict(self._counts.most_common(5)),
        }
