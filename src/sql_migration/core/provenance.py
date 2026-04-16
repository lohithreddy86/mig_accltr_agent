"""
provenance.py
=============
Genealogy-based provenance tracking for shared artifacts.

Ref: "From Spark to Fire" (Xie et al., March 2026, arXiv 2603.04474)
  — genealogy-graph-based governance suppresses error amplification
    by tracking the lineage of every claim in shared context.

Problem solved:
  When the Orchestrator loads a callee's converted function signature
  (O2) and injects it into a caller's C2 prompt, the caller has no
  visibility into HOW the callee was validated. If the callee only
  reached PARTIAL with TODO markers, or was converted with multiple
  self-correction attempts, the signature is LOW_CONFIDENCE — but the
  caller's C2 prompt treats it as ground truth.

  Similarly, if the adapter's construct_map has a mapping that causes
  failures across multiple procs, there is no feedback loop to trace
  the downstream failures back to the shared artifact that caused them.

Design:
  1. Every shared artifact (converted_code, adapter mapping, table_registry
     entry) gets a Provenance record:
       - producing_agent: who created it
       - validation_status: what validation it passed
       - confidence: 0.0–1.0 based on how it was validated
       - lineage_tags: list of signals (e.g., "self_corrected_2x", "partial")
       - error_count: how many downstream errors reference this artifact

  2. Before O2 injects callee signatures, it reads the callee's provenance
     and tags LOW_CONFIDENCE signatures with a warning in the C2 prompt.

  3. The GlobalErrorAggregator (separate module) can increment error_count
     on shared artifacts when downstream procs fail with matching signatures.

Integration:
  - ConversionAgent._c5_assemble → writes provenance after assembly
  - OrchestratorAgent._process_next_chunk → reads provenance for callee sigs
  - No changes to agent interfaces or artifact store API
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


# ---------------------------------------------------------------------------
# Provenance record for a shared artifact
# ---------------------------------------------------------------------------

@dataclass
class ArtifactProvenance:
    """
    Provenance metadata for a shared artifact.

    Attached to: converted_code, adapter mappings, table_registry entries.
    Read by: Orchestrator (callee sig injection), GlobalErrorAggregator.
    """

    # Identity
    artifact_type: str = ""       # "converted_code" | "adapter_mapping" | "table_entry"
    artifact_key: str = ""        # e.g. proc_name or "oracle.NVL" or "ACCOUNT_MASTER"
    run_id: str = ""

    # Producer
    producing_agent: str = ""     # "conversion" | "analysis"
    produced_at: str = ""         # ISO timestamp

    # Validation status
    validation_status: str = ""   # "VALIDATED" | "PARTIAL" | "UNVALIDATED" | "FAILED"
    validation_level: int = 0     # 0=none, 1=static, 2=schema+exec, 3=full

    # Confidence score (0.0–1.0) — computed from multiple signals
    confidence: float = 1.0

    # Lineage tags — machine-readable signals about how this artifact was produced
    # Examples: ["self_corrected_2x", "fallback_adapter", "partial_with_todos",
    #            "unresolved_deps_present", "write_conflict_table"]
    lineage_tags: list[str] = field(default_factory=list)

    # Downstream error tracking (updated by GlobalErrorAggregator)
    downstream_error_count: int = 0
    downstream_error_procs: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "artifact_type": self.artifact_type,
            "artifact_key": self.artifact_key,
            "run_id": self.run_id,
            "producing_agent": self.producing_agent,
            "produced_at": self.produced_at,
            "validation_status": self.validation_status,
            "validation_level": self.validation_level,
            "confidence": round(self.confidence, 3),
            "lineage_tags": self.lineage_tags,
            "downstream_error_count": self.downstream_error_count,
            "downstream_error_procs": self.downstream_error_procs,
        }

    @classmethod
    def from_dict(cls, data: dict) -> ArtifactProvenance:
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# Provenance computation for converted code
# ---------------------------------------------------------------------------

def compute_conversion_provenance(
    proc_name: str,
    run_id: str,
    validation_outcome: str,
    validation_level: int,
    total_self_corrections: int,
    todo_count: int,
    has_unresolved_deps: bool,
    has_write_conflict: bool,
    replan_count: int,
) -> ArtifactProvenance:
    """
    Compute provenance for a converted proc's code.

    Called after C5 assembly + validation completes.
    The confidence score is a weighted combination of signals —
    lower confidence = higher risk if used as a callee signature.

    Confidence scoring:
      1.0  = VALIDATED at LEVEL_3, zero corrections, zero TODOs
      0.8  = VALIDATED at LEVEL_2, minor corrections
      0.6  = PARTIAL (has TODOs or level < 3)
      0.4  = Multiple self-corrections + unresolved deps
      0.2  = FAILED validation but code exists (replan candidate)
      0.0  = No code produced
    """
    tags: list[str] = []
    confidence = 1.0

    # Validation outcome
    if validation_outcome == "PASS":
        pass  # Full confidence from validation
    elif validation_outcome == "PARTIAL":
        confidence -= 0.25
        tags.append("partial_validation")
    elif validation_outcome == "FAIL":
        confidence -= 0.6
        tags.append("failed_validation")
    else:
        confidence -= 0.4
        tags.append("unvalidated")

    # Validation depth
    if validation_level < 3:
        confidence -= 0.1 * (3 - validation_level)
        tags.append(f"validation_level_{validation_level}")

    # Self-corrections applied
    if total_self_corrections > 0:
        penalty = min(0.15, total_self_corrections * 0.05)
        confidence -= penalty
        tags.append(f"self_corrected_{total_self_corrections}x")

    # TODO markers (unmapped constructs)
    if todo_count > 0:
        penalty = min(0.2, todo_count * 0.04)
        confidence -= penalty
        tags.append(f"has_{todo_count}_todos")

    # Unresolved dependencies
    if has_unresolved_deps:
        confidence -= 0.15
        tags.append("unresolved_deps_present")

    # Write conflict involvement
    if has_write_conflict:
        tags.append("write_conflict_table")
        # Not a confidence penalty per se, but a lineage signal

    # Replans
    if replan_count > 0:
        penalty = min(0.2, replan_count * 0.1)
        confidence -= penalty
        tags.append(f"replanned_{replan_count}x")

    # Clamp to [0.0, 1.0]
    confidence = max(0.0, min(1.0, confidence))

    return ArtifactProvenance(
        artifact_type="converted_code",
        artifact_key=proc_name,
        run_id=run_id,
        producing_agent="conversion",
        produced_at=datetime.now(timezone.utc).isoformat(),
        validation_status=validation_outcome or "UNVALIDATED",
        validation_level=validation_level,
        confidence=confidence,
        lineage_tags=tags,
    )


# ---------------------------------------------------------------------------
# Callee signature confidence tagging
# ---------------------------------------------------------------------------

# Threshold below which callee signatures get a LOW_CONFIDENCE warning
# injected into the caller's C2 prompt. Read from config at call time.
def _get_low_confidence_threshold() -> float:
    try:
        from sql_migration.core.config_loader import get_config
        return get_config().provenance.low_confidence_threshold
    except Exception:
        return 0.6  # Safe default


def build_callee_sig_with_provenance(
    callee_name: str,
    signature: str,
    provenance: ArtifactProvenance | None,
) -> tuple[str, bool]:
    """
    Wrap a callee signature with provenance context for C2 injection.

    Returns:
        (enriched_signature, is_low_confidence)

    If provenance is missing or confidence >= threshold:
        Returns the raw signature unchanged.

    If confidence < threshold:
        Returns the signature wrapped with a LOW_CONFIDENCE warning
        that tells the LLM to treat the callee's interface cautiously.
    """
    threshold = _get_low_confidence_threshold()

    if provenance is None:
        # No provenance data — treat as unknown, add caution tag
        warning = (
            f"{signature}\n"
            f"    # ⚠️ CAUTION: No provenance data for '{callee_name}'. "
            f"Verify this signature matches the callee's actual interface."
        )
        return warning, True

    if provenance.confidence >= threshold:
        return signature, False

    # Low confidence — build a detailed warning
    tag_str = ", ".join(provenance.lineage_tags[:4]) if provenance.lineage_tags else "unknown"

    warning = (
        f"{signature}\n"
        f"    # ⚠️ LOW CONFIDENCE ({provenance.confidence:.1%}) for '{callee_name}'. "
        f"Signals: [{tag_str}]. "
        f"This callee's conversion may be incomplete — verify parameter names "
        f"and return types against the original source proc declaration."
    )
    return warning, True


# ---------------------------------------------------------------------------
# Provenance store helper (operates on the existing ArtifactStore)
# ---------------------------------------------------------------------------

class ProvenanceTracker:
    """
    Manages provenance records for shared artifacts within a run.

    Storage: artifacts/{run_id}/provenance/ directory.
    One JSON file per artifact: provenance/{artifact_type}_{artifact_key}.json

    This is intentionally lightweight — provenance is read-only metadata
    that supplements existing artifacts. It never blocks the pipeline.
    """

    def __init__(self, store) -> None:
        """
        Args:
            store: ArtifactStore instance for this run.
        """
        self.store = store

    def write(self, prov: ArtifactProvenance) -> None:
        """Persist a provenance record to filesystem and optionally to DB."""
        key = f"{prov.artifact_type}_{prov.artifact_key}"
        try:
            self.store.write("provenance", f"{key}.json", prov.to_dict())
        except Exception:
            # Provenance is advisory — never block the pipeline on write failure
            pass

        # v6-fix (Gap 10): Also write to converted_code.provenance_json in DB
        # when the artifact type is a converted proc.
        if (prov.artifact_type == "converted_code"
                and hasattr(self.store, '_db') and self.store._db):
            try:
                from sql_migration.core.db_models import get_session, ConvertedCode
                from sqlalchemy import select, update
                with get_session(self.store._db._engine) as session:
                    session.execute(
                        update(ConvertedCode)
                        .where(ConvertedCode.run_id == self.store.run_id)
                        .where(ConvertedCode.proc_name == prov.artifact_key)
                        .values(provenance_json=prov.to_dict())
                    )
            except Exception:
                pass  # DB provenance is advisory

    def read(self, artifact_type: str, artifact_key: str) -> ArtifactProvenance | None:
        """
        Read a provenance record.
        Returns None if not found (graceful — provenance is optional).
        """
        key = f"{artifact_type}_{artifact_key}"
        try:
            data = self.store.read("provenance", f"{key}.json")
            return ArtifactProvenance.from_dict(data)
        except (FileNotFoundError, KeyError, TypeError):
            return None

    def read_callee_provenance(self, callee_name: str) -> ArtifactProvenance | None:
        """Shorthand for reading converted_code provenance."""
        return self.read("converted_code", callee_name)

    def increment_downstream_errors(
        self,
        artifact_type: str,
        artifact_key: str,
        failing_proc: str,
    ) -> None:
        """
        Record that a downstream proc failed with an error potentially
        caused by this shared artifact.

        Called by GlobalErrorAggregator when a systematic pattern is detected.
        """
        prov = self.read(artifact_type, artifact_key)
        if prov is None:
            return

        prov.downstream_error_count += 1
        if failing_proc not in prov.downstream_error_procs:
            prov.downstream_error_procs.append(failing_proc)

        self.write(prov)
