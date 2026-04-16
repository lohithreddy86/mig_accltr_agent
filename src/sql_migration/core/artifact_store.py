"""
artifact_store.py
=================
All inter-agent file I/O goes through this module.
Agents never construct file paths manually — they call ArtifactStore methods.

Design:
- Every artifact is a JSON file (or raw text for code files)
- Atomic writes: write to .tmp then rename to prevent partial reads
- Metadata envelope: every JSON artifact is wrapped with run_id, agent,
  timestamp so the UI can display provenance
- ArtifactStore is instantiated once per run and passed to each agent

Usage:
    store = ArtifactStore(run_id="migration_20250323_143000")
    store.write("analysis", "dialect_profile.json", data_dict)
    profile = store.read("analysis", "dialect_profile.json")
    exists = store.exists("analysis", "manifest.json")
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sql_migration.core.config_loader import get_config
from sql_migration.core.logger import get_logger

log = get_logger("artifact_store")


# ---------------------------------------------------------------------------
# Envelope schema
# ---------------------------------------------------------------------------

def _wrap(data: Any, agent: str, run_id: str) -> dict:
    """Wrap artifact data in a metadata envelope."""
    return {
        "_meta": {
            "run_id":    run_id,
            "agent":     agent,
            "written_at": datetime.now(timezone.utc).isoformat(),
            "version":   "1",
        },
        "data": data,
    }


def _unwrap(envelope: dict) -> Any:
    """Extract data from envelope. Handles both wrapped and bare artifacts."""
    if isinstance(envelope, dict) and "_meta" in envelope and "data" in envelope:
        return envelope["data"]
    return envelope  # bare artifact (legacy / external)


# ---------------------------------------------------------------------------
# ArtifactStore
# ---------------------------------------------------------------------------

class ArtifactStore:
    """
    Central I/O layer for all inter-agent artifacts.

    All methods are synchronous (file I/O is fast relative to LLM calls).
    Thread-safe for single-writer-per-file scenarios (atomic rename).

    Optional: accepts a DatabaseStore for DB-backed queries (list_frozen_procs,
    update_run_status, etc). When db is None, falls back to filesystem-only.
    """

    def __init__(self, run_id: str, db=None) -> None:
        cfg = get_config()
        self.run_id   = run_id
        # Each run gets its own subdirectory: {artifact_store}/{run_id}/
        # This guarantees full isolation between runs and makes
        # list_frozen_procs(), artifact_summary() etc. automatically scoped.
        self.base_dir = Path(cfg.paths.artifact_store) / run_id
        self.subdirs  = cfg.paths.subdirs
        # Optional DatabaseStore — when provided, DB-backed queries are
        # used instead of filesystem globs. Falls back to filesystem when None.
        self._db = db
        self._ensure_dirs()

    # ------------------------------------------------------------------
    # Directory helpers
    # ------------------------------------------------------------------

    def _subdir(self, agent: str) -> Path:
        subdir_name = getattr(self.subdirs, agent, agent)
        return self.base_dir / subdir_name

    def _ensure_dirs(self) -> None:
        """Create all artifact subdirectories upfront."""
        for field in type(self.subdirs).model_fields:
            path = self._subdir(field)
            path.mkdir(parents=True, exist_ok=True)
        # Also create run-specific subdir for human feedback
        (self.base_dir / self.subdirs.feedback).mkdir(parents=True, exist_ok=True)

    def path(self, agent: str, filename: str) -> Path:
        """Return absolute path for an artifact without reading it."""
        return self._subdir(agent) / filename

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def write(
        self,
        agent: str,
        filename: str,
        data: Any,
        wrap: bool = True,
    ) -> Path:
        """
        Write data to an artifact file.
        Uses atomic write (tmp → rename) to prevent partial reads.

        Args:
            agent:    Agent subdirectory key (e.g. "analysis", "analysis")
            filename: File name (e.g. "dialect_profile.json")
            data:     Data to write. Dicts/lists → JSON. Strings → raw text.
            wrap:     If True, wrap in metadata envelope (default). Set False
                      for code files (.py, .sql) written by Conversion Agent.

        Returns:
            Absolute path of written file.
        """
        dest = self._subdir(agent) / filename
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + f".tmp_{uuid.uuid4().hex[:8]}")

        try:
            if isinstance(data, str) and not filename.endswith(".json"):
                # Raw text (code files)
                tmp.write_text(data, encoding="utf-8")
            else:
                payload = _wrap(data, agent, self.run_id) if wrap else data
                tmp.write_text(
                    json.dumps(payload, indent=2, default=str),
                    encoding="utf-8",
                )
            os.replace(tmp, dest)  # atomic on POSIX
        except Exception:
            tmp.unlink(missing_ok=True)
            raise

        log.debug("artifact_written", agent=agent, file=filename, path=str(dest))
        return dest

    def write_code(self, agent: str, filename: str, code: str) -> Path:
        """Convenience: write a raw code file (no JSON envelope)."""
        return self.write(agent, filename, code, wrap=False)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def read(self, agent: str, filename: str) -> Any:
        """
        Read and return artifact data.
        Automatically unwraps the metadata envelope.
        Raises FileNotFoundError if artifact does not exist.
        """
        path = self._subdir(agent) / filename
        if not path.exists():
            raise FileNotFoundError(
                f"Artifact not found: {agent}/{filename} (looked in {path})"
            )

        content = path.read_text(encoding="utf-8")

        if filename.endswith(".json"):
            try:
                parsed = json.loads(content)
                return _unwrap(parsed)
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"Artifact {agent}/{filename} contains invalid JSON: {e}"
                ) from e

        return content  # raw text (code files)

    def read_code(self, agent: str, filename: str) -> str:
        """Read a raw code file as string."""
        return self.read(agent, filename)

    # ------------------------------------------------------------------
    # Existence / listing
    # ------------------------------------------------------------------

    def exists(self, agent: str, filename: str) -> bool:
        return (self._subdir(agent) / filename).exists()

    def list_files(self, agent: str, pattern: str = "*.json") -> list[Path]:
        """List artifact files in an agent's subdirectory."""
        return sorted(self._subdir(agent).glob(pattern))

    def list_matching(self, agent: str, prefix: str) -> list[Path]:
        """List files whose name starts with prefix."""
        return sorted(
            p for p in self._subdir(agent).iterdir()
            if p.name.startswith(prefix)
        )

    # ------------------------------------------------------------------
    # Metadata inspection (for UI)
    # ------------------------------------------------------------------

    def read_meta(self, agent: str, filename: str) -> dict | None:
        """Return the _meta envelope without unwrapping data. Returns None if bare."""
        path = self._subdir(agent) / filename
        if not path.exists() or not filename.endswith(".json"):
            return None
        try:
            parsed = json.loads(path.read_text(encoding="utf-8"))
            return parsed.get("_meta") if isinstance(parsed, dict) else None
        except Exception:
            return None

    def artifact_summary(self) -> list[dict]:
        """
        Return a flat list of all artifacts with metadata.
        Used by the Streamlit UI to populate the artifact browser.
        """
        summary = []
        for field in type(self.subdirs).model_fields:
            subdir = self._subdir(field)
            if not subdir.exists():
                continue
            for f in sorted(subdir.iterdir()):
                if f.suffix in (".json", ".py", ".sql", ".txt"):
                    meta = self.read_meta(field, f.name) or {}
                    summary.append({
                        "agent":      field,
                        "filename":   f.name,
                        "path":       str(f),
                        "size_bytes": f.stat().st_size,
                        "written_at": meta.get("written_at", ""),
                        "run_id":     meta.get("run_id", ""),
                    })
        return summary

    # ------------------------------------------------------------------
    # Human feedback I/O
    # ------------------------------------------------------------------

    def write_feedback_bundle(self, proc_name: str, bundle: dict) -> Path:
        """Write a FROZEN proc's developer feedback bundle."""
        filename = f"developer_feedback_{proc_name}.json"
        return self.write("feedback", filename, bundle)

    def read_feedback_resolution(self, proc_name: str) -> dict | None:
        """
        Read a human's resolution for a FROZEN proc.
        Returns None if the resolution file does not exist yet.
        """
        filename = f"feedback_resolved_{proc_name}.json"
        if not self.exists("feedback", filename):
            return None
        return self.read("feedback", filename)

    def write_feedback_resolution(self, proc_name: str, resolution: dict) -> Path:
        """Write a human's resolution (used by Streamlit UI submit handler)."""
        filename = f"feedback_resolved_{proc_name}.json"
        return self.write("feedback", filename, resolution)

    def consume_feedback_resolution(self, proc_name: str) -> bool:
        """
        Consume (archive) a resolution file after the Orchestrator has
        successfully applied it.

        Renames feedback_resolved_{proc}.json → feedback_resolved_{proc}_applied_{ts}.json.
        This prevents the Orchestrator from re-applying a stale resolution
        if the proc freezes again after a subsequent conversion attempt.
        The renamed file is preserved for audit trail.

        Returns True if a file was consumed, False if no resolution existed.
        """
        filename = f"feedback_resolved_{proc_name}.json"
        source = self._subdir("feedback") / filename
        if not source.exists():
            return False

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        archived = self._subdir("feedback") / f"feedback_resolved_{proc_name}_applied_{ts}.json"
        try:
            source.rename(archived)
            log.debug("feedback_resolution_consumed",
                      proc=proc_name, archived=str(archived.name))
            return True
        except OSError as e:
            log.warning("feedback_resolution_consume_failed",
                        proc=proc_name, error=str(e)[:100])
            return False

    def list_frozen_procs(self) -> list[str]:
        """Return proc names that are FROZEN and awaiting resolution.

        Uses DB query when DatabaseStore is available (fast, consistent with
        checkpoint state). Falls back to filesystem glob on feedback bundle
        files when DB is not configured.
        """
        # DB path: SELECT proc_name WHERE status='FROZEN' — consistent with checkpoint
        if self._db:
            try:
                return self._db.get_frozen_proc_names()
            except Exception:
                pass  # Fall through to filesystem

        # Filesystem fallback: procs with feedback bundle but no resolution file
        frozen = []
        for f in self._subdir("feedback").glob("developer_feedback_*.json"):
            proc_name = f.stem.replace("developer_feedback_", "")
            if not (self._subdir("feedback") / f"feedback_resolved_{proc_name}.json").exists():
                frozen.append(proc_name)
        return sorted(frozen)

    def list_resolved_procs(self) -> list[str]:
        """Return proc names that have been resolved by a human.

        Excludes consumed (archived) resolution files — those have
        '_applied_' in the filename and represent already-processed resolutions.
        """
        return sorted(
            f.stem.replace("feedback_resolved_", "")
            for f in self._subdir("feedback").glob("feedback_resolved_*.json")
            if "_applied_" not in f.stem
        )

    # ------------------------------------------------------------------
    # DB-backed run tracking (delegates to DatabaseStore when available)
    # ------------------------------------------------------------------

    def update_run_status(self, status: str) -> None:
        """Update the migration run status in the database.

        Silently no-ops when DatabaseStore is not configured.
        Called by main.py at pipeline start (RUNNING) and end (COMPLETE/FAILED).
        """
        if self._db:
            try:
                self._db.update_run_status(status)
            except Exception as e:
                log.debug("db_update_run_status_failed", error=str(e)[:100])

    def refresh_run_counts(self, checkpoint_data: dict) -> None:
        """Update pipeline-level counts in the migration_runs DB row.

        Called after orchestration completes with the final checkpoint dict.
        Silently no-ops when DatabaseStore is not configured.
        """
        if self._db:
            try:
                self._db.refresh_run_counts(checkpoint_data)
            except Exception as e:
                log.debug("db_refresh_run_counts_failed", error=str(e)[:100])
