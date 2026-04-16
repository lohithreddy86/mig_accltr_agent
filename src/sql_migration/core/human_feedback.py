"""
human_feedback.py
=================
Human-in-the-loop handler for FROZEN procs.

When the Orchestrator freezes a proc after exhausting replan retries,
it calls HumanFeedbackHandler.submit_for_review(). This:
  1. Assembles a complete feedback bundle with all context
  2. Writes developer_feedback_{proc}.json to the artifact store
  3. Optionally sends a webhook/email notification
  4. In "pause" mode: blocks the pipeline until a resolution arrives
  5. In "file" mode: continues pipeline and checks back later

The Streamlit UI reads feedback bundles and writes resolution files.
The Orchestrator calls poll_for_resolution() to check if a human responded.

Resolution actions (defined in config.yaml):
  - manual_rewrite_provided: Human supplies corrected code
  - mark_skip:               Skip this proc entirely
  - override_pass:           Human confirms output is acceptable as-is
  - replan_with_notes:       Human adds notes for Planning Agent to retry
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from sql_migration.core.artifact_store import ArtifactStore
from sql_migration.core.config_loader import get_config
from sql_migration.core.logger import get_logger

log = get_logger("human_feedback")


# ---------------------------------------------------------------------------
# Bundle builder
# ---------------------------------------------------------------------------

def build_feedback_bundle(
    proc_name:          str,
    manifest_entry:     dict,
    complexity_entry:   dict,
    chunk_codes:        dict[str, str],    # {chunk_id: converted_code}
    validation_reports: dict[str, dict],  # {chunk_id: validation_result}
    error_history:      list[dict],
    manual_review_items: list[dict],
    replan_count:       int,
    retry_count:        int,
) -> dict:
    """
    Assemble the complete developer feedback bundle for a FROZEN proc.
    This is what appears in the Streamlit feedback UI.
    """
    return {
        "proc_name":          proc_name,
        "frozen_at":          datetime.now(timezone.utc).isoformat(),
        "replan_count":       replan_count,
        "retry_count":        retry_count,
        "status":             "FROZEN",

        # Source context
        "manifest_entry":     manifest_entry,
        "complexity_entry":   complexity_entry,

        # Conversion artifacts
        "chunk_codes":        chunk_codes,

        # Validation reports (what failed)
        "validation_reports": validation_reports,

        # Error trail
        "error_history":      error_history,

        # UNMAPPED constructs requiring manual attention
        "manual_review_items": manual_review_items,

        # Suggested resolution actions
        "available_actions": get_config().human_feedback.valid_actions,

        # Metadata for UI display
        "bundle_version":     "1",
    }


# ---------------------------------------------------------------------------
# HumanFeedbackHandler
# ---------------------------------------------------------------------------

class HumanFeedbackHandler:
    """
    Manages the human feedback loop for FROZEN procs.
    Shared instance across the Orchestrator.
    """

    def __init__(self, store: ArtifactStore) -> None:
        self.store  = store
        self.cfg    = get_config()
        self.fb_cfg = self.cfg.orchestrator.human_feedback

    # ------------------------------------------------------------------
    # Submit a proc for human review
    # ------------------------------------------------------------------

    def submit_for_review(
        self,
        proc_name:           str,
        manifest_entry:      dict,
        complexity_entry:    dict,
        chunk_codes:         dict[str, str],
        validation_reports:  dict[str, dict],
        error_history:       list[dict],
        manual_review_items: list[dict],
        replan_count:        int,
        retry_count:         int,
    ) -> None:
        """
        Write feedback bundle and optionally pause until resolved.
        Called by Orchestrator when a proc is FROZEN.
        """
        bundle = build_feedback_bundle(
            proc_name=proc_name,
            manifest_entry=manifest_entry,
            complexity_entry=complexity_entry,
            chunk_codes=chunk_codes,
            validation_reports=validation_reports,
            error_history=error_history,
            manual_review_items=manual_review_items,
            replan_count=replan_count,
            retry_count=retry_count,
        )

        self.store.write_feedback_bundle(proc_name, bundle)

        log.info(
            "proc_frozen_feedback_written",
            proc=proc_name,
            replan_count=replan_count,
            manual_items=len(manual_review_items),
        )

        # Optional: notify via webhook
        self._notify(proc_name, bundle)

        # Pause mode: block until resolution or timeout
        if self.fb_cfg.mode == "pause":
            self._wait_for_resolution(proc_name)

    # ------------------------------------------------------------------
    # Poll for resolution
    # ------------------------------------------------------------------

    def poll_for_resolution(self, proc_name: str) -> dict | None:
        """
        Check if a human has submitted a resolution for this proc.
        Returns the resolution dict if found, None otherwise.

        Called by Orchestrator in its dispatch loop.
        """
        return self.store.read_feedback_resolution(proc_name)

    def all_pending_frozen(self) -> list[str]:
        """Return proc names that are FROZEN and awaiting resolution."""
        return self.store.list_frozen_procs()

    # ------------------------------------------------------------------
    # Submit resolution (called by Streamlit UI)
    # ------------------------------------------------------------------

    def submit_resolution(
        self,
        proc_name: str,
        action:    str,
        notes:     str = "",
        corrected_code: str = "",
        uploaded_file_paths: list[str] | None = None,
    ) -> dict:
        """
        Submit a human resolution for a FROZEN proc.
        Validates the action and writes feedback_resolved_{proc}.json.

        Args:
            proc_name:           Name of the frozen proc
            action:              One of config.human_feedback.valid_actions
            notes:               Free-text notes for replan_with_notes action
            corrected_code:      Full corrected code for manual_rewrite_provided action
            uploaded_file_paths: File paths for upload_source_and_reanalyze action

        Returns:
            The written resolution dict.
        """
        valid_actions = self.cfg.human_feedback.valid_actions
        if action not in valid_actions:
            raise ValueError(
                f"Invalid action '{action}'. Must be one of: {valid_actions}"
            )

        if action == "manual_rewrite_provided" and not corrected_code.strip():
            raise ValueError(
                "Action 'manual_rewrite_provided' requires corrected_code to be non-empty."
            )

        if action == "replan_with_notes" and not notes.strip():
            raise ValueError(
                "Action 'replan_with_notes' requires notes to be non-empty."
            )

        if action == "upload_source_and_reanalyze" and not uploaded_file_paths:
            raise ValueError(
                "Action 'upload_source_and_reanalyze' requires at least one uploaded file."
            )

        resolution = {
            "proc_name":           proc_name,
            "action":              action,
            "notes":               notes,
            "corrected_code":      corrected_code,
            "uploaded_file_paths": uploaded_file_paths or [],
            "resolved_at":         datetime.now(timezone.utc).isoformat(),
            "resolved_by":         "human",
        }

        self.store.write_feedback_resolution(proc_name, resolution)

        log.info(
            "feedback_resolution_submitted",
            proc=proc_name,
            action=action,
            has_code=bool(corrected_code),
            uploaded_files=len(uploaded_file_paths or []),
            notes_length=len(notes),
        )

        return resolution

    # ------------------------------------------------------------------
    # Pause mode: wait for resolution
    # ------------------------------------------------------------------

    def _wait_for_resolution(self, proc_name: str) -> None:
        """Block until a resolution is submitted or timeout is reached."""
        timeout   = self.fb_cfg.pause_timeout
        poll_interval = 5  # seconds
        elapsed   = 0

        log.info(
            "feedback_waiting_for_resolution",
            proc=proc_name,
            timeout_seconds=timeout,
        )

        while elapsed < timeout:
            resolution = self.poll_for_resolution(proc_name)
            if resolution:
                log.info(
                    "feedback_resolution_received",
                    proc=proc_name,
                    action=resolution.get("action"),
                )
                return
            time.sleep(poll_interval)
            elapsed += poll_interval

        log.warning(
            "feedback_wait_timeout",
            proc=proc_name,
            timeout_seconds=timeout,
        )
        # Auto-continue without resolution (pipeline skips this proc)

    # ------------------------------------------------------------------
    # Webhook notification
    # ------------------------------------------------------------------

    def _notify(self, proc_name: str, bundle: dict) -> None:
        """Send a notification when a proc is FROZEN (optional)."""
        import os
        webhook_url = os.environ.get("FEEDBACK_WEBHOOK_URL", "")
        if not webhook_url:
            return

        payload = {
            "text": (
                f"🔴 SQL Migration: Proc *{proc_name}* is FROZEN and needs review.\n"
                f"Replan attempts: {bundle['replan_count']}/{self.cfg.planning.loop_guards.frozen_after}\n"
                f"Manual review items: {len(bundle['manual_review_items'])}\n"
                f"Open the feedback UI to resolve."
            )
        }

        try:
            with httpx.Client(timeout=10) as client:
                resp = client.post(webhook_url, json=payload)
                resp.raise_for_status()
                log.info("feedback_notification_sent", proc=proc_name)
        except Exception as e:
            log.warning("feedback_notification_failed", proc=proc_name, error=str(e))
