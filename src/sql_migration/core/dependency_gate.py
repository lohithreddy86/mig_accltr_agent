"""
dependency_gate.py
==================
Pre-Planning Dependency Gate.

Runs between Analysis Agent (A3) and Planning Agent (P1).
Surfaces unresolved external dependencies BEFORE any conversion compute.

Design rationale (informed by CrackSQL SIGMOD 2025 + MetaRAG 2025):
  LLMs hallucinate more when forced to handle unknown references — they
  invent plausible-looking function signatures, fake return types, and
  silently produce code that compiles but is semantically wrong.  The
  current system handles this gracefully via PARTIAL status, but the
  core problem is that the user simply forgot to upload a file.

  This gate catches that case *before* any LLM conversion call:
    - If external_deps exist → PAUSE, surface them with full context
    - User can: upload the missing file, confirm "treat as TODO stub",
      or provide the UDF body inline
    - Gate produces a DependencyResolution artifact consumed by Planning

Anti-hallucination principle:
  "Never let the LLM guess what it could be told."
  If a function definition exists and the user has it, get it into the
  system.  If it doesn't exist, make the LLM explicitly aware it's
  unknown rather than letting it silently infer behaviour from the name.

Integration:
  Called by main.py between analysis_agent.run() and planning_agent.run().
  In Streamlit mode: renders an interactive resolution UI.
  In CLI mode: checks for a pre-existing resolution file or pauses.

Artifact written:
  artifacts/01_analysis/dependency_resolution.json
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sql_migration.core.artifact_store import ArtifactStore
from sql_migration.core.config_loader import get_config
from sql_migration.core.logger import get_logger

log = get_logger("dependency_gate")


# ---------------------------------------------------------------------------
# Resolution types per external dependency
# ---------------------------------------------------------------------------

class DepAction:
    """Resolution actions a user can take for each unresolved dependency."""
    UPLOAD = "upload"              # User will upload the missing file
    STUB_TODO = "stub_todo"       # Treat as unknown UDF → TODO markers
    INLINE_BODY = "inline_body"   # User provides the function body inline
    SKIP_CALLERS = "skip_callers" # Skip all procs that call this dep


@dataclass
class ExternalDepInfo:
    """Full context for one unresolved external dependency."""
    dep_name: str
    called_by: list[str]           # Proc names that call this dep
    call_count: int                # Total call sites across all procs
    sample_call_sites: list[str]   # Up to 3 example lines showing the call
    resolution: str = ""           # One of DepAction values
    uploaded_path: str = ""        # Path to uploaded file (if action=UPLOAD)
    inline_body: str = ""          # Function body (if action=INLINE_BODY)


@dataclass
class DependencyGateResult:
    """Result of the dependency gate check."""
    has_unresolved: bool = False
    deps: list[ExternalDepInfo] = field(default_factory=list)
    all_resolved: bool = False
    # Summary for downstream agents
    stub_deps: list[str] = field(default_factory=list)      # Deps to treat as TODO
    skip_deps: list[str] = field(default_factory=list)       # Deps whose callers to skip
    uploaded_files: list[str] = field(default_factory=list)   # New files to re-analyze
    inline_bodies: dict[str, str] = field(default_factory=dict)  # {dep: body}

    def to_dict(self) -> dict:
        return {
            "has_unresolved": self.has_unresolved,
            "all_resolved": self.all_resolved,
            "deps": [
                {
                    "dep_name": d.dep_name,
                    "called_by": d.called_by,
                    "call_count": d.call_count,
                    "sample_call_sites": d.sample_call_sites,
                    "resolution": d.resolution,
                }
                for d in self.deps
            ],
            "stub_deps": self.stub_deps,
            "skip_deps": self.skip_deps,
            "uploaded_files": self.uploaded_files,
            # Full body persisted — truncation only in UI/logging, never in artifacts
            "inline_bodies": dict(self.inline_bodies),
            "resolved_at": datetime.now(timezone.utc).isoformat(),
        }


# ---------------------------------------------------------------------------
# Gate logic
# ---------------------------------------------------------------------------

class DependencyGate:
    """
    Pre-Planning gate that checks for unresolved external dependencies
    and requires explicit resolution before proceeding.

    Usage:
        gate = DependencyGate(store)
        result = gate.check()

        if result.has_unresolved and not result.all_resolved:
            # In Streamlit: show resolution UI
            # In CLI: pause and wait for resolution file
            gate.wait_for_resolution(result)

        # After resolution:
        if result.uploaded_files:
            # Re-run Analysis with new files appended
            ...
        if result.inline_bodies:
            # Inject inline UDF bodies into adapter/construct_map
            ...
    """

    def __init__(self, store: ArtifactStore) -> None:
        self.store = store

    def check(self) -> DependencyGateResult:
        """
        Check Analysis artifacts for unresolved external dependencies.
        Returns DependencyGateResult describing what needs resolution.
        """
        # Check for existing resolution first (crash recovery / pre-populated)
        if self.store.exists("analysis", "dependency_resolution.json"):
            try:
                existing = self.store.read("analysis", "dependency_resolution.json")
                if existing.get("all_resolved"):
                    log.info("dependency_gate_already_resolved")
                    return self._parse_existing_resolution(existing)
            except Exception:
                pass

        # Load dependency graph from Analysis
        try:
            dep_graph = self.store.read("analysis", "dependency_graph.json")
        except FileNotFoundError:
            log.info("dependency_gate_no_graph")
            return DependencyGateResult()

        external_deps = dep_graph.get("external_deps", {})
        if not external_deps:
            log.info("dependency_gate_no_external_deps")
            return DependencyGateResult()

        # Load manifest for call-site context
        try:
            manifest = self.store.read("analysis", "manifest.json")
        except FileNotFoundError:
            manifest = {"procs": []}

        # Build detailed dep info
        deps: list[ExternalDepInfo] = []
        all_callers: dict[str, list[str]] = {}  # dep_name → [caller_procs]

        for proc_name, dep_list in external_deps.items():
            for dep_name in dep_list:
                if dep_name not in all_callers:
                    all_callers[dep_name] = []
                all_callers[dep_name].append(proc_name)

        for dep_name, callers in all_callers.items():
            # Extract sample call sites from manifest
            sample_sites = self._extract_call_sites(dep_name, callers, manifest)
            total_calls = sum(
                self._count_calls_in_proc(dep_name, proc, manifest)
                for proc in callers
            )

            deps.append(ExternalDepInfo(
                dep_name=dep_name,
                called_by=callers,
                call_count=max(total_calls, len(callers)),
                sample_call_sites=sample_sites[:3],
            ))

        # Sort by impact: most callers first
        deps.sort(key=lambda d: len(d.called_by), reverse=True)

        result = DependencyGateResult(
            has_unresolved=True,
            deps=deps,
        )

        log.warning(
            "dependency_gate_unresolved_found",
            dep_count=len(deps),
            total_affected_procs=len(set(
                proc for d in deps for proc in d.called_by
            )),
            deps=[d.dep_name for d in deps],
        )

        return result

    def apply_resolutions(
        self,
        result: DependencyGateResult,
        resolutions: dict[str, dict],
    ) -> DependencyGateResult:
        """
        Apply user resolutions to the gate result.

        Args:
            result: The DependencyGateResult from check()
            resolutions: {dep_name: {"action": "stub_todo|upload|inline_body|skip_callers",
                                      "path": "...",  # for upload
                                      "body": "...",  # for inline_body
                                     }}

        Returns:
            Updated DependencyGateResult with all_resolved=True if complete.
        """
        for dep in result.deps:
            res = resolutions.get(dep.dep_name, {})
            action = res.get("action", DepAction.STUB_TODO)
            dep.resolution = action

            if action == DepAction.UPLOAD:
                dep.uploaded_path = res.get("path", "")
                if dep.uploaded_path:
                    result.uploaded_files.append(dep.uploaded_path)
            elif action == DepAction.INLINE_BODY:
                dep.inline_body = res.get("body", "")
                if dep.inline_body:
                    result.inline_bodies[dep.dep_name] = dep.inline_body
            elif action == DepAction.STUB_TODO:
                result.stub_deps.append(dep.dep_name)
            elif action == DepAction.SKIP_CALLERS:
                result.skip_deps.append(dep.dep_name)

        result.all_resolved = all(d.resolution for d in result.deps)

        # Persist resolution
        self.store.write(
            "analysis",
            "dependency_resolution.json",
            result.to_dict(),
        )

        log.info(
            "dependency_gate_resolved",
            stub_count=len(result.stub_deps),
            upload_count=len(result.uploaded_files),
            inline_count=len(result.inline_bodies),
            skip_count=len(result.skip_deps),
        )

        return result

    def get_enhanced_unresolved_context(
        self,
        result: DependencyGateResult,
    ) -> dict[str, str]:
        """
        Build enriched unresolved-dep context for injection into C2 prompts.

        For stub_deps: a clear "UDF unknown, emit TODO" instruction.
        For inline_bodies: the actual function signature/body so the LLM
        can translate the call correctly instead of guessing.

        Returns:
            {dep_name: context_string} — injected into DispatchTask
        """
        context: dict[str, str] = {}

        for dep_name in result.stub_deps:
            context[dep_name] = (
                f"UNKNOWN UDF: {dep_name} — definition not available. "
                f"Do NOT guess its behavior. Emit: "
                f"# TODO: UNMAPPED — original: {dep_name}(...) — UDF definition unknown"
            )

        for dep_name, body in result.inline_bodies.items():
            # Provide the actual body so the LLM can translate correctly
            context[dep_name] = (
                f"UDF DEFINITION (provided by developer):\n"
                f"{body[:500]}\n"
                f"Translate this function alongside the proc code."
            )

        return context

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def _extract_call_sites(
        self, dep_name: str, callers: list[str], manifest: dict,
    ) -> list[str]:
        """Extract example lines where dep_name is called."""
        # We don't have raw code access here (by design — LLM never sees bulk code).
        # Return caller proc names with context from manifest.
        sites = []
        for proc in callers[:3]:
            proc_entry = next(
                (p for p in manifest.get("procs", []) if p["name"] == proc),
                None,
            )
            if proc_entry:
                site = (
                    f"  {proc} (lines {proc_entry.get('start_line', '?')}-"
                    f"{proc_entry.get('end_line', '?')}, "
                    f"{proc_entry.get('line_count', '?')} lines)"
                )
                sites.append(site)
        return sites

    def _count_calls_in_proc(
        self, dep_name: str, proc_name: str, manifest: dict,
    ) -> int:
        """Count how many times dep_name appears in a proc's calls list."""
        proc_entry = next(
            (p for p in manifest.get("procs", []) if p["name"] == proc_name),
            None,
        )
        if not proc_entry:
            return 1
        return proc_entry.get("calls", []).count(dep_name) or 1

    def _parse_existing_resolution(self, data: dict) -> DependencyGateResult:
        """Parse a previously persisted resolution."""
        result = DependencyGateResult(
            has_unresolved=data.get("has_unresolved", False),
            all_resolved=data.get("all_resolved", False),
            stub_deps=data.get("stub_deps", []),
            skip_deps=data.get("skip_deps", []),
            uploaded_files=data.get("uploaded_files", []),
            inline_bodies=data.get("inline_bodies", {}),
        )
        for d in data.get("deps", []):
            result.deps.append(ExternalDepInfo(
                dep_name=d["dep_name"],
                called_by=d.get("called_by", []),
                call_count=d.get("call_count", 0),
                sample_call_sites=d.get("sample_call_sites", []),
                resolution=d.get("resolution", ""),
            ))
        return result
