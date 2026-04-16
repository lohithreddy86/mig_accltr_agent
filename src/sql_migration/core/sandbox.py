"""
sandbox.py
==========
Podman-based sandbox for deterministic script execution.
Each call runs a Python script inside an isolated container with:
  - Artifact store mounted read-write at /artifacts
  - Sandbox scripts mounted read-only at /scripts
  - Source SQL files mounted read-only at a caller-specified path
  - Strict CPU/memory limits
  - Hard timeout (SIGKILL after N seconds)

Design:
  - Scripts are Python files in sandbox_scripts/{agent}/
  - Scripts communicate results via stdout (JSON) or by writing to /artifacts
  - stderr is captured and logged on failure
  - Container is always removed after execution (--rm)

Usage:
    sandbox = Sandbox(source_dir="/data/sql_files")
    result = sandbox.run(
        script="extraction/extract_structure.py",
        args={"filepath": "/source/procedures.sql", "adapter_path": "/artifacts/..."},
    )
    # result.stdout, result.returncode, result.stdout_json
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sql_migration.core.config_loader import get_config
from sql_migration.core.logger import get_logger

log = get_logger("sandbox")


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class SandboxResult:
    returncode:  int
    stdout:      str
    stderr:      str
    duration_s:  float
    script:      str
    timed_out:   bool = False

    @property
    def success(self) -> bool:
        return self.returncode == 0 and not self.timed_out

    @property
    def stdout_json(self) -> Any:
        """Parse stdout as JSON. Raises ValueError on failure."""
        try:
            return json.loads(self.stdout.strip())
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Sandbox script {self.script!r} stdout is not valid JSON: "
                f"{self.stdout[:300]!r}"
            ) from e

    def raise_on_failure(self) -> None:
        if self.timed_out:
            raise TimeoutError(
                f"Sandbox script {self.script!r} timed out "
                f"(limit: {get_config().sandbox.timeout_seconds}s)"
            )
        if self.returncode != 0:
            raise RuntimeError(
                f"Sandbox script {self.script!r} exited with code {self.returncode}.\n"
                f"STDERR:\n{self.stderr[-2000:]}"
            )


# ---------------------------------------------------------------------------
# Sandbox
# ---------------------------------------------------------------------------

class Sandbox:
    """
    Runs sandbox scripts in Podman containers.
    One Sandbox instance per pipeline run (shared across agents).
    """

    def __init__(self, source_dir: str | Path | None = None) -> None:
        """
        Args:
            source_dir: Host path to SQL source files.
                        Mounted read-only at /source inside the container.
                        Can be None if the agent doesn't need source files.
        """
        cfg = get_config()
        self.cfg        = cfg.sandbox
        self.source_dir = Path(source_dir) if source_dir else None

        self.artifact_store_path = Path(cfg.paths.artifact_store)
        self.scripts_path        = Path(cfg.paths.sandbox_scripts)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        script: str,
        args: dict[str, Any] | None = None,
        extra_mounts: list[dict] | None = None,
        env_overrides: dict[str, str] | None = None,
        timeout: int | None = None,
    ) -> SandboxResult:
        """
        Run a sandbox script inside a Podman container.

        Execution modes (in priority order):
          1. Podman (cfg.enabled=True, runtime available): full isolation
          2. Local (cfg.enabled=False OR runtime not found): direct subprocess
          3. Mock (cfg.enabled=False AND local fails): stub results (last resort)

        Args:
            script:        Path relative to sandbox_scripts/ (e.g. "extraction/parse_readme.py")
            args:          Dict of arguments passed to the script as --args JSON via stdin
            extra_mounts:  Additional bind mounts [{source, target, options}]
            env_overrides: Extra environment variables for this specific run
            timeout:       Override default timeout (seconds)

        Returns:
            SandboxResult with returncode, stdout, stderr, duration
        """
        if not self.cfg.enabled:
            # No Podman — run script locally via subprocess
            return self._run_local(script, args, timeout)

        run_id   = uuid.uuid4().hex[:8]
        timeout  = timeout or self.cfg.timeout_seconds
        cmd      = self._build_command(script, args, extra_mounts, env_overrides, run_id)

        log.debug(
            "sandbox_run_start",
            script=script,
            run_id=run_id,
            args_keys=list(args.keys()) if args else [],
        )

        start = time.monotonic()
        timed_out = False

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            returncode = proc.returncode
            stdout     = proc.stdout
            stderr     = proc.stderr

        except subprocess.TimeoutExpired as e:
            timed_out  = True
            returncode = -1
            stdout     = e.output.decode() if e.output else ""
            stderr     = e.stderr.decode() if e.stderr else ""
            # Kill the container
            self._kill_container(run_id)

        duration = time.monotonic() - start

        result = SandboxResult(
            returncode=returncode,
            stdout=stdout,
            stderr=stderr,
            duration_s=round(duration, 2),
            script=script,
            timed_out=timed_out,
        )

        if result.success:
            log.debug(
                "sandbox_run_success",
                script=script,
                run_id=run_id,
                duration_s=result.duration_s,
                stdout_len=len(stdout),
            )
        else:
            log.error(
                "sandbox_run_failed",
                script=script,
                run_id=run_id,
                returncode=returncode,
                timed_out=timed_out,
                stderr_snippet=stderr[-500:],
            )

        return result

    def run_inline(
        self,
        python_code: str,
        timeout: int | None = None,
    ) -> SandboxResult:
        """
        Run an inline Python code string.
        Useful for one-off checks during Conversion/Validation.

        Routes to Podman container when enabled, or direct subprocess when not.
        """
        tmp_script = Path(f"/tmp/sql_migration/inline_{uuid.uuid4().hex[:8]}.py")
        tmp_script.parent.mkdir(parents=True, exist_ok=True)
        tmp_script.write_text(python_code, encoding="utf-8")

        if not self.cfg.enabled:
            # Run locally via subprocess
            timeout = timeout or self.cfg.timeout_seconds
            start = time.monotonic()
            try:
                proc = subprocess.run(
                    [sys.executable, str(tmp_script)],
                    capture_output=True, text=True, timeout=timeout,
                )
                return SandboxResult(
                    returncode=proc.returncode, stdout=proc.stdout,
                    stderr=proc.stderr,
                    duration_s=round(time.monotonic() - start, 2),
                    script=str(tmp_script),
                )
            except subprocess.TimeoutExpired as e:
                return SandboxResult(
                    returncode=-1,
                    stdout=e.output.decode() if e.output else "",
                    stderr=e.stderr.decode() if e.stderr else "",
                    duration_s=round(time.monotonic() - start, 2),
                    script=str(tmp_script), timed_out=True,
                )

        # Podman path: mount the temp script into the container
        extra_mounts = [{
            "source":  str(tmp_script),
            "target":  "/tmp/inline_script.py",
            "options": "ro",
        }]

        return self.run(
            script="/tmp/inline_script.py",
            extra_mounts=extra_mounts,
            timeout=timeout,
        )

    # ------------------------------------------------------------------
    # Command builder
    # ------------------------------------------------------------------

    def _build_command(
        self,
        script: str,
        args: dict | None,
        extra_mounts: list[dict] | None,
        env_overrides: dict | None,
        run_id: str,
    ) -> list[str]:
        """Build the full podman run command."""
        cmd = [
            self.cfg.runtime, "run",
            "--rm",
            f"--name=sql-migration-sandbox-{run_id}",
            f"--cpus={self.cfg.cpu_limit}",
            f"--memory={self.cfg.memory_limit}",
            "--network=none",  # No outbound network from sandbox
        ]

        # Base mounts from config
        cmd += self._mount_flags(self.cfg.base_mounts)

        # Source SQL files (read-only)
        if self.source_dir and self.source_dir.exists():
            cmd += ["-v", f"{self.source_dir}:/source:ro"]

        # Extra mounts (per-call)
        if extra_mounts:
            cmd += self._mount_flags(extra_mounts)

        # Environment variables
        all_env = {**self.cfg.container_env}
        if env_overrides:
            all_env.update(env_overrides)
        for k, v in all_env.items():
            cmd += ["-e", f"{k}={v}"]

        # Image
        cmd.append(self.cfg.image)

        # Script + JSON args via stdin flag
        script_path = f"/scripts/{script}" if not script.startswith("/") else script
        cmd += ["python3", script_path]

        # v10-fix (Bug 2): Translate host paths to container mount points
        # before serializing args. Scripts receive /source/file.sql instead
        # of /data/sql_files/file.sql, matching the Podman bind mounts.
        if args:
            container_args = self._translate_args_for_container(args)
            cmd += ["--args", json.dumps(container_args)]

        return cmd

    # ------------------------------------------------------------------
    # Path translation (host → container)
    # ------------------------------------------------------------------

    def _translate_path(self, host_path: str) -> str:
        """
        Translate a single host path to its container mount equivalent.

        Mount mappings:
          {source_dir}/*           → /source/*
          {artifact_store_path}/*  → /artifacts/*

        Returns the original path unchanged if it doesn't match any mount.
        """
        if not host_path or not isinstance(host_path, str):
            return host_path

        # Resolve to absolute for reliable prefix matching
        try:
            abs_path = str(Path(host_path).resolve())
        except Exception:
            return host_path

        # Source dir: /data/sql_files/foo.sql → /source/foo.sql
        if self.source_dir:
            abs_source = str(self.source_dir.resolve())
            if abs_path.startswith(abs_source):
                relative = abs_path[len(abs_source):].lstrip("/")
                return f"/source/{relative}" if relative else "/source"

        # Artifact store: ./artifacts/run_001/... → /artifacts/run_001/...
        abs_artifact = str(self.artifact_store_path.resolve())
        if abs_path.startswith(abs_artifact):
            relative = abs_path[len(abs_artifact):].lstrip("/")
            return f"/artifacts/{relative}" if relative else "/artifacts"

        return host_path

    def _translate_args_for_container(self, args: dict) -> dict:
        """
        Recursively walk args dict and translate host file paths to
        container mount paths. Handles strings, lists, and nested dicts.

        Only translates strings that contain '/' (likely paths).
        _translate_path returns the original string unchanged if it
        doesn't match any known host mount prefix.
        """
        def _walk(obj):
            if isinstance(obj, str):
                if "/" in obj:
                    return self._translate_path(obj)
                return obj
            elif isinstance(obj, list):
                return [_walk(item) for item in obj]
            elif isinstance(obj, dict):
                return {k: _walk(v) for k, v in obj.items()}
            return obj

        return _walk(args)

    def _mount_flags(self, mounts: list) -> list[str]:
        flags = []
        for mount in mounts:
            if isinstance(mount, dict):
                src  = mount.get("source", "")
                tgt  = mount.get("target", "")
                opts = mount.get("options", "rw")
            else:
                # Pydantic model
                src  = mount.source
                tgt  = mount.target
                opts = mount.options

            # Resolve config variable placeholders
            if src:
                src = src.replace("${ARTIFACT_STORE_PATH}", str(self.artifact_store_path.resolve()))
                src = src.replace("${SANDBOX_SCRIPTS_PATH}", str(self.scripts_path.resolve()))
                if src.startswith("${") and src.endswith("}"):
                    log.warning("unresolved_mount_variable", source=src)
                    continue

            if src and tgt:
                flags += ["-v", f"{src}:{tgt}:{opts}"]
        return flags

    def _kill_container(self, run_id: str) -> None:
        try:
            subprocess.run(
                [self.cfg.runtime, "kill", f"sql-migration-sandbox-{run_id}"],
                capture_output=True,
                timeout=5,
            )
        except Exception:
            pass  # Best effort

    # ------------------------------------------------------------------
    # Local execution (no-Podman path)
    # ------------------------------------------------------------------

    def _run_local(
        self,
        script: str,
        args: dict | None,
        timeout: int | None = None,
    ) -> SandboxResult:
        """
        Execute a sandbox script locally via subprocess, without Podman.

        Used when:
          - sandbox.enabled=False (no Podman available or not desired)
          - dry_run mode where LLM calls are mocked but sandbox scripts
            must still run to produce real structural extraction, validation,
            and chunk computation results

        The script runs with the same args interface as the Podman path
        (--args JSON) so scripts don't need two code paths.

        Environment:
          - ARTIFACT_STORE_PATH set to match Podman's /artifacts mount
          - SOURCE_DIR set to match Podman's /source mount
          - PYTHONPATH includes sandbox_scripts/ directory
        """
        timeout = timeout or self.cfg.timeout_seconds

        # Resolve script path (same convention as Podman: relative to scripts dir)
        script_path = self.scripts_path / script
        if not script_path.exists():
            log.error("sandbox_local_script_not_found",
                      script=script, path=str(script_path))
            return SandboxResult(
                returncode=1,
                stdout="",
                stderr=f"Script not found: {script_path}",
                duration_s=0.0,
                script=script,
            )

        cmd = [sys.executable, str(script_path)]
        if args:
            cmd += ["--args", json.dumps(args)]

        # Set up environment to match Podman container paths
        env = dict(__import__("os").environ)
        env["ARTIFACT_STORE_PATH"] = str(self.artifact_store_path)
        if self.source_dir:
            env["SOURCE_DIR"] = str(self.source_dir)
        # Add sandbox_scripts to PYTHONPATH so scripts can import shared modules
        existing_pypath = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = f"{self.scripts_path}:{existing_pypath}"

        log.debug("sandbox_local_run_start",
                  script=script, path=str(script_path))

        start = time.monotonic()
        timed_out = False

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=env,
            )
            returncode = proc.returncode
            stdout     = proc.stdout
            stderr     = proc.stderr

        except subprocess.TimeoutExpired as e:
            timed_out  = True
            returncode = -1
            stdout     = e.output.decode() if e.output else ""
            stderr     = e.stderr.decode() if e.stderr else ""

        duration = time.monotonic() - start

        result = SandboxResult(
            returncode=returncode,
            stdout=stdout,
            stderr=stderr,
            duration_s=round(duration, 2),
            script=script,
            timed_out=timed_out,
        )

        if result.success:
            log.debug("sandbox_local_run_success",
                      script=script, duration_s=result.duration_s)
        else:
            log.error("sandbox_local_run_failed",
                      script=script, returncode=returncode,
                      timed_out=timed_out,
                      stderr_snippet=stderr[-500:])

        return result

    # ------------------------------------------------------------------
    # Mock mode (fully disabled — returns stub results)
    # ------------------------------------------------------------------

    def _mock_run(self, script: str, args: dict | None) -> SandboxResult:
        """
        Return a stub result when sandbox is fully disabled.
        WARNING: agents receive {"mock": True} and must handle gracefully.
        Prefer _run_local() over _mock_run() — local execution produces
        real results while mock produces stubs.
        """
        log.warning("sandbox_mock_run", script=script)
        return SandboxResult(
            returncode=0,
            stdout=json.dumps({"mock": True, "script": script, "args": args}),
            stderr="",
            duration_s=0.0,
            script=script,
        )