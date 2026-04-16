"""
agent.py (Parser)
=================
SSIS package parser: Phase 0 of the pipeline.

Parses .dtsx / .ispac files into:
  - extracted_sql/*.sql   → fed to Analysis Agent as sql_file_paths
  - task_context.json     → per-task transformation metadata for Planning/Conversion
  - scheduling_graph.json → precedence constraints + conversion order for Orchestrator
  - connection_map.json   → SSIS connections → suggested Airflow connection IDs
  - package_manifest.json → package inventory

Runs BEFORE the Analysis Agent. Only invoked when source_type == "ssis_package".
All parsing is deterministic XML processing — no LLM calls.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

from pydantic import BaseModel, Field

from sql_migration.core.logger import get_logger

log = get_logger("parser")


# ---------------------------------------------------------------------------
# Output model
# ---------------------------------------------------------------------------

class ParserOutput(BaseModel):
    extracted_sql_paths: list[str] = Field(default_factory=list)
    task_context: dict = Field(default_factory=dict)
    scheduling_graph: dict = Field(default_factory=dict)
    connection_map: dict = Field(default_factory=dict)
    package_manifest: dict = Field(default_factory=dict)
    total_packages: int = 0
    total_tasks: int = 0
    total_sql_files: int = 0


# ---------------------------------------------------------------------------
# Parser Agent
# ---------------------------------------------------------------------------

class ParserAgent:
    """
    Phase 0 — Source Decomposer for SSIS packages.

    Steps:
      P0: Package discovery (unzip .ispac or use .dtsx directly)
      P1: SQL extraction (per task → write extracted_sql/*.sql)
      P2: Scheduling graph (precedence constraints → edges + conversion_order)
      P3: Write artifacts to store
    """

    def __init__(self, store, sandbox=None, pipeline_config=None):
        self.store = store
        self.sandbox = sandbox
        self._pipeline_config = pipeline_config

    def run(
        self,
        input_file_paths: list[str],
        job_manifest_path: str = "",
    ) -> ParserOutput:
        log.info("parser_start", files=len(input_file_paths))

        # ── P0: Package discovery ─────────────────────────────────────────
        dtsx_paths = self._p0_discover_packages(input_file_paths)
        log.info("p0_complete", dtsx_files=len(dtsx_paths))

        # ── P1: Parse all .dtsx files and extract SQL ─────────────────────
        all_sql_paths: list[str] = []
        all_task_context: dict[str, dict] = {}
        parsed_packages: list[dict] = []

        # Determine output directory for extracted SQL.
        # Write to the same parent directory as the input files so that
        # the Conversion Agent's sandbox (which mounts source_dir) can access them.
        if input_file_paths:
            sql_output_dir = str(Path(input_file_paths[0]).parent / "extracted_sql")
        else:
            run_dir = self.store.path("parser", "").parent
            sql_output_dir = str(run_dir / "extracted_sql")

        for dtsx_path in dtsx_paths:
            from sql_migration.agents.parser.dtsx_parser import parse_dtsx
            from sql_migration.agents.parser.sql_extractor import extract_sql_and_context

            parsed = parse_dtsx(dtsx_path)
            parsed_packages.append(parsed)

            # For multi-package: prefix IDs with pkg name to avoid collisions
            # sql_extractor uses this prefix for BOTH filenames and task_context keys
            pkg_prefix = _safe_id(parsed["package_name"]) if len(dtsx_paths) > 1 else ""

            sql_paths, task_ctx = extract_sql_and_context(
                parsed, sql_output_dir, file_prefix=pkg_prefix)
            all_sql_paths.extend(sql_paths)
            all_task_context.update(task_ctx)

        log.info("p1_complete",
                 sql_files=len(all_sql_paths),
                 tasks=len(all_task_context))

        # ── P2: Build scheduling graph ────────────────────────────────────
        scheduling_graph = self._p2_build_scheduling_graph(
            parsed_packages, job_manifest_path)
        log.info("p2_complete",
                 nodes=len(scheduling_graph.get("nodes", [])),
                 edges=len(scheduling_graph.get("edges", [])))

        # ── P3: Build remaining artifacts and write to store ──────────────
        connection_map = self._p3_build_connection_map(parsed_packages)
        package_manifest = self._p3_build_package_manifest(
            parsed_packages, all_sql_paths)

        # Write all artifacts to store
        self.store.write("parser", "task_context.json", all_task_context)
        self.store.write("parser", "scheduling_graph.json", scheduling_graph)
        self.store.write("parser", "connection_map.json", connection_map)
        self.store.write("parser", "package_manifest.json", package_manifest)

        output = ParserOutput(
            extracted_sql_paths=all_sql_paths,
            task_context=all_task_context,
            scheduling_graph=scheduling_graph,
            connection_map=connection_map,
            package_manifest=package_manifest,
            total_packages=len(parsed_packages),
            total_tasks=len(all_task_context),
            total_sql_files=len(all_sql_paths),
        )

        log.info("parser_complete",
                 packages=output.total_packages,
                 tasks=output.total_tasks,
                 sql_files=output.total_sql_files)

        return output

    # ─── P0: Package discovery ────────────────────────────────────────────

    def _p0_discover_packages(self, input_file_paths: list[str]) -> list[str]:
        dtsx_paths: list[str] = []

        for fpath in input_file_paths:
            p = Path(fpath)
            suffix = p.suffix.lower()

            if suffix == ".ispac":
                from sql_migration.agents.parser.ispac_extractor import extract_ispac
                run_dir = self.store.path("parser", "").parent
                extract_dir = str(run_dir / "ispac_extracted" / p.stem)
                result = extract_ispac(fpath, extract_dir)
                dtsx_paths.extend(result["dtsx_paths"])

            elif suffix == ".dtsx":
                dtsx_paths.append(fpath)
            else:
                log.debug("p0_skipping_non_ssis_file", file=fpath, suffix=suffix)

        if not dtsx_paths:
            raise ValueError(
                "No .dtsx or .ispac files found in input. "
                "Please upload SSIS package files."
            )

        return dtsx_paths

    # ─── P2: Scheduling graph ─────────────────────────────────────────────

    def _p2_build_scheduling_graph(
        self,
        parsed_packages: list[dict],
        job_manifest_path: str,
    ) -> dict:
        all_nodes: list[dict] = []
        all_edges: list[tuple[str, str]] = []
        all_task_ids: set[str] = set()

        for pkg in parsed_packages:
            pkg_prefix = _safe_id(pkg["package_name"])
            multi = len(parsed_packages) > 1

            for task in pkg["tasks"]:
                task_id = f"{pkg_prefix}__{task['task_id']}" if multi else task["task_id"]
                all_task_ids.add(task_id)
                all_nodes.append({
                    "id": task_id,
                    "task_name": task["task_name"],
                    "task_type": task["task_type"],
                    "package": pkg["package_name"],
                    "container": task.get("container"),
                    "non_sql": task["config"].get("non_sql", False),
                })

            for pc in pkg["precedence_constraints"]:
                from_id = f"{pkg_prefix}__{pc['from_id']}" if multi else pc["from_id"]
                to_id = f"{pkg_prefix}__{pc['to_id']}" if multi else pc["to_id"]
                all_edges.append((from_id, to_id))

            # Execute Package Task references create inter-package edges
            for task in pkg["tasks"]:
                if task["task_type"] == "ExecutePackageTask":
                    child_pkg = task["config"].get("child_package", "")
                    if child_pkg:
                        child_prefix = _safe_id(Path(child_pkg).stem)
                        # Find first task of child package
                        for other_pkg in parsed_packages:
                            if _safe_id(other_pkg["package_name"]) == child_prefix:
                                if other_pkg["tasks"]:
                                    child_first = other_pkg["tasks"][0]["task_id"]
                                    if multi:
                                        child_first = f"{child_prefix}__{child_first}"
                                    task_id = f"{pkg_prefix}__{task['task_id']}" if multi else task["task_id"]
                                    all_edges.append((task_id, child_first))

        # Parse job manifest for inter-package ordering
        if job_manifest_path:
            from sql_migration.agents.parser.job_manifest_parser import parse_job_manifest
            jobs = parse_job_manifest(job_manifest_path)
            for job in jobs:
                steps = job.get("steps", [])
                for i in range(len(steps) - 1):
                    from_pkg = _safe_id(Path(steps[i]).stem)
                    to_pkg = _safe_id(Path(steps[i + 1]).stem)
                    # Find last task of from_pkg and first task of to_pkg
                    last_task = self._find_boundary_task(parsed_packages, from_pkg, "last")
                    first_task = self._find_boundary_task(parsed_packages, to_pkg, "first")
                    if last_task and first_task:
                        all_edges.append((last_task, first_task))

        # Topological sort for conversion order
        conversion_order = self._topological_sort(all_task_ids, all_edges)

        return {
            "nodes": all_nodes,
            "edges": [list(e) for e in all_edges],
            "conversion_order": conversion_order,
        }

    def _find_boundary_task(
        self,
        parsed_packages: list[dict],
        pkg_id: str,
        which: str,
    ) -> str | None:
        multi = len(parsed_packages) > 1
        for pkg in parsed_packages:
            if _safe_id(pkg["package_name"]) == pkg_id and pkg["tasks"]:
                task = pkg["tasks"][0] if which == "first" else pkg["tasks"][-1]
                tid = task["task_id"]
                return f"{pkg_id}__{tid}" if multi else tid
        return None

    def _topological_sort(
        self,
        nodes: set[str],
        edges: list[tuple[str, str]],
    ) -> list[str]:
        from collections import defaultdict, deque

        in_degree: dict[str, int] = {n: 0 for n in nodes}
        adj: dict[str, list[str]] = defaultdict(list)

        for src, dst in edges:
            if src in nodes and dst in nodes:
                adj[src].append(dst)
                in_degree[dst] = in_degree.get(dst, 0) + 1

        queue = deque(n for n in nodes if in_degree.get(n, 0) == 0)
        order: list[str] = []

        while queue:
            node = queue.popleft()
            order.append(node)
            for neighbor in adj[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        # If cycle detected, append remaining nodes
        remaining = nodes - set(order)
        if remaining:
            log.warning("scheduling_graph_has_cycle",
                        remaining=sorted(remaining))
            order.extend(sorted(remaining))

        return order

    # ─── P3: Connection map + package manifest ────────────────────────────

    def _p3_build_connection_map(self, parsed_packages: list[dict]) -> dict:
        connections = []
        seen = set()

        for pkg in parsed_packages:
            for cm in pkg.get("connection_managers", []):
                name = cm.get("name", "")
                if name in seen:
                    continue
                seen.add(name)

                provider = cm.get("provider", "").lower()
                conn_type = "generic"
                if "oracle" in provider or "ora" in provider:
                    conn_type = "oracle"
                elif "sqlncli" in provider or "sqlserver" in provider or "msoledbsql" in provider:
                    conn_type = "mssql"
                elif "db2" in provider:
                    conn_type = "db2"
                elif "mysql" in provider:
                    conn_type = "mysql"
                elif "postgres" in provider:
                    conn_type = "postgres"

                suggested_id = re.sub(r"[^a-zA-Z0-9_]", "_", name).lower()

                connections.append({
                    "ssis_name": name,
                    "ssis_type": cm.get("type", ""),
                    "provider": cm.get("provider", ""),
                    "server": cm.get("server", ""),
                    "database": cm.get("database", ""),
                    "suggested_airflow_conn_id": f"{conn_type}_{suggested_id}",
                    "suggested_airflow_conn_type": conn_type,
                })

        return {"connections": connections}

    def _p3_build_package_manifest(
        self,
        parsed_packages: list[dict],
        all_sql_paths: list[str],
    ) -> dict:
        packages = []
        for pkg in parsed_packages:
            tasks_summary = []
            for task in pkg["tasks"]:
                tasks_summary.append({
                    "task_id": task["task_id"],
                    "task_name": task["task_name"],
                    "task_type": task["task_type"],
                    "non_sql": task["config"].get("non_sql", False),
                })

            packages.append({
                "package_name": pkg["package_name"],
                "package_file": pkg["package_file"],
                "tasks": tasks_summary,
                "total_tasks": len(tasks_summary),
                "connections": [cm["name"] for cm in pkg.get("connection_managers", [])],
                "variables": [v["name"] for v in pkg.get("variables", [])],
            })

        return {
            "packages": packages,
            "total_packages": len(packages),
            "total_sql_files": len(all_sql_paths),
        }


def _safe_id(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_").lower()