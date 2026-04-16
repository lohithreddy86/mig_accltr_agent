"""
sql_extractor.py
================
Extract SQL statements from parsed SSIS task metadata and write
individual .sql files for downstream pipeline processing.

Also builds the task_context dict with transformation metadata.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

from sql_migration.core.logger import get_logger

log = get_logger("parser.sql_extractor")


def extract_sql_and_context(
    parsed_package: dict,
    output_dir: str,
    file_prefix: str = "",
) -> tuple[list[str], dict[str, dict]]:
    """
    Process all tasks in a parsed package and extract SQL.

    Args:
        parsed_package: Output from dtsx_parser.parse_dtsx()
        output_dir: Directory to write extracted_sql/*.sql files
        file_prefix: Optional prefix for filenames (set for multi-package to avoid collisions)

    Returns:
        (sql_file_paths, task_context)
        - sql_file_paths: list of paths to written .sql files (one per conversion unit)
        - task_context: {effective_id: {...}} where effective_id matches the filename stem
    """
    pkg_name = parsed_package["package_name"]
    tasks = parsed_package["tasks"]
    connections = {c["dts_id"]: c for c in parsed_package.get("connection_managers", [])}
    # Also index by name for fallback
    conn_by_name = {c["name"]: c for c in parsed_package.get("connection_managers", [])}

    sql_dir = Path(output_dir)
    sql_dir.mkdir(parents=True, exist_ok=True)

    sql_paths: list[str] = []
    task_context: dict[str, dict] = {}

    for task in tasks:
        task_id = task["task_id"]
        effective_id = f"{file_prefix}__{task_id}" if file_prefix else task_id
        task_name = task["task_name"]
        task_type = task["task_type"]
        config = task.get("config", {})

        ctx: dict = {
            "task_type": task_type,
            "task_name": task_name,
            "source_connection": "",
            "sql_file": "",
            "transformation_chain": [],
            "column_mappings": {},
            "destination": None,
            "ssis_expressions": {},
            "non_sql": config.get("non_sql", False),
            "needs_manual_review": config.get("needs_manual_rewrite", False),
        }

        if task_type == "ExecuteSQLTask":
            paths = _extract_execute_sql_task(
                task, effective_id, pkg_name, config, connections, conn_by_name, sql_dir, ctx)
            sql_paths.extend(paths)

        elif task_type == "DataFlowTask":
            paths = _extract_data_flow_task(
                task, effective_id, pkg_name, config, connections, conn_by_name, sql_dir, ctx)
            sql_paths.extend(paths)

        elif task_type == "ExecutePackageTask":
            ctx["child_package"] = config.get("child_package", "")

        elif config.get("non_sql"):
            ctx["non_sql"] = True

        task_context[effective_id] = ctx

    log.info("sql_extraction_complete",
             package=pkg_name,
             sql_files=len(sql_paths),
             tasks_with_context=len(task_context))

    return sql_paths, task_context


# ---------------------------------------------------------------------------
# Execute SQL Task
# ---------------------------------------------------------------------------

def _extract_execute_sql_task(
    task: dict,
    effective_id: str,
    pkg_name: str,
    config: dict,
    connections: dict,
    conn_by_name: dict,
    sql_dir: Path,
    ctx: dict,
) -> list[str]:
    sql = config.get("sql_statement", "").strip()
    source_type = config.get("sql_source_type", 1)
    is_proc_call = config.get("is_stored_proc_call", False)
    conn_ref = config.get("connection", "")
    conn_info = connections.get(conn_ref) or conn_by_name.get(conn_ref, {})

    ctx["source_connection"] = conn_info.get("name", conn_ref)

    # Source type: 1=Direct, 2=File Connection, 3=Variable
    if source_type == 3:
        ctx["needs_manual_review"] = True
        ctx["variable_sql"] = sql
        log.warning("sql_from_variable",
                    task=task["task_name"],
                    expression=sql[:100])
        return []

    if source_type == 2:
        ctx["file_connection_ref"] = sql
        log.warning("sql_from_file_connection",
                    task=task["task_name"],
                    file_ref=sql[:100])
        return []

    if not sql:
        return []

    if is_proc_call:
        ctx["is_stored_proc_call"] = True
        ctx["proc_name"] = sql.strip()

    # Write .sql file — filename uses effective_id so stem matches task_context key
    file_name = f"{effective_id}.sql"
    file_path = sql_dir / file_name

    header = (
        f"-- SSIS Task: {task['task_name']}\n"
        f"-- Package: {pkg_name}\n"
        f"-- Task Type: ExecuteSQLTask\n"
        f"-- Connection: {conn_info.get('name', conn_ref)}"
        f" ({conn_info.get('provider', '')} → {conn_info.get('server', '')}/{conn_info.get('database', '')})\n"
    )
    if is_proc_call:
        header += f"-- Note: Stored procedure CALL — body not in package\n"

    file_path.write_text(header + "\n" + sql + "\n", encoding="utf-8")
    ctx["sql_file"] = str(file_path)

    return [str(file_path)]


# ---------------------------------------------------------------------------
# Data Flow Task
# ---------------------------------------------------------------------------

def _extract_data_flow_task(
    task: dict,
    effective_id: str,
    pkg_name: str,
    config: dict,
    connections: dict,
    conn_by_name: dict,
    sql_dir: Path,
    ctx: dict,
) -> list[str]:
    sources = config.get("sources", [])
    transforms = config.get("transformations", [])
    destinations = config.get("destinations", [])

    sql_paths: list[str] = []

    # Extract source queries — these are the conversion units
    for src in sources:
        sql_cmd = src.get("sql_command", "").strip()
        table_ref = src.get("open_rowset", "").strip()
        conn_ref = src.get("connection", "")
        conn_info = _resolve_conn(conn_ref, connections, conn_by_name)

        ctx["source_connection"] = conn_info.get("name", conn_ref)

        if sql_cmd:
            # OLE DB Source with SQL Command
            file_name = f"{effective_id}.sql"
            file_path = sql_dir / file_name
            header = (
                f"-- SSIS Task: {task['task_name']}\n"
                f"-- Package: {pkg_name}\n"
                f"-- Task Type: DataFlowTask / OLE DB Source (SQL Command)\n"
                f"-- Connection: {conn_info.get('name', conn_ref)}"
                f" ({conn_info.get('provider', '')} → {conn_info.get('server', '')}/{conn_info.get('database', '')})\n"
            )
            if transforms:
                transform_desc = ", ".join(
                    f"{t.get('transform_type', 'Unknown')}({t.get('name', '')})"
                    for t in transforms
                )
                header += f"-- Transformations: {transform_desc}\n"
            if destinations:
                dest_tables = ", ".join(d.get("table", "?") for d in destinations)
                header += f"-- Destination: {dest_tables}\n"

            file_path.write_text(header + "\n" + sql_cmd + "\n", encoding="utf-8")
            ctx["sql_file"] = str(file_path)
            sql_paths.append(str(file_path))

        elif table_ref:
            # OLE DB Source with Table/View mode — generate SELECT *
            file_name = f"{effective_id}.sql"
            file_path = sql_dir / file_name
            generated_sql = f"SELECT * FROM {table_ref}"
            header = (
                f"-- SSIS Task: {task['task_name']}\n"
                f"-- Package: {pkg_name}\n"
                f"-- Task Type: DataFlowTask / OLE DB Source (Table mode)\n"
                f"-- Connection: {conn_info.get('name', conn_ref)}\n"
                f"-- Note: Generated SELECT * from table/view reference\n"
            )
            file_path.write_text(header + "\n" + generated_sql + "\n", encoding="utf-8")
            ctx["sql_file"] = str(file_path)
            sql_paths.append(str(file_path))

    # Extract transformation metadata
    transformation_chain = []
    for t in transforms:
        t_type = t.get("transform_type", "Other")
        t_entry: dict = {
            "type": t_type,
            "name": t.get("name", ""),
        }

        if t_type == "Lookup":
            lookup_sql = t.get("sql_command", "").strip()
            t_entry["connection"] = _resolve_conn(
                t.get("connection", ""), connections, conn_by_name
            ).get("name", "")
            if lookup_sql:
                # Write lookup SQL for reference — NOT a conversion unit
                ref_name = f"{effective_id}__{_safe_fn(t.get('name', 'lookup'))}__ref.sql"
                ref_path = sql_dir / ref_name
                header = (
                    f"-- SSIS Lookup: {t.get('name', '')}\n"
                    f"-- Parent Task: {task['task_name']}\n"
                    f"-- Package: {pkg_name}\n"
                    f"-- Note: Reference only — not a conversion unit\n"
                )
                ref_path.write_text(header + "\n" + lookup_sql + "\n", encoding="utf-8")
                t_entry["sql_file"] = str(ref_path)
                t_entry["sql_command"] = lookup_sql
                # NOT appended to sql_paths — lookup is context, not a conversion unit
            t_entry["output_columns"] = [
                c["name"] for c in t.get("output_columns", [])
            ]

        elif t_type == "DerivedColumn":
            t_entry["expressions"] = t.get("expressions", {})
            ctx["ssis_expressions"].update(t.get("expressions", {}))

        elif t_type == "OleDbCommand":
            cmd_sql = t.get("sql_command", "").strip()
            if cmd_sql:
                t_entry["sql_command"] = cmd_sql
                t_entry["connection"] = _resolve_conn(
                    t.get("connection", ""), connections, conn_by_name
                ).get("name", "")

        transformation_chain.append(t_entry)

    ctx["transformation_chain"] = transformation_chain

    # Extract destination metadata
    if destinations:
        dest = destinations[0]
        conn_info = _resolve_conn(dest.get("connection", ""), connections, conn_by_name)
        column_mappings = {}
        for col in dest.get("input_columns", []):
            column_mappings[col["name"]] = col["name"]

        ctx["destination"] = {
            "table": dest.get("table", ""),
            "connection": conn_info.get("name", ""),
            "mode": "overwrite",
        }
        ctx["column_mappings"] = column_mappings

    return sql_paths


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_conn(ref: str, connections: dict, conn_by_name: dict) -> dict:
    return connections.get(ref) or conn_by_name.get(ref, {})


def _safe_fn(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_")