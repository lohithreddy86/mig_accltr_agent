"""
dtsx_parser.py
==============
Parse a .dtsx file (SSIS package XML) into structured metadata.

Uses xml.etree.ElementTree (stdlib) — no lxml dependency.

DTSX XML namespaces:
  DTS:      www.microsoft.com/SqlServer/Dts          — executables, variables, connections
  SQLTask:  www.microsoft.com/sqlserver/dts/tasks/sqltask  — Execute SQL Task config
  Pipeline: www.microsoft.com/SqlServer/Dts/Pipeline       — Data Flow components
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from pathlib import Path

from sql_migration.core.logger import get_logger

log = get_logger("parser.dtsx")

# ---------------------------------------------------------------------------
# XML namespace constants
# ---------------------------------------------------------------------------

NS_DTS = "www.microsoft.com/SqlServer/Dts"
NS_SQLTASK = "www.microsoft.com/sqlserver/dts/tasks/sqltask"
NS_PIPELINE = "www.microsoft.com/SqlServer/Dts/Pipeline"
NS_WRAPPER = "www.microsoft.com/SqlServer/Dts/Pipeline/Wrapper"

NS = {
    "DTS": NS_DTS,
    "SQLTask": NS_SQLTASK,
    "pipeline": NS_PIPELINE,
    "wrapper": NS_WRAPPER,
}


def _dts(tag: str) -> str:
    return f"{{{NS_DTS}}}{tag}"


def _sqltask(tag: str) -> str:
    return f"{{{NS_SQLTASK}}}{tag}"


def _pipeline(tag: str) -> str:
    return f"{{{NS_PIPELINE}}}{tag}"


def _attr(elem: ET.Element, attr: str, ns: str = NS_DTS) -> str:
    return elem.get(f"{{{ns}}}{attr}", "")


def _safe_id(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_").lower()


# ---------------------------------------------------------------------------
# Known executable types
# ---------------------------------------------------------------------------

EXEC_TYPE_MAP = {
    "Microsoft.ExecuteSQLTask": "ExecuteSQLTask",
    "Microsoft.Pipeline": "DataFlowTask",
    "STOCK:SEQUENCE": "SequenceContainer",
    "STOCK:FORLOOP": "ForLoopContainer",
    "STOCK:FOREACHLOOP": "ForEachLoopContainer",
    "Microsoft.SendMailTask": "SendMailTask",
    "Microsoft.FileSystemTask": "FileSystemTask",
    "Microsoft.FtpTask": "FtpTask",
    "Microsoft.ExecuteProcess": "ExecuteProcessTask",
    "Microsoft.ScriptTask": "ScriptTask",
    "Microsoft.ExecutePackageTask": "ExecutePackageTask",
}


def _classify_executable(exec_type_raw: str) -> str:
    for key, val in EXEC_TYPE_MAP.items():
        if key.lower() in exec_type_raw.lower():
            return val
    if "sqltask" in exec_type_raw.lower():
        return "ExecuteSQLTask"
    if "pipeline" in exec_type_raw.lower():
        return "DataFlowTask"
    if "script" in exec_type_raw.lower():
        return "ScriptTask"
    return "Unknown"


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------

def parse_dtsx(dtsx_path: str) -> dict:
    """
    Parse a .dtsx file into structured metadata.

    Returns:
        {
            "package_name": str,
            "package_file": str,
            "tasks": [{task_id, task_name, task_type, config}],
            "precedence_constraints": [{from_id, to_id, constraint}],
            "connection_managers": [{name, type, provider, connection_string}],
            "variables": [{name, type, value, scope}],
            "event_handlers": [{event, scope, tasks}],
        }
    """
    path = Path(dtsx_path)
    if not path.exists():
        raise FileNotFoundError(f"DTSX file not found: {dtsx_path}")

    tree = ET.parse(str(path))
    root = tree.getroot()

    package_name = _attr(root, "ObjectName") or path.stem

    tasks = _extract_tasks(root)
    constraints = _extract_precedence_constraints(root, tasks)
    connections = _extract_connection_managers(root)
    variables = _extract_variables(root)
    event_handlers = _extract_event_handlers(root)

    log.info("dtsx_parsed",
             package=package_name,
             tasks=len(tasks),
             constraints=len(constraints),
             connections=len(connections),
             variables=len(variables))

    return {
        "package_name": package_name,
        "package_file": path.name,
        "tasks": tasks,
        "precedence_constraints": constraints,
        "connection_managers": connections,
        "variables": variables,
        "event_handlers": event_handlers,
    }


# ---------------------------------------------------------------------------
# Task extraction
# ---------------------------------------------------------------------------

def _extract_tasks(root: ET.Element) -> list[dict]:
    tasks = []
    # Tasks are nested under DTS:Executables/DTS:Executable
    executables_container = root.find(_dts("Executables"))
    if executables_container is None:
        return tasks

    for exe in executables_container.findall(_dts("Executable")):
        task = _parse_single_task(exe)
        if task:
            tasks.append(task)
            # Recurse into containers (SequenceContainer, ForLoop, etc.)
            nested = _extract_tasks(exe)
            for nt in nested:
                nt["container"] = task["task_id"]
            tasks.extend(nested)

    return tasks


def _parse_single_task(exe: ET.Element) -> dict | None:
    task_name = _attr(exe, "ObjectName")
    dts_id = _attr(exe, "DTSID")
    exec_type_raw = _attr(exe, "ExecutableType")

    if not task_name:
        return None

    task_type = _classify_executable(exec_type_raw)
    task_id = _safe_id(task_name)

    config: dict = {
        "exec_type_raw": exec_type_raw,
    }

    # Extract task-specific configuration
    obj_data = exe.find(_dts("ObjectData"))

    if task_type == "ExecuteSQLTask" and obj_data is not None:
        config.update(_parse_execute_sql_task(obj_data))

    elif task_type == "DataFlowTask" and obj_data is not None:
        config.update(_parse_data_flow_task(obj_data))

    elif task_type == "ExecutePackageTask" and obj_data is not None:
        config.update(_parse_execute_package_task(obj_data, exe))

    elif task_type == "ScriptTask":
        config["non_sql"] = True
        config["needs_manual_rewrite"] = True

    elif task_type in ("SendMailTask", "FileSystemTask", "FtpTask",
                       "ExecuteProcessTask"):
        config["non_sql"] = True

    return {
        "task_id": task_id,
        "task_name": task_name,
        "task_type": task_type,
        "dts_id": dts_id,
        "container": None,
        "config": config,
    }


# ---------------------------------------------------------------------------
# Execute SQL Task parsing
# ---------------------------------------------------------------------------

def _parse_execute_sql_task(obj_data: ET.Element) -> dict:
    config: dict = {}

    sql_data = obj_data.find(_sqltask("SqlTaskData"))
    if sql_data is None:
        # Try without namespace (some SSIS versions)
        for child in obj_data:
            if "SqlTaskData" in child.tag:
                sql_data = child
                break

    if sql_data is None:
        return config

    sql_source = sql_data.get(
        f"{{{NS_SQLTASK}}}SqlStatementSource",
        sql_data.get("SqlStatementSource", ""),
    )
    config["sql_statement"] = sql_source

    sql_source_type = sql_data.get(
        f"{{{NS_SQLTASK}}}SqlSourceType",
        sql_data.get("SqlSourceType", "1"),
    )
    # 1=Direct input, 2=File connection, 3=Variable
    config["sql_source_type"] = int(sql_source_type) if sql_source_type.isdigit() else 1

    is_stored_proc = sql_data.get(
        f"{{{NS_SQLTASK}}}IsQueryStoredProcedure",
        sql_data.get("IsQueryStoredProcedure", "False"),
    )
    config["is_stored_proc_call"] = is_stored_proc.lower() == "true"

    result_set = sql_data.get(
        f"{{{NS_SQLTASK}}}ResultSetType",
        sql_data.get("ResultSetType", "1"),
    )
    config["result_set_type"] = int(result_set) if result_set.isdigit() else 1

    connection = sql_data.get(
        f"{{{NS_SQLTASK}}}Connection",
        sql_data.get("Connection", ""),
    )
    config["connection"] = connection

    return config


# ---------------------------------------------------------------------------
# Data Flow Task parsing
# ---------------------------------------------------------------------------

def _parse_data_flow_task(obj_data: ET.Element) -> dict:
    config: dict = {
        "sources": [],
        "transformations": [],
        "destinations": [],
    }

    # Data Flow components are under pipeline:components/pipeline:component
    pipeline_elem = None
    for child in obj_data:
        if "pipeline" in child.tag.lower() or "Pipeline" in child.tag:
            pipeline_elem = child
            break

    if pipeline_elem is None:
        return config

    # Find components container
    components = None
    for child in pipeline_elem:
        if "component" in child.tag.lower():
            # Could be "components" (container) or direct "component"
            if child.tag.endswith("components") or child.tag.endswith("Components"):
                components = child
                break

    if components is None:
        # Try direct iteration — some versions nest differently
        components = pipeline_elem

    for comp in components:
        if not ("component" in comp.tag.lower() or "Component" in comp.tag):
            continue
        comp_data = _parse_pipeline_component(comp)
        if comp_data:
            cat = comp_data.pop("_category", "transformations")
            config[cat].append(comp_data)

    return config


def _parse_pipeline_component(comp: ET.Element) -> dict | None:
    name = comp.get("name", comp.get("Name", ""))
    class_id = comp.get("componentClassID", comp.get("componentClassId", ""))
    desc = comp.get("description", comp.get("Description", ""))

    if not name:
        return None

    result: dict = {"name": name, "class_id": class_id, "description": desc}

    # Read properties
    props = {}
    props_container = None
    for child in comp:
        if "properties" in child.tag.lower():
            props_container = child
            break

    if props_container is not None:
        for prop in props_container:
            prop_name = prop.get("name", prop.get("Name", ""))
            prop_value = prop.text or ""
            if prop_name:
                props[prop_name] = prop_value

    result["properties"] = props

    # Read column mappings from input/output columns
    result["input_columns"] = _extract_column_list(comp, "input")
    result["output_columns"] = _extract_column_list(comp, "output")

    # Classify component
    class_lower = class_id.lower()
    desc_lower = desc.lower()

    if "source" in desc_lower or "src" in name.lower():
        result["_category"] = "sources"
        # Extract SQL command / table name
        result["access_mode"] = props.get("AccessMode", "")
        result["sql_command"] = props.get("SqlCommand", "")
        result["open_rowset"] = props.get("OpenRowset", "")
        result["connection"] = _find_connection_ref(comp)

    elif "destination" in desc_lower or "dst" in name.lower():
        result["_category"] = "destinations"
        result["table"] = props.get("OpenRowset", "")
        result["access_mode"] = props.get("AccessMode", "")
        result["sql_command"] = props.get("SqlCommand", "")
        result["connection"] = _find_connection_ref(comp)

    else:
        result["_category"] = "transformations"
        # Classify transformation type
        if "lookup" in desc_lower or "lookup" in class_lower:
            result["transform_type"] = "Lookup"
            result["sql_command"] = props.get("SqlCommand", "")
            result["connection"] = _find_connection_ref(comp)
        elif "derived" in desc_lower or "derivedcolumn" in class_lower:
            result["transform_type"] = "DerivedColumn"
            result["expressions"] = _extract_derived_expressions(comp)
        elif "conditional" in desc_lower or "conditionalsplit" in class_lower:
            result["transform_type"] = "ConditionalSplit"
        elif "aggregate" in desc_lower:
            result["transform_type"] = "Aggregate"
        elif "sort" in desc_lower:
            result["transform_type"] = "Sort"
        elif "merge" in desc_lower:
            result["transform_type"] = "MergeJoin"
        elif "union" in desc_lower:
            result["transform_type"] = "UnionAll"
        elif "oledbcommand" in class_lower or "ole db command" in desc_lower:
            result["transform_type"] = "OleDbCommand"
            result["sql_command"] = props.get("SqlCommand", "")
            result["connection"] = _find_connection_ref(comp)
        else:
            result["transform_type"] = "Other"

    return result


def _find_connection_ref(comp: ET.Element) -> str:
    for child in comp:
        if "connection" in child.tag.lower():
            # Could be connections container or direct connection
            for conn in child:
                conn_mgr = conn.get("connectionManagerID",
                                    conn.get("connectionManagerId", ""))
                if conn_mgr:
                    return conn_mgr
            # Direct attribute
            conn_mgr = child.get("connectionManagerID",
                                 child.get("connectionManagerId", ""))
            if conn_mgr:
                return conn_mgr
    return ""


def _extract_column_list(comp: ET.Element, direction: str) -> list[dict]:
    columns = []
    for child in comp:
        tag_lower = child.tag.lower()
        if direction in tag_lower and ("column" in tag_lower or "columns" in tag_lower):
            for col_container in child:
                for col in col_container:
                    col_name = col.get("name", col.get("Name", ""))
                    if col_name:
                        columns.append({
                            "name": col_name,
                            "data_type": col.get("dataType", col.get("DataType", "")),
                            "length": col.get("length", col.get("Length", "")),
                        })
            break
    return columns


def _extract_derived_expressions(comp: ET.Element) -> dict[str, str]:
    expressions = {}
    for child in comp:
        if "output" in child.tag.lower():
            for out_cols in child:
                for col in out_cols:
                    col_name = col.get("name", col.get("Name", ""))
                    # Derived column expressions are in properties
                    for prop_container in col:
                        if "properties" in prop_container.tag.lower():
                            for prop in prop_container:
                                prop_name = prop.get("name", prop.get("Name", ""))
                                if prop_name == "Expression" and prop.text:
                                    expressions[col_name] = prop.text
    return expressions


# ---------------------------------------------------------------------------
# Execute Package Task parsing
# ---------------------------------------------------------------------------

def _parse_execute_package_task(obj_data: ET.Element, exe: ET.Element) -> dict:
    config: dict = {"child_package": ""}

    for child in obj_data:
        # Look for the package reference
        for prop in child:
            tag = prop.tag.lower() if hasattr(prop, 'tag') else ""
            if "packagename" in tag or "PackageName" in (prop.get("name", "") + prop.get("Name", "")):
                config["child_package"] = prop.text or prop.get("Value", "")
            name_attr = prop.get("name", prop.get("Name", ""))
            if name_attr and "PackageName" in name_attr:
                config["child_package"] = prop.text or ""

    # Also check connection-based package reference
    conn_managers = exe.find(_dts("ConnectionManagers"))
    if conn_managers is not None:
        for cm in conn_managers.findall(_dts("ConnectionManager")):
            conn_str = ""
            obj = cm.find(_dts("ObjectData"))
            if obj is not None:
                for inner in obj:
                    conn_str = _attr(inner, "ConnectionString") or inner.get("ConnectionString", "")
            if conn_str and conn_str.endswith(".dtsx"):
                config["child_package"] = conn_str

    return config


# ---------------------------------------------------------------------------
# Precedence Constraints
# ---------------------------------------------------------------------------

CONSTRAINT_VALUES = {
    "0": "success",
    "1": "failure",
    "2": "completion",
}


def _extract_precedence_constraints(
    root: ET.Element,
    tasks: list[dict],
) -> list[dict]:
    constraints = []

    # Build DTSID → task_id lookup
    dts_to_id: dict[str, str] = {}
    name_to_id: dict[str, str] = {}
    for t in tasks:
        if t.get("dts_id"):
            dts_to_id[t["dts_id"]] = t["task_id"]
        name_to_id[t["task_name"]] = t["task_id"]

    def _resolve(ref: str) -> str:
        if ref in dts_to_id:
            return dts_to_id[ref]
        if ref in name_to_id:
            return name_to_id[ref]
        return _safe_id(ref)

    # Constraints can be at package level or inside containers
    for pc in root.iter(_dts("PrecedenceConstraint")):
        from_ref = _attr(pc, "From")
        to_ref = _attr(pc, "To")
        value = _attr(pc, "Value") or "0"
        expression = _attr(pc, "Expression")
        eval_op = _attr(pc, "EvalOp")

        if from_ref and to_ref:
            constraints.append({
                "from_id": _resolve(from_ref),
                "to_id": _resolve(to_ref),
                "constraint": CONSTRAINT_VALUES.get(value, "success"),
                "expression": expression,
                "eval_op": eval_op,
            })

    return constraints


# ---------------------------------------------------------------------------
# Connection Managers
# ---------------------------------------------------------------------------

def _extract_connection_managers(root: ET.Element) -> list[dict]:
    connections = []

    cm_container = root.find(_dts("ConnectionManagers"))
    if cm_container is None:
        return connections

    for cm in cm_container.findall(_dts("ConnectionManager")):
        name = _attr(cm, "ObjectName")
        dts_id = _attr(cm, "DTSID")
        creation_name = _attr(cm, "CreationName")

        conn_string = ""
        obj_data = cm.find(_dts("ObjectData"))
        if obj_data is not None:
            for inner in obj_data:
                conn_string = (
                    _attr(inner, "ConnectionString")
                    or inner.get("ConnectionString", "")
                )
                if conn_string:
                    break

        provider = ""
        server = ""
        database = ""
        if conn_string:
            provider = _extract_from_connstr(conn_string, "Provider")
            server = _extract_from_connstr(conn_string, "Data Source")
            database = _extract_from_connstr(conn_string, "Initial Catalog")
            if not server:
                server = _extract_from_connstr(conn_string, "Server")

        connections.append({
            "name": name,
            "dts_id": dts_id,
            "type": creation_name,
            "provider": provider,
            "server": server,
            "database": database,
            "connection_string": conn_string,
        })

    return connections


def _extract_from_connstr(conn_str: str, key: str) -> str:
    pattern = re.compile(rf"{re.escape(key)}\s*=\s*([^;]+)", re.IGNORECASE)
    match = pattern.search(conn_str)
    return match.group(1).strip() if match else ""


# ---------------------------------------------------------------------------
# Variables
# ---------------------------------------------------------------------------

def _extract_variables(root: ET.Element) -> list[dict]:
    variables = []
    for var in root.iter(_dts("Variable")):
        name = _attr(var, "ObjectName")
        ns = _attr(var, "Namespace") or "User"
        expression = _attr(var, "Expression")

        # Value is in a DTS:VariableValue child
        value = ""
        val_type = ""
        val_elem = var.find(_dts("VariableValue"))
        if val_elem is not None:
            value = val_elem.text or ""
            val_type = _attr(val_elem, "DataType")

        if name:
            variables.append({
                "name": f"{ns}::{name}",
                "type": val_type,
                "value": value,
                "expression": expression,
                "namespace": ns,
            })
    return variables


# ---------------------------------------------------------------------------
# Event Handlers
# ---------------------------------------------------------------------------

def _extract_event_handlers(root: ET.Element) -> list[dict]:
    handlers = []
    eh_container = root.find(_dts("EventHandlers"))
    if eh_container is None:
        return handlers

    for eh in eh_container.findall(_dts("EventHandler")):
        event_name = _attr(eh, "EventName")
        if event_name:
            handlers.append({
                "event": event_name,
                "scope": "package",
            })
    return handlers