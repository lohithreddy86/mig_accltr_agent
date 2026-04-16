"""
job_manifest_parser.py
======================
Parse job manifest files (YAML or CSV) that define inter-package
ordering for SQL Agent jobs.

YAML format:
    jobs:
      - name: Daily_ETL
        schedule: "0 2 * * *"
        steps:
          - PackageA.dtsx
          - PackageB.dtsx

CSV format (from SQL Agent export):
    job_name, step_id, step_name, step_command, ...
"""
from __future__ import annotations

import csv
import io
import re
from pathlib import Path

from sql_migration.core.logger import get_logger

log = get_logger("parser.manifest")


def parse_job_manifest(manifest_path: str) -> list[dict]:
    """
    Parse a job manifest file and return structured job definitions.

    Returns:
        [
            {
                "job_name": "Daily_ETL",
                "schedule": "0 2 * * *",
                "retries": 3,
                "retry_delay_minutes": 5,
                "steps": ["PackageA.dtsx", "PackageB.dtsx"],
            }
        ]
    """
    path = Path(manifest_path)
    if not path.exists():
        log.warning("job_manifest_not_found", path=manifest_path)
        return []

    text = path.read_text(encoding="utf-8", errors="replace")
    suffix = path.suffix.lower()

    if suffix in (".yaml", ".yml"):
        return _parse_yaml(text, manifest_path)
    elif suffix == ".csv":
        return _parse_csv(text, manifest_path)
    else:
        # Try YAML first, fall back to CSV
        try:
            return _parse_yaml(text, manifest_path)
        except Exception:
            return _parse_csv(text, manifest_path)


def _parse_yaml(text: str, path: str) -> list[dict]:
    try:
        import yaml
    except ImportError:
        log.warning("yaml_not_available", path=path)
        return []

    data = yaml.safe_load(text)
    if not data or not isinstance(data, dict):
        return []

    raw_jobs = data.get("jobs", [])
    if not isinstance(raw_jobs, list):
        return []

    jobs = []
    for j in raw_jobs:
        if not isinstance(j, dict):
            continue
        jobs.append({
            "job_name": j.get("name", "unnamed_job"),
            "schedule": j.get("schedule", ""),
            "retries": j.get("retries", 0),
            "retry_delay_minutes": j.get("retry_delay_minutes", 5),
            "steps": [str(s) for s in j.get("steps", [])],
        })

    log.info("yaml_manifest_parsed", path=path, jobs=len(jobs))
    return jobs


def _parse_csv(text: str, path: str) -> list[dict]:
    reader = csv.DictReader(io.StringIO(text))

    # Normalize column names
    jobs_map: dict[str, dict] = {}
    for row in reader:
        # Case-insensitive column lookup
        row_lower = {k.strip().lower(): v.strip() for k, v in row.items() if k}

        job_name = row_lower.get("job_name", "")
        step_id = row_lower.get("step_id", "0")
        step_command = row_lower.get("step_command", row_lower.get("command", ""))

        if not job_name:
            continue

        if job_name not in jobs_map:
            jobs_map[job_name] = {
                "job_name": job_name,
                "schedule": "",
                "retries": 0,
                "retry_delay_minutes": 5,
                "steps_with_order": [],
            }

        # Extract package name from step_command (SSIS steps reference .dtsx)
        pkg = _extract_package_from_command(step_command)
        if pkg:
            try:
                order = int(step_id)
            except ValueError:
                order = len(jobs_map[job_name]["steps_with_order"])
            jobs_map[job_name]["steps_with_order"].append((order, pkg))

    jobs = []
    for j in jobs_map.values():
        sorted_steps = sorted(j["steps_with_order"], key=lambda x: x[0])
        jobs.append({
            "job_name": j["job_name"],
            "schedule": j["schedule"],
            "retries": j["retries"],
            "retry_delay_minutes": j["retry_delay_minutes"],
            "steps": [s[1] for s in sorted_steps],
        })

    log.info("csv_manifest_parsed", path=path, jobs=len(jobs))
    return jobs


def _extract_package_from_command(command: str) -> str:
    match = re.search(r'[\\/]?(\w+\.dtsx)', command, re.IGNORECASE)
    if match:
        return match.group(1)
    # If command is just a package name
    if command.lower().endswith(".dtsx"):
        return command.strip()
    return ""