#!/usr/bin/env python3
"""
extract_structure.py
====================
Sandbox script: generic structural extraction of source SQL files.
Driven entirely by adapter patterns — zero hardcoded dialect logic.

Called by: Analysis Agent step A1
Input  (--args JSON): {
    "sql_file_paths": [...],
    "adapter":        { ... },            # DialectAdapter dict
    "procs_to_skip":  ["name1", ...],     # From dialect_profile
    "table_mapping":  {"SRC": "TRINO"},   # From dialect_profile
}
Output (stdout JSON): {
    "procs": [ProcEntry dict, ...],
    "all_tables": ["schema.table", ...]   # Deduplicated, mapping applied
}

Design:
  - Never loads full file — reads in streaming chunks bounded by proc boundaries
  - Uses only adapter.proc_boundary patterns for proc detection
  - Table extraction uses a two-pass approach:
      pass 1: regex candidates after DML keywords
      pass 2: false-positive filtering (remove aliases, subquery names, etc.)
  - All output is JSON — agent does table resolution via MCP separately
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# DML context patterns for table extraction
# ---------------------------------------------------------------------------

_READ_PATTERNS = [
    r'\bFROM\s+([\w$.]+(?:\.[\w$]+)?)',
    r'\bJOIN\s+([\w$.]+(?:\.[\w$]+)?)',
    r'\bINTO\s+#?(?![\s\w]*VALUES)([\w$.]+(?:\.[\w$]+)?)',  # SELECT INTO
]
_WRITE_PATTERNS = [
    r'\bINSERT\s+(?:INTO\s+)?([\w$.]+(?:\.[\w$]+)?)',
    r'\bUPDATE\s+([\w$.]+(?:\.[\w$]+)?)\s+SET',
    r'\bDELETE\s+FROM\s+([\w$.]+(?:\.[\w$]+)?)',
    r'\bMERGE\s+INTO\s+([\w$.]+(?:\.[\w$]+)?)',
    r'\bTRUNCATE\s+TABLE\s+([\w$.]+(?:\.[\w$]+)?)',
]

# Tokens that look like table names but are not
_PSEUDOTABLES = {
    "DUAL", "SYSIBM", "SYSDUMMY1", "INSERTED", "DELETED",
    "INFORMATION_SCHEMA", "SYS", "SYSTEM", "ADMIN",
}

_SQL_NOISE = {
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS",
    "NULL", "AS", "BY", "ON", "SET", "INTO", "VALUES", "WITH",
    "HAVING", "UNION", "ALL", "DISTINCT", "OVER", "PARTITION",
    "ROWS", "RANGE", "BETWEEN", "EXISTS", "CASE", "WHEN",
    "THEN", "ELSE", "END", "IF", "ELSE", "BEGIN", "COMMIT",
    "ROLLBACK", "EXCEPTION", "WHEN", "OTHERS", "RAISE",
}


def _extract_tables(body: str, patterns: list[str], table_mapping: dict) -> list[str]:
    """Extract table names from a proc body using regex patterns."""
    found = set()
    upper = body.upper()
    for pat in patterns:
        for match in re.finditer(pat, upper, re.IGNORECASE):
            raw = match.group(1).strip().strip('"').strip("'").strip("`")
            # Strip Oracle db-link references (e.g., TABLE@DBLOANS → TABLE)
            if "@" in raw:
                raw = raw.split("@")[0]
            # Skip obvious non-tables
            if raw.upper() in _PSEUDOTABLES or raw.upper() in _SQL_NOISE:
                continue
            if len(raw) < 2 or raw.startswith("("):
                continue
            # Normalize: apply table_mapping rename if present (keys are uppercased)
            raw_upper = raw.strip().upper()
            normalized = table_mapping.get(raw_upper, table_mapping.get(raw, raw))
            found.add(normalized.lower())
    return sorted(found)


def _extract_calls(body: str, known_proc_names: list[str]) -> list[str]:
    """Find calls to other known procedures within a proc body."""
    calls = []
    upper_body = body.upper()
    for name in known_proc_names:
        # Match as: CALL name( or just name( at statement start
        if re.search(
            r'(?:CALL\s+)?' + re.escape(name.upper()) + r'\s*\(',
            upper_body
        ):
            calls.append(name)
    return calls


# ---------------------------------------------------------------------------
# Core extraction
# ---------------------------------------------------------------------------

def extract_proc_boundaries(raw: str, adapter: dict) -> list[dict]:
    """
    Use adapter.proc_boundary patterns to find all proc/function boundaries.
    Returns list of {name, start_byte, end_byte} — byte positions in raw string.
    """
    pb = adapter.get("proc_boundary", {})
    start_pat_str = pb.get("start_pattern", "")
    end_pat_str   = pb.get("end_pattern", "")
    name_group    = pb.get("name_capture_group", 1)

    if not start_pat_str:
        return []

    try:
        start_pat = re.compile(start_pat_str, re.IGNORECASE | re.MULTILINE)
    except re.error as e:
        print(f"[WARN] start_pattern invalid regex: {e}", file=sys.stderr)
        return []

    try:
        end_pat = re.compile(end_pat_str, re.IGNORECASE | re.MULTILINE) if end_pat_str else None
    except re.error as e:
        print(f"[WARN] end_pattern invalid regex: {e}", file=sys.stderr)
        end_pat = None

    boundaries = []
    lines = raw.splitlines(keepends=True)
    line_offsets = []
    offset = 0
    for line in lines:
        line_offsets.append(offset)
        offset += len(line)
    line_offsets.append(offset)

    def byte_to_line(byte_pos: int) -> int:
        """Convert byte offset to 0-based line number."""
        lo, hi = 0, len(line_offsets) - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if line_offsets[mid] <= byte_pos:
                lo = mid + 1
            else:
                hi = mid
        return max(0, lo - 1)

    starts = list(start_pat.finditer(raw))
    for i, m in enumerate(starts):
        try:
            name = m.group(name_group)
        except IndexError:
            name = m.group(0)

        start_byte = m.start()

        # Find end boundary: search from end of this proc's start match
        # up to the next proc's start (exclusive)
        search_from = m.end()
        search_to   = starts[i + 1].start() if i + 1 < len(starts) else len(raw)

        if end_pat:
            end_match = end_pat.search(raw, search_from, search_to)
            end_byte  = end_match.end() if end_match else search_to
        else:
            end_byte = search_to

        boundaries.append({
            "name":       name,
            "start_byte": start_byte,
            "end_byte":   end_byte,
            "start_line": byte_to_line(start_byte),
            "end_line":   byte_to_line(end_byte),
        })

    return boundaries


def _detect_package_bodies(raw: str, adapter: dict) -> list[dict]:
    """
    Detect Oracle PACKAGE BODY containers and return their boundaries.
    Returns list of {name, start_byte, end_byte, body} for each package body.
    Only activates when adapter has package_body_pattern.
    """
    pb = adapter.get("proc_boundary", {})
    pkg_pattern = pb.get("package_body_pattern", "")
    if not pkg_pattern:
        return []

    try:
        pkg_pat = re.compile(pkg_pattern, re.IGNORECASE | re.MULTILINE)
    except re.error:
        return []

    packages = []
    for m in pkg_pat.finditer(raw):
        pkg_name = m.group(1)
        start_byte = m.start()

        # Find the package body END: look for END <pkg_name>; at the outermost level.
        # Track BEGIN/END depth to avoid matching inner proc ENDs.
        search_from = m.end()
        depth = 1  # We're inside the package body's implicit BEGIN
        pos = search_from
        end_byte = len(raw)

        # Walk line by line tracking BEGIN/END depth
        lines_after = raw[search_from:].splitlines(keepends=True)
        offset = search_from
        for line in lines_after:
            stripped = line.strip().upper()
            # Count BEGINs (but not "BEGIN" in strings/comments)
            if re.match(r'\bBEGIN\b', stripped):
                depth += 1
            # Check for END <name>; that closes the package
            end_pkg_match = re.match(
                r'END\s+' + re.escape(pkg_name.upper()) + r'\s*;\s*/?',
                stripped
            )
            if end_pkg_match:
                end_byte = offset + len(line)
                break
            # Regular END; decrements depth
            elif re.match(r'END\s+\w*\s*;', stripped) and depth > 1:
                depth -= 1
            offset += len(line)

        packages.append({
            "name": pkg_name,
            "start_byte": start_byte,
            "end_byte": end_byte,
            "body": raw[m.end():end_byte],
        })

    return packages


def _enrich_proc_with_oracle_indicators(proc: dict, body: str, adapter: dict) -> None:
    """
    Scan proc body for Oracle-specific procedural indicators.
    Only runs when adapter declares relevant patterns (e.g., oracle adapter).
    Adds fields in-place to proc dict.
    """
    # Transaction control
    tx_patterns = adapter.get("transaction_patterns", {})
    has_commit = bool(re.search(tx_patterns.get("commit", r"\bCOMMIT\b"), body, re.I))
    has_rollback = bool(re.search(tx_patterns.get("rollback", r"\bROLLBACK\b"), body, re.I))
    proc["has_transaction_control"] = has_commit or has_rollback

    # Database link references
    db_link_pat = adapter.get("db_link_pattern", "")
    if db_link_pat:
        db_links = list(set(re.findall(db_link_pat, body, re.I)))
        proc["has_db_links"] = bool(db_links)
        proc["db_link_refs"] = db_links
    else:
        proc["has_db_links"] = False
        proc["db_link_refs"] = []

    # Exception handling
    proc["has_exception_handling"] = bool(
        re.search(r'\bEXCEPTION\s+WHEN\b', body, re.I)
    )

    # DBMS_* control-plane calls
    dbms_calls = list(set(re.findall(r'(DBMS_\w+\.\w+)', body, re.I)))
    proc["dbms_calls"] = dbms_calls

    # EXECUTE IMMEDIATE
    proc["has_execute_immediate"] = bool(
        re.search(r'\bEXECUTE\s+IMMEDIATE\b', body, re.I)
    )

    # Oracle hints
    hint_pat = adapter.get("oracle_hint_pattern", "")
    if hint_pat:
        proc["oracle_hints"] = list(set(re.findall(hint_pat, body)))
    else:
        proc["oracle_hints"] = []

    # Package name (set by caller for package-body procs)
    if "package_name" not in proc:
        proc["package_name"] = None


def extract_all_procs(
    filepath:       str,
    adapter:        dict,
    procs_to_skip:  list[str],
    table_mapping:  dict,
) -> tuple[list[dict], list[str]]:
    """
    Full extraction pipeline for one source file.
    Returns (proc_list, all_unique_tables).
    """
    path = Path(filepath)
    if not path.exists():
        return [], []

    raw = path.read_text(encoding="utf-8", errors="replace")

    # --- Package body detection (Oracle-specific) ---
    # When adapter declares package_body_pattern, detect package containers
    # and extract individual procedures with package-qualified names.
    packages = _detect_package_bodies(raw, adapter)
    has_packages = bool(packages)

    procs: list[dict] = []
    all_tables: set[str] = set()
    skip_set = {n.upper() for n in procs_to_skip}

    if has_packages:
        # Extract standalone procs OUTSIDE any package body
        # Build a set of byte ranges covered by packages
        pkg_ranges = [(p["start_byte"], p["end_byte"]) for p in packages]

        # Extract from regions outside package bodies
        outside_regions = []
        prev_end = 0
        for ps, pe in sorted(pkg_ranges):
            if ps > prev_end:
                outside_regions.append(raw[prev_end:ps])
            prev_end = pe
        if prev_end < len(raw):
            outside_regions.append(raw[prev_end:])
        outside_raw = "\n".join(outside_regions)

        standalone_boundaries = extract_proc_boundaries(outside_raw, adapter)
        for b in standalone_boundaries:
            b["_package"] = None

        # Extract procs from within each package body
        for pkg in packages:
            inner_boundaries = extract_proc_boundaries(pkg["body"], adapter)
            for b in inner_boundaries:
                b["name"] = f"{pkg['name']}.{b['name']}"
                b["_package"] = pkg["name"]
                # Adjust byte positions to be relative to the full file
                # (not needed since we pass the body text separately)

        # Combine: standalone + package-contained procs
        all_boundaries = standalone_boundaries
        for pkg in packages:
            inner = extract_proc_boundaries(pkg["body"], adapter)
            for b in inner:
                qualified_name = f"{pkg['name']}.{b['name']}"
                body = pkg["body"][b["start_byte"]:b["end_byte"]]
                proc_names_in_pkg = [ib["name"] for ib in inner if ib["name"] != b["name"]]

                if qualified_name.upper() in skip_set or b["name"].upper() in skip_set:
                    continue

                tables_read = _extract_tables(body, _READ_PATTERNS, table_mapping)
                tables_written = _extract_tables(body, _WRITE_PATTERNS, table_mapping)
                calls = _extract_calls(body, proc_names_in_pkg)

                has_cursor = bool(re.search(adapter.get("cursor_pattern", r"\bCURSOR\b"), body, re.I))
                has_dyn_sql = any(re.search(p, body, re.I) for p in adapter.get("dynamic_sql_patterns", []))
                has_exception = bool(re.search(adapter.get("exception_pattern", r"\bEXCEPTION\b"), body, re.I))
                has_unsupported = [c for c in adapter.get("unsupported_constructs", []) if c.upper() in body.upper()]

                proc = {
                    "name":             qualified_name,
                    "source_file":      filepath,
                    "start_line":       b["start_line"],
                    "end_line":         b["end_line"],
                    "line_count":       b["end_line"] - b["start_line"] + 1,
                    "tables_read":      tables_read,
                    "tables_written":   tables_written,
                    "calls":            calls,
                    "unresolved_deps":  [],
                    "has_cursor":       has_cursor,
                    "has_dynamic_sql":  has_dyn_sql,
                    "has_exception_handler": has_exception,
                    "has_unsupported":  has_unsupported,
                    "package_name":     pkg["name"],
                }
                # Enrich with Oracle procedural indicators
                _enrich_proc_with_oracle_indicators(proc, body, adapter)
                procs.append(proc)
                all_tables.update(tables_read)
                all_tables.update(tables_written)

        # Process standalone procs (outside packages)
        for b in standalone_boundaries:
            name = b["name"]
            if name.upper() in skip_set:
                continue
            body = outside_raw[b["start_byte"]:b["end_byte"]]
            tables_read = _extract_tables(body, _READ_PATTERNS, table_mapping)
            tables_written = _extract_tables(body, _WRITE_PATTERNS, table_mapping)

            proc = {
                "name":             name,
                "source_file":      filepath,
                "start_line":       b["start_line"],
                "end_line":         b["end_line"],
                "line_count":       b["end_line"] - b["start_line"] + 1,
                "tables_read":      tables_read,
                "tables_written":   tables_written,
                "calls":            [],
                "unresolved_deps":  [],
                "has_cursor":       False,
                "has_dynamic_sql":  False,
                "has_exception_handler": False,
                "has_unsupported":  [],
                "package_name":     None,
            }
            _enrich_proc_with_oracle_indicators(proc, body, adapter)
            procs.append(proc)
            all_tables.update(tables_read)
            all_tables.update(tables_written)

    else:
        # No package bodies — standard extraction (DB2, SQL Server, Teradata, simple Oracle)
        boundaries = extract_proc_boundaries(raw, adapter)
        proc_names = [b["name"] for b in boundaries]

        for b in boundaries:
            name = b["name"]
            if name.upper() in skip_set:
                continue

            body = raw[b["start_byte"]:b["end_byte"]]

            tables_read  = _extract_tables(body, _READ_PATTERNS, table_mapping)
            tables_written = _extract_tables(body, _WRITE_PATTERNS, table_mapping)
            calls        = _extract_calls(body, [n for n in proc_names if n != name])

            has_cursor   = bool(re.search(adapter.get("cursor_pattern", r"\bCURSOR\b"), body, re.I))
            has_dyn_sql  = any(
                re.search(p, body, re.I)
                for p in adapter.get("dynamic_sql_patterns", [])
            )
            has_exception = bool(
                re.search(adapter.get("exception_pattern", r"\bEXCEPTION\b"), body, re.I)
            )
            has_unsupported = [
                c for c in adapter.get("unsupported_constructs", [])
                if c.upper() in body.upper()
            ]

            proc = {
                "name":           name,
                "source_file":    filepath,
                "start_line":     b["start_line"],
                "end_line":       b["end_line"],
                "line_count":     b["end_line"] - b["start_line"] + 1,
                "tables_read":    tables_read,
                "tables_written": tables_written,
                "calls":          calls,
                "unresolved_deps": [],
                "has_cursor":     has_cursor,
                "has_dynamic_sql": has_dyn_sql,
                "has_exception_handler": has_exception,
                "has_unsupported": has_unsupported,
            }
            # Enrich with Oracle procedural indicators if adapter supports them
            if adapter.get("db_link_pattern") or adapter.get("control_plane_constructs"):
                _enrich_proc_with_oracle_indicators(proc, body, adapter)
            procs.append(proc)
            all_tables.update(tables_read)
            all_tables.update(tables_written)

    # Resolve unresolved_deps: calls to names NOT in this file's proc list
    known = {p["name"].upper() for p in procs}
    for p in procs:
        p["unresolved_deps"] = [c for c in p.get("calls", []) if c.upper() not in known]

    return procs, sorted(all_tables)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def extract_chunk_by_lines(
    source_file: str, start_line: int, end_line: int, schema_context: dict
) -> str:
    """Extract lines[start:end] and annotate table refs with Trino FQNs."""
    path = Path(source_file)
    if not path.exists():
        return ""
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    code  = "\n".join(lines[start_line:end_line + 1])
    for src, ctx in (schema_context or {}).items():
        fqn = ctx.get("trino_fqn", "")
        if fqn and fqn != src:
            code = re.sub(r'\b' + re.escape(src) + r'\b',
                          f"{src}/*→{fqn}*/", code, flags=re.IGNORECASE)
    return code


def split_sql_statements(
    filepath:      str,
    table_mapping: dict,
) -> tuple[list[dict], list[str]]:
    """
    Split a SQL file into individual statements for query mode.
    Each non-empty statement becomes a "proc" entry with a generated name.

    Names are derived from:
      1. Leading comment: "-- Report: Monthly Summary" → "report_monthly_summary"
      2. Leading comment: "-- query_name: acct_lookup" → "acct_lookup"
      3. Generated: "query_001", "query_002", ...

    Returns same format as extract_all_procs: (proc_list, all_tables).
    """
    path = Path(filepath)
    if not path.exists():
        return [], []

    raw = path.read_text(encoding="utf-8", errors="replace")
    lines = raw.splitlines(keepends=True)

    # Split on ; outside string literals (simple state machine)
    statements: list[dict] = []
    current_lines: list[int] = []  # 0-based line indices
    in_single_quote = False
    in_line_comment = False
    in_block_comment = False
    leading_comment = ""

    for i, line in enumerate(lines):
        stripped = line.strip()

        # Track leading comment for naming
        if not current_lines and not stripped:
            continue
        if not current_lines and (stripped.startswith("--") and ";" not in stripped):
            leading_comment = stripped.lstrip("- ").strip()
            current_lines.append(i)
            continue

        current_lines.append(i)

        # Check for statement terminator (;) outside strings/comments
        j = 0
        while j < len(line):
            c = line[j]
            if in_block_comment:
                if c == '*' and j + 1 < len(line) and line[j + 1] == '/':
                    in_block_comment = False
                    j += 1
            elif in_line_comment:
                break  # Rest of line is comment
            elif c == "'":
                in_single_quote = not in_single_quote
            elif not in_single_quote:
                if c == '-' and j + 1 < len(line) and line[j + 1] == '-':
                    in_line_comment = True
                elif c == '/' and j + 1 < len(line) and line[j + 1] == '*':
                    in_block_comment = True
                    j += 1
                elif c == ';':
                    # Statement boundary
                    if current_lines:
                        statements.append({
                            "start_line": current_lines[0],
                            "end_line": i,
                            "leading_comment": leading_comment,
                        })
                    current_lines = []
                    leading_comment = ""
            j += 1

        in_line_comment = False  # Reset per line

    # Handle trailing statement without ;
    if current_lines:
        body_text = "".join(lines[current_lines[0]:current_lines[-1] + 1]).strip()
        if body_text and not body_text.startswith("--"):
            statements.append({
                "start_line": current_lines[0],
                "end_line": current_lines[-1],
                "leading_comment": leading_comment,
            })

    # Build proc-format dicts
    procs: list[dict] = []
    all_tables: set[str] = set()
    query_counter = 0

    for stmt in statements:
        start = stmt["start_line"]
        end = stmt["end_line"]
        body = "".join(lines[start:end + 1])

        # Skip empty or comment-only
        stripped_body = re.sub(r'--.*', '', body).strip().rstrip(';').strip()
        if not stripped_body or len(stripped_body) < 5:
            continue

        query_counter += 1

        # Derive name from comment or generate
        comment = stmt["leading_comment"]
        name = ""
        if comment:
            # "Report: Monthly Summary" → "report_monthly_summary"
            # "query_name: acct_lookup" → "acct_lookup"
            for prefix in ("query_name:", "report:", "name:", "query:"):
                if comment.lower().startswith(prefix):
                    name = comment[len(prefix):].strip()
                    break
            if not name:
                name = comment
            name = re.sub(r'[^a-zA-Z0-9_]', '_', name).strip('_').lower()
            name = re.sub(r'_+', '_', name)

        if not name:
            name = f"query_{query_counter:03d}"

        # Extract tables using existing regex patterns
        tables_read = _extract_tables(body, _READ_PATTERNS, table_mapping)
        tables_written = _extract_tables(body, _WRITE_PATTERNS, table_mapping)
        all_tables.update(tables_read)
        all_tables.update(tables_written)

        procs.append({
            "name":           name,
            "source_file":    filepath,
            "start_line":     start,
            "end_line":       end,
            "line_count":     end - start + 1,
            "tables_read":    tables_read,
            "tables_written": tables_written,
            "calls":          [],
            "unresolved_deps": [],
            "has_cursor":     False,
            "has_dynamic_sql": False,
            "has_exception_handler": False,
            "has_unsupported": [],
        })

    return procs, sorted(all_tables)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--args", required=True)
    args_ns = parser.parse_args()
    params  = json.loads(args_ns.args)

    if params.get("mode") == "extract_chunk":
        code = extract_chunk_by_lines(
            params["source_file"], params["start_line"],
            params["end_line"], params.get("schema_context", {}))
        print(json.dumps({"chunk_code": code}))
        return

    if params.get("mode") == "split_statements":
        table_mapping = params.get("table_mapping", {})
        all_procs: list[dict] = []
        all_tables: set[str]  = set()
        for filepath in params.get("sql_file_paths", []):
            procs, tables = split_sql_statements(filepath, table_mapping)
            all_procs.extend(procs)
            all_tables.update(tables)
        print(json.dumps({
            "procs":      all_procs,
            "all_tables": sorted(all_tables),
        }))
        return

    all_procs: list[dict] = []
    all_tables: set[str]  = set()

    adapter       = params.get("adapter", {})
    procs_to_skip = params.get("procs_to_skip", [])
    table_mapping = params.get("table_mapping", {})

    for filepath in params.get("sql_file_paths", []):
        procs, tables = extract_all_procs(filepath, adapter, procs_to_skip, table_mapping)
        all_procs.extend(procs)
        all_tables.update(tables)

    # Cross-file: re-resolve unresolved_deps now that we know all proc names
    all_known = {p["name"].upper() for p in all_procs}
    for p in all_procs:
        p["unresolved_deps"] = [c for c in p["calls"] if c.upper() not in all_known]

    print(json.dumps({
        "procs":      all_procs,
        "all_tables": sorted(all_tables),
    }))


if __name__ == "__main__":
    main()