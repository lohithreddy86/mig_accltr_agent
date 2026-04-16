#!/usr/bin/env python3
"""
parse_and_scan.py
=================
Sandbox script: intelligent source file parsing for the LLM-driven Analysis Agent.

Three modes:
  parse_file    — Parse proc/function boundaries using sqlglot AST + regex fallback.
                  Returns structured proc list the LLM can validate and correct.
  find_all_calls — Scan for ALL function-call-like patterns (name followed by parens).
                   Returns raw list — the LLM decides which are real calls vs keywords.
  search_code   — Grep for a pattern across one or all source files.
                  The LLM uses this to investigate: "where is calc_risk_score defined?"

Called by: ToolExecutor for Analysis Agent tool calls
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# SQL keywords to exclude from call scanning
# ---------------------------------------------------------------------------

_SQL_KEYWORDS = {
    "SELECT", "FROM", "WHERE", "JOIN", "INNER", "LEFT", "RIGHT", "OUTER",
    "ON", "AND", "OR", "NOT", "IN", "IS", "NULL", "AS", "BY", "ORDER",
    "GROUP", "HAVING", "UNION", "ALL", "DISTINCT", "CASE", "WHEN", "THEN",
    "ELSE", "END", "WITH", "OVER", "PARTITION", "ROWS", "RANGE", "BETWEEN",
    "EXISTS", "LIKE", "LIMIT", "OFFSET", "INTO", "VALUES", "SET",
    "CREATE", "ALTER", "DROP", "TABLE", "VIEW", "INDEX", "SCHEMA",
    "INSERT", "UPDATE", "DELETE", "MERGE", "BEGIN", "COMMIT", "ROLLBACK",
    "IF", "ELSE", "LOOP", "FOR", "WHILE", "RETURN", "DECLARE", "CURSOR",
    "OPEN", "FETCH", "CLOSE", "PROCEDURE", "FUNCTION", "TRIGGER", "PACKAGE",
    "BODY", "TYPE", "RECORD", "EXCEPTION", "RAISE", "REPLACE", "GRANT",
    "CAST", "CONVERT", "TRIM", "UPPER", "LOWER", "LENGTH",
    "COUNT", "SUM", "AVG", "MIN", "MAX", "COALESCE", "NULLIF",
    "ROW_NUMBER", "RANK", "DENSE_RANK", "LAG", "LEAD", "FIRST_VALUE",
    "SUBSTR", "SUBSTRING", "INSTR", "REPLACE", "TRANSLATE",
    "TO_CHAR", "TO_DATE", "TO_NUMBER", "TO_TIMESTAMP",
    "SYSDATE", "SYSTIMESTAMP", "CURRENT_DATE", "CURRENT_TIMESTAMP",
}

# Common DML table-extraction patterns
_READ_PATTERNS = [
    r'\bFROM\s+([\w$.]+(?:\.[\w$]+)?)',
    r'\bJOIN\s+([\w$.]+(?:\.[\w$]+)?)',
]
_WRITE_PATTERNS = [
    r'\bINSERT\s+(?:INTO\s+)?([\w$.]+(?:\.[\w$]+)?)',
    r'\bUPDATE\s+([\w$.]+(?:\.[\w$]+)?)\s+SET',
    r'\bDELETE\s+FROM\s+([\w$.]+(?:\.[\w$]+)?)',
    r'\bMERGE\s+INTO\s+([\w$.]+(?:\.[\w$]+)?)',
    r'\bTRUNCATE\s+TABLE\s+([\w$.]+(?:\.[\w$]+)?)',
]

_PSEUDOTABLES = {
    "DUAL", "SYSIBM", "SYSDUMMY1", "INSERTED", "DELETED",
    "INFORMATION_SCHEMA", "SYS", "SYSTEM",
}


# ---------------------------------------------------------------------------
# Mode 1: parse_file — structured proc boundary detection
# ---------------------------------------------------------------------------

def parse_file(file_path: str, adapter: dict | None = None) -> dict:
    """
    Parse a SQL file for proc/function boundaries.
    
    Strategy:
      1. Try sqlglot AST parsing (best accuracy for known dialects)
      2. Fall back to multi-pattern regex (handles packages, anonymous blocks)
      3. LLM sees the result and can correct boundaries via read_lines
    
    Returns:
        {
            "procs": [{name, start_line, end_line, line_count, params, type}],
            "packages": [{name, start_line, end_line, members: [proc_names]}],
            "anonymous_blocks": [{start_line, end_line, line_count}],
            "total_lines": int,
            "parse_method": "sqlglot" | "regex" | "hybrid",
            "parse_warnings": [str]
        }
    """
    path = Path(file_path)
    if not path.exists():
        return {"error": f"File not found: {file_path}", "procs": []}
    
    raw = path.read_text(encoding="utf-8", errors="replace")
    lines = raw.splitlines()
    total_lines = len(lines)
    
    warnings = []
    procs = []
    packages = []
    anonymous_blocks = []
    parse_method = "regex"
    
    # ── Try sqlglot first (best for known dialects) ────────────────────────
    sqlglot_procs = _try_sqlglot_parse(raw, warnings)
    if sqlglot_procs:
        procs = sqlglot_procs
        parse_method = "sqlglot"
    
    # ── Regex fallback / supplement ────────────────────────────────────────
    # Always run regex to catch package bodies and things sqlglot misses
    regex_procs, regex_packages, regex_anon = _regex_parse(raw, lines, adapter)
    
    if not procs:
        procs = regex_procs
        parse_method = "regex"
    elif regex_procs:
        # Hybrid: sqlglot found some, regex found others
        # Merge — add regex-only procs that sqlglot missed
        sqlglot_names = {p["name"].upper() for p in procs}
        for rp in regex_procs:
            if rp["name"].upper() not in sqlglot_names:
                procs.append(rp)
                warnings.append(f"Proc '{rp['name']}' found by regex but not sqlglot")
        parse_method = "hybrid"
    
    packages = regex_packages
    anonymous_blocks = regex_anon
    
    # ── Extract tables and flags for each proc ─────────────────────────────
    for proc in procs:
        body = "\n".join(lines[proc["start_line"]:proc["end_line"] + 1])
        proc["tables_read"] = _extract_tables(body, _READ_PATTERNS)
        proc["tables_written"] = _extract_tables(body, _WRITE_PATTERNS)
        proc["has_cursor"] = bool(re.search(r'\bCURSOR\b', body, re.I))
        proc["has_dynamic_sql"] = bool(re.search(
            r'EXECUTE\s+IMMEDIATE|EXEC\s+SP_EXECUTESQL|PREPARE\s+\w+\s+FROM',
            body, re.I
        ))
        proc["has_exception_handler"] = bool(re.search(
            r'\bEXCEPTION\b|\bCATCH\b|\bERROR\s+HANDLER\b', body, re.I
        ))
    
    # ── Sanity checks for the LLM to see ──────────────────────────────────
    if not procs:
        warnings.append(
            f"NO PROCS FOUND in {total_lines} lines. The file may use "
            f"non-standard syntax. Use read_lines to inspect the file manually."
        )
    
    avg_lines = sum(p["line_count"] for p in procs) / len(procs) if procs else 0
    if avg_lines < 5 and len(procs) > 3:
        warnings.append(
            f"Average proc size is {avg_lines:.0f} lines — suspiciously small. "
            f"The end boundary pattern may be matching too early (nested ENDs?)."
        )
    
    coverage = sum(p["line_count"] for p in procs)
    if procs and coverage < total_lines * 0.5:
        warnings.append(
            f"Procs cover only {coverage}/{total_lines} lines ({coverage*100//total_lines}%). "
            f"Significant code outside proc boundaries — may be anonymous blocks, "
            f"package-level declarations, or missed proc boundaries."
        )
    
    return {
        "procs": procs,
        "packages": packages,
        "anonymous_blocks": anonymous_blocks,
        "total_lines": total_lines,
        "parse_method": parse_method,
        "parse_warnings": warnings,
    }


def _try_sqlglot_parse(raw: str, warnings: list) -> list[dict]:
    """Try to parse proc boundaries using sqlglot."""
    try:
        import sqlglot
    except ImportError:
        warnings.append("sqlglot not available — using regex only")
        return []
    
    procs = []
    # Try multiple dialects
    for dialect in ["oracle", "tsql", "postgres", "hive", None]:
        try:
            stmts = sqlglot.parse(raw, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN)
            for stmt in stmts:
                if stmt is None:
                    continue
                # Look for CREATE PROCEDURE/FUNCTION statements
                if hasattr(stmt, 'kind') and stmt.kind in ('PROCEDURE', 'FUNCTION'):
                    name = stmt.name if hasattr(stmt, 'name') else str(stmt.find(sqlglot.exp.Identifier))
                    # Get line positions from the statement's source positions
                    # sqlglot doesn't always have exact line numbers — estimate from text
                    start_pos = raw.find(str(stmt)[:50])
                    if start_pos >= 0:
                        start_line = raw[:start_pos].count('\n')
                        end_line = start_line + str(stmt).count('\n')
                        procs.append({
                            "name": str(name),
                            "start_line": start_line,
                            "end_line": end_line,
                            "line_count": end_line - start_line + 1,
                            "type": stmt.kind.lower() if hasattr(stmt, 'kind') else "procedure",
                            "params": [],
                        })
            if procs:
                return procs
        except Exception:
            continue
    
    return []


def _regex_parse(raw: str, lines: list[str], adapter: dict | None) -> tuple:
    """
    Multi-pattern regex parsing — handles:
      - CREATE [OR REPLACE] PROCEDURE/FUNCTION name
      - Package-body-internal PROCEDURE/FUNCTION name IS/AS
      - CREATE PACKAGE BODY name wrappers
      - Anonymous BEGIN...END blocks
    """
    procs = []
    packages = []
    anonymous_blocks = []
    
    # ── Proc/function boundaries ──────────────────────────────────────────
    # Pattern 1: Standard CREATE ... PROCEDURE/FUNCTION
    # Pattern 2: Package-body-internal (bare PROCEDURE/FUNCTION name IS/AS)
    patterns = [
        # Standard standalone
        (r'^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:PROCEDURE|FUNCTION)\s+(?:[\w$.]+\.)?(\w+)',
         r'END\s+\1\s*;|END\s*;'),
        # Package-body-internal
        (r'^\s+(?:PROCEDURE|FUNCTION)\s+(\w+)\s*(?:\([^)]*\))?\s*(?:IS|AS)\b',
         r'END\s+\1\s*;|END\s*;'),
    ]
    
    # If adapter provides patterns, add them as priority
    if adapter:
        pb = adapter.get("proc_boundary", {})
        start_pat = pb.get("start_pattern", "")
        end_pat = pb.get("end_pattern", "")
        if start_pat:
            patterns.insert(0, (start_pat, end_pat))
    
    found_names = set()
    
    for start_pattern, end_pattern in patterns:
        try:
            start_re = re.compile(start_pattern, re.IGNORECASE | re.MULTILINE)
        except re.error:
            continue
        
        for match in start_re.finditer(raw):
            try:
                name = match.group(1)
            except IndexError:
                name = match.group(0).split()[-1].strip("(")
            
            if name.upper() in found_names:
                continue
            
            start_byte = match.start()
            start_line = raw[:start_byte].count('\n')
            
            # Find end boundary
            search_from = match.end()
            end_line = start_line  # default
            
            if end_pattern:
                try:
                    # Use nesting-aware end detection
                    end_line = _find_end_with_nesting(
                        lines, start_line, name
                    )
                except Exception:
                    # Simple regex fallback
                    try:
                        end_re = re.compile(
                            end_pattern.replace(r'\1', re.escape(name)),
                            re.IGNORECASE
                        )
                        end_match = end_re.search(raw, search_from)
                        if end_match:
                            end_line = raw[:end_match.end()].count('\n')
                        else:
                            end_line = min(start_line + 500, len(lines) - 1)
                    except re.error:
                        end_line = min(start_line + 500, len(lines) - 1)
            
            if end_line <= start_line:
                end_line = min(start_line + 50, len(lines) - 1)
            
            found_names.add(name.upper())
            procs.append({
                "name": name,
                "start_line": start_line,
                "end_line": end_line,
                "line_count": end_line - start_line + 1,
                "type": "function" if "FUNCTION" in match.group(0).upper() else "procedure",
                "params": [],
            })
    
    # ── Package body detection ────────────────────────────────────────────
    pkg_re = re.compile(
        r'CREATE\s+(?:OR\s+REPLACE\s+)?PACKAGE\s+BODY\s+(?:[\w$.]+\.)?(\w+)',
        re.IGNORECASE | re.MULTILINE,
    )
    for match in pkg_re.finditer(raw):
        pkg_name = match.group(1)
        start_line = raw[:match.start()].count('\n')
        # Find member procs within this package
        members = [p["name"] for p in procs
                    if p["start_line"] > start_line]
        packages.append({
            "name": pkg_name,
            "start_line": start_line,
            "end_line": max(p["end_line"] for p in procs if p["name"] in members)
                        if members else start_line + 100,
            "members": members,
        })
    
    # Sort by start_line
    procs.sort(key=lambda p: p["start_line"])
    
    return procs, packages, anonymous_blocks


def _find_end_with_nesting(lines: list[str], start_line: int, proc_name: str) -> int:
    """Find proc end by tracking BEGIN/END nesting depth."""
    depth = 0
    in_proc = False
    
    for i in range(start_line, min(start_line + 2000, len(lines))):
        line_upper = lines[i].upper().strip()
        
        # Skip comments and empty lines
        if line_upper.startswith('--') or not line_upper:
            continue
        
        if re.search(r'\bBEGIN\b', line_upper):
            depth += 1
            in_proc = True
        
        if re.search(r'\bEND\b', line_upper) and in_proc:
            depth -= 1
            if depth <= 0:
                return i
    
    return min(start_line + 500, len(lines) - 1)


def _extract_tables(body: str, patterns: list[str]) -> list[str]:
    """Extract table names from a code body."""
    found = set()
    for pat in patterns:
        for match in re.finditer(pat, body, re.IGNORECASE):
            raw = match.group(1).strip().strip('"').strip("'")
            if raw.upper() not in _PSEUDOTABLES and len(raw) > 1:
                found.add(raw.lower())
    return sorted(found)


# ---------------------------------------------------------------------------
# Mode 2: find_all_calls — comprehensive function call scanning
# ---------------------------------------------------------------------------

def find_all_calls(file_path: str) -> dict:
    """
    Find ALL function-call-like patterns in a file.
    Returns raw list — the LLM filters SQL keywords vs real UDFs.
    
    Returns:
        {
            "calls": [{"name": str, "count": int, "sample_lines": [int]}],
            "total_unique": int,
            "total_occurrences": int
        }
    """
    path = Path(file_path)
    if not path.exists():
        return {"error": f"File not found: {file_path}", "calls": []}
    
    raw = path.read_text(encoding="utf-8", errors="replace")
    lines = raw.splitlines()
    
    # Find all patterns matching: IDENTIFIER(
    # Exclude lines that are comments
    call_counts: dict[str, dict] = {}  # name → {count, lines}
    
    pattern = re.compile(r'\b([A-Za-z_]\w{1,60})\s*\(', re.MULTILINE)
    
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith('--') or stripped.startswith('/*'):
            continue
        
        for match in pattern.finditer(line):
            name = match.group(1)
            name_upper = name.upper()
            
            # Skip SQL keywords
            if name_upper in _SQL_KEYWORDS:
                continue
            
            if name_upper not in call_counts:
                call_counts[name_upper] = {
                    "name": name,
                    "count": 0,
                    "sample_lines": [],
                }
            call_counts[name_upper]["count"] += 1
            if len(call_counts[name_upper]["sample_lines"]) < 3:
                call_counts[name_upper]["sample_lines"].append(i)
    
    # Sort by frequency (most common first)
    calls = sorted(call_counts.values(), key=lambda x: -x["count"])
    
    return {
        "calls": calls[:100],  # Cap at 100 unique names
        "total_unique": len(call_counts),
        "total_occurrences": sum(c["count"] for c in call_counts.values()),
    }


# ---------------------------------------------------------------------------
# Mode 3: search_code — grep across source files
# ---------------------------------------------------------------------------

def search_code(pattern: str, file_paths: list[str], max_results: int = 30) -> dict:
    """
    Search for a pattern across source files.
    
    Returns:
        {
            "matches": [{"file": str, "line": int, "text": str}],
            "total_matches": int,
            "files_searched": int
        }
    """
    matches = []
    files_searched = 0
    
    try:
        regex = re.compile(pattern, re.IGNORECASE)
    except re.error as e:
        return {"error": f"Invalid regex: {e}", "matches": []}
    
    for fp in file_paths:
        path = Path(fp)
        if not path.exists():
            continue
        files_searched += 1
        
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
            for i, line in enumerate(lines):
                if regex.search(line):
                    matches.append({
                        "file": fp,
                        "line": i,
                        "text": line.strip()[:200],
                    })
                    if len(matches) >= max_results:
                        return {
                            "matches": matches,
                            "total_matches": len(matches),
                            "files_searched": files_searched,
                            "truncated": True,
                        }
        except Exception:
            continue
    
    return {
        "matches": matches,
        "total_matches": len(matches),
        "files_searched": files_searched,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--args", required=True)
    args_ns = parser.parse_args()
    params = json.loads(args_ns.args)
    
    mode = params.get("mode", "parse_file")
    
    if mode == "parse_file":
        result = parse_file(
            params["file_path"],
            adapter=params.get("adapter"),
        )
    elif mode == "find_all_calls":
        result = find_all_calls(params["file_path"])
    elif mode == "search_code":
        result = search_code(
            params["pattern"],
            params.get("file_paths", []),
            max_results=params.get("max_results", 30),
        )
    else:
        result = {"error": f"Unknown mode: {mode}"}
    
    print(json.dumps(result, default=str))


if __name__ == "__main__":
    main()
