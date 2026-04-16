#!/usr/bin/env python3
"""
build_graph.py
==============
Sandbox script: dependency graph, topological sort, and deep extraction.
Runs inside Podman container (networkx available in image).

Handles two modes via --args mode field:

MODE "dependency_graph":
  Called by: Analysis Agent step A3
  Input: {"mode": "dependency_graph", "procs": [...manifest proc list...]}
  Output: {
    "edges":         [[caller, callee], ...],
    "cycles":        [[proc1, proc2, ...], ...],
    "conversion_order": ["proc_b", "proc_a", ...],  # deps first
    "external_deps": {"proc": ["missing_dep", ...]}
  }

MODE "deep_extract":
  Called by: Analysis Agent step A4
  Input: {
    "mode":           "deep_extract",
    "proc_name":      "sp_calc_interest",
    "source_file":    "/source/procs.sql",
    "start_line":     10,
    "end_line":       350,
    "adapter":        { ... }
  }
  Output: {
    "cursor_nesting_depth":       2,
    "dynamic_sql_patterns_found": [...],
    "unsupported_occurrences":    {"CONNECT BY": 2},
    "dialect_functions_in_body":  ["NVL", "DECODE", ...]
  }
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# MODE: dependency_graph
# ---------------------------------------------------------------------------

def build_dependency_graph(procs: list[dict]) -> dict:
    """
    Build directed call graph and topological conversion order.
    Edge A→B means proc A calls proc B (B must convert first).
    """
    try:
        import networkx as nx
    except ImportError:
        # Fallback without networkx — simple linear order
        return _simple_order_fallback(procs)

    G = nx.DiGraph()
    all_names = {p["name"] for p in procs}

    for p in procs:
        G.add_node(p["name"])
        for callee in p.get("calls", []):
            if callee in all_names:
                # A calls B → edge A→B
                G.add_edge(p["name"], callee)

    # Detect cycles
    cycles = list(nx.simple_cycles(G))

    # External deps: called but not in manifest
    external_deps: dict[str, list[str]] = {}
    for p in procs:
        missing = p.get("unresolved_deps", [])
        if missing:
            external_deps[p["name"]] = missing

    # Topological sort — reversed so dependencies come first
    try:
        topo = list(reversed(list(nx.topological_sort(G))))
        # Shared utilities (called by many) bubble to front
        caller_count = {n: 0 for n in G.nodes}
        for u, v in G.edges():
            caller_count[v] = caller_count.get(v, 0) + 1
        topo.sort(key=lambda n: -caller_count.get(n, 0))
    except nx.NetworkXUnfeasible:
        # Has cycles — use BFS best-effort order, flag cycle nodes
        cycle_nodes = {node for cycle in cycles for node in cycle}
        non_cycle = [n for n in G.nodes if n not in cycle_nodes]
        try:
            safe_order = list(reversed(list(
                nx.topological_sort(G.subgraph(non_cycle))
            )))
        except Exception:
            safe_order = list(non_cycle)
        topo = safe_order + list(cycle_nodes)

    edges = [[u, v] for u, v in G.edges()]

    return {
        "edges":            edges,
        "cycles":           cycles,
        "has_cycles":       len(cycles) > 0,
        "cycle_warning":    f"{len(cycles)} circular dependency group(s) found — marked NEEDS_MANUAL"
                            if cycles else "",
        "conversion_order": topo,
        "external_deps":    external_deps,
    }


def _simple_order_fallback(procs: list[dict]) -> dict:
    """Fallback topological order without networkx (linear by call depth)."""
    name_to_proc = {p["name"]: p for p in procs}
    visited = set()
    order   = []

    def visit(name: str, depth: int = 0) -> None:
        if name in visited or depth > 100:
            return
        visited.add(name)
        proc = name_to_proc.get(name, {})
        for callee in proc.get("calls", []):
            if callee in name_to_proc:
                visit(callee, depth + 1)
        order.append(name)

    for p in procs:
        visit(p["name"])

    return {
        "edges":            [],
        "cycles":           [],
        "has_cycles":       False,
        "cycle_warning":    "",
        "conversion_order": order,
        "external_deps":    {},
    }


# ---------------------------------------------------------------------------
# MODE: deep_extract
# ---------------------------------------------------------------------------

def deep_extract(
    proc_name:   str,
    source_file: str,
    start_line:  int,
    end_line:    int,
    adapter:     dict,
) -> dict:
    """
    Deep analysis of a single proc body.
    Called only for procs flagged as needing deep analysis (HIGH/NEEDS_MANUAL candidates).
    Reads ONLY the specific line range — never the full file.
    """
    path = Path(source_file)
    if not path.exists():
        return {"error": f"File not found: {source_file}"}

    with open(path, encoding="utf-8", errors="replace") as f:
        all_lines = f.readlines()

    body = "".join(all_lines[start_line:end_line + 1])

    result: dict = {
        "proc_name":               proc_name,
        "cursor_nesting_depth":    0,
        "dynamic_sql_patterns_found": [],
        "unsupported_occurrences": {},
        "dialect_functions_in_body": [],
        "has_bulk_collect":        False,
        "has_merge":               False,
        "autonomous_transaction":  False,
        "loop_count":              0,
    }

    # ── Cursor nesting depth ──────────────────────────────────────────────
    depth = 0
    max_depth = 0
    # Look for loop start/end patterns (FOR...LOOP, WHILE...LOOP, LOOP/END LOOP)
    for line in body.splitlines():
        ul = line.upper().strip()
        if re.search(r'\bFOR\b.+\bLOOP\b', ul) or re.search(r'\bWHILE\b.+\bLOOP\b', ul) or ul == 'LOOP':
            depth += 1
            result["loop_count"] += 1
        if re.search(r'\bEND\s+LOOP\b', ul):
            depth = max(0, depth - 1)
        max_depth = max(max_depth, depth)
    result["cursor_nesting_depth"] = max_depth

    # ── Dynamic SQL pattern analysis ──────────────────────────────────────
    dyn_patterns = adapter.get("dynamic_sql_patterns", [])
    found_dyn = []
    for pat in dyn_patterns:
        matches = re.findall(pat + r'.{0,120}', body, re.IGNORECASE)
        found_dyn.extend(m.strip() for m in matches[:3])  # max 3 examples
    result["dynamic_sql_patterns_found"] = found_dyn

    # Classify what's being built dynamically
    dyn_text = " ".join(found_dyn).upper()
    if re.search(r'\bSELECT\b|\bINSERT\b|\bUPDATE\b', dyn_text):
        result["dynamic_sql_complexity"] = "full_queries"
    elif re.search(r'\bCOLUMN\b|\bFIELD\b', dyn_text):
        result["dynamic_sql_complexity"] = "column_lists"
    elif found_dyn:
        result["dynamic_sql_complexity"] = "table_names"
    else:
        result["dynamic_sql_complexity"] = ""

    # ── Unsupported construct occurrences ─────────────────────────────────
    unsupported = adapter.get("unsupported_constructs", [])
    for construct in unsupported:
        count = len(re.findall(re.escape(construct), body, re.IGNORECASE))
        if count > 0:
            result["unsupported_occurrences"][construct] = count

    # ── Dialect functions actually present in this proc body ──────────────
    dialect_fns = adapter.get("dialect_functions", [])
    body_upper  = body.upper()
    result["dialect_functions_in_body"] = [
        fn for fn in dialect_fns
        if re.search(r'\b' + re.escape(fn.upper()) + r'\b', body_upper)
    ]

    # ── Special flags ─────────────────────────────────────────────────────
    result["has_bulk_collect"]       = bool(re.search(r'\bBULK\s+COLLECT\b', body, re.I))
    result["has_merge"]              = bool(re.search(r'\bMERGE\s+INTO\b', body, re.I))
    result["autonomous_transaction"] = bool(
        re.search(r'\bPRAGMA\s+AUTONOMOUS_TRANSACTION\b', body, re.I)
    )

    return result


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--args", required=True)
    args_ns = parser.parse_args()
    params  = json.loads(args_ns.args)

    mode = params.get("mode", "dependency_graph")

    if mode == "dependency_graph":
        result = build_dependency_graph(params.get("procs", []))
        print(json.dumps(result))

    elif mode == "deep_extract":
        result = deep_extract(
            proc_name=params["proc_name"],
            source_file=params["source_file"],
            start_line=params["start_line"],
            end_line=params["end_line"],
            adapter=params.get("adapter", {}),
        )
        print(json.dumps(result))

    else:
        print(json.dumps({"error": f"Unknown mode: {mode}"}), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
