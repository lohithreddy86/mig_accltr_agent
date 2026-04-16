#!/usr/bin/env python3
"""
compute_chunks.py
=================
Sandbox script: compute safe chunk boundaries for large procs.
Driven by adapter.transaction_patterns — zero hardcoded dialect logic.

Called by: Planning Agent step P2
Input  (--args JSON): {
    "proc_name":   "sp_calc_interest",
    "source_file": "/source/procs.sql",
    "start_line":  10,
    "end_line":    420,
    "adapter":     { ... },
    "target_lines":  175,
    "max_lines":     200,
    "min_lines":     50,
}
Output (stdout JSON): {
    "chunks": [
        {
            "chunk_id":   "sp_calc_interest_c0",
            "start_line": 10,
            "end_line":   184,
            "line_count": 175,
            "state_vars": {"df_accounts": "DataFrame from prior chunk", ...}
        },
        ...
    ],
    "is_single_chunk": true/false,
    "total_chunks": 1
}

Chunking strategy:
  1. Read only lines[start_line:end_line]
  2. Track block nesting depth using adapter transaction patterns
     (BEGIN/END) — split only when depth == 1 (top-level block boundary)
  3. Target ~target_lines per chunk; hard max = max_lines
  4. Never split mid-expression: look ahead up to 10 lines for a clean boundary
  5. Track live variables at each boundary for state_var passing
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Variable / dataframe tracking
# ---------------------------------------------------------------------------

# Patterns that declare a variable in PySpark context (post-conversion)
_PYSPARK_VAR_PATTERNS = [
    r'\b(\w+)\s*=\s*spark\.(?:read|sql|table)',
    r'\b(\w+)\s*=\s*\w+\.(?:join|filter|select|groupBy|agg|withColumn)',
    r'\b(\w+)\s*=\s*(?:df|spark)\b',
]

# Patterns for SQL variables / INTO targets
_SQL_VAR_PATTERNS = [
    r'\bINTO\s+(\w+)',                        # SELECT ... INTO v_total
    r'\b(\w+)\s*:=\s*',                       # v_rate := 0.05 (PL/SQL assign)
    r'\bDECLARE\s+(\w+)\b',                   # DECLARE v_name TYPE
    r'\b(\w+)\s+\w+%TYPE\b',                  # v_id accounts.id%TYPE
]

def extract_live_vars(lines: list[str], start: int, end: int) -> dict[str, str]:
    """
    Identify variables that are live at the end of a chunk (start:end).
    Returns {var_name: "description"} for passing to the next chunk.
    Simple heuristic — real type inference is done by conversion LLM.
    """
    live = {}
    chunk_text = "\n".join(lines[start:end])

    for pat in _SQL_VAR_PATTERNS + _PYSPARK_VAR_PATTERNS:
        for m in re.finditer(pat, chunk_text, re.IGNORECASE):
            name = m.group(1)
            if name and len(name) > 1 and not name.upper() in {"NULL", "TRUE", "FALSE"}:
                live[name] = f"variable declared in chunk lines {start}–{end}"

    return live


# ---------------------------------------------------------------------------
# Block depth tracking
# ---------------------------------------------------------------------------

def compute_block_depth(
    line: str,
    depth: int,
    begin_re: re.Pattern,
    end_re: re.Pattern,
    cursor_depth: int = 0,
) -> tuple[int, int]:
    """
    Update block nesting depth AND cursor scope depth for a single line.

    Block depth: BEGIN increments, END decrements. Used for finding safe
    split points — only split at depth == 1 (top-level statement boundary).

    Cursor depth: OPEN/FOR..LOOP increments, CLOSE/END LOOP decrements.
    Used to prevent splitting inside a cursor loop, which would produce
    two fragments neither of which has a complete cursor pattern.

    Returns:
        (block_depth, cursor_depth)
    """
    stripped = line.strip().upper()
    # Skip commented lines
    if stripped.startswith("--") or stripped.startswith("/*"):
        return depth, cursor_depth

    begin_hits = len(begin_re.findall(line))
    end_hits   = len(end_re.findall(line))

    # ── Cursor scope tracking ─────────────────────────────────────────────
    # OPEN cursor / FOR rec IN (...) LOOP → entering cursor scope
    if re.search(r'\bOPEN\s+\w+', stripped) or \
       re.search(r'\bFOR\s+\w+\s+IN\b', stripped):
        cursor_depth += 1

    # CLOSE cursor → leaving cursor scope
    if re.search(r'\bCLOSE\s+\w+', stripped):
        cursor_depth = max(0, cursor_depth - 1)

    # END LOOP → leaving cursor scope AND should NOT decrement block depth.
    # The problem: \bEND\b matches both "END;" (block end) and "END LOOP;"
    # (loop end). END LOOP is not a block boundary — it closes a LOOP that
    # had no matching BEGIN. Subtract it from end_hits.
    end_loop_hits = len(re.findall(r'\bEND\s+LOOP\b', stripped))
    if end_loop_hits:
        end_hits = max(0, end_hits - end_loop_hits)
        cursor_depth = max(0, cursor_depth - end_loop_hits)

    # Similarly, END IF should not decrement block depth
    end_if_hits = len(re.findall(r'\bEND\s+IF\b', stripped))
    if end_if_hits:
        end_hits = max(0, end_hits - end_if_hits)

    new_depth = max(0, depth + begin_hits - end_hits)
    return new_depth, cursor_depth


# ---------------------------------------------------------------------------
# Core chunking
# ---------------------------------------------------------------------------

def compute_chunks(
    lines:        list[str],
    proc_start:   int,
    proc_end:     int,
    adapter:      dict,
    target_lines: int = 175,
    max_lines:    int = 200,
    min_lines:    int = 50,
    proc_name:    str = "proc",
) -> list[dict]:
    """
    Split a proc body into safe chunks for LLM conversion.
    Only ever splits at depth == 1 (top-level block boundaries).
    """
    proc_lines  = lines[proc_start:proc_end + 1]
    total_lines = len(proc_lines)

    # If proc fits in one chunk — return immediately
    if total_lines <= max_lines:
        return [{
            "chunk_id":   f"{proc_name}_c0",
            "start_line": proc_start,
            "end_line":   proc_end,
            "line_count": total_lines,
            "state_vars": {},
        }]

    # Build depth tracker from adapter patterns
    tp = adapter.get("transaction_patterns", {})
    begin_pat = tp.get("begin", r"\bBEGIN\b")
    end_pat   = tp.get("end_block", r"\bEND\b")

    try:
        begin_re = re.compile(begin_pat, re.IGNORECASE)
        end_re   = re.compile(end_pat,   re.IGNORECASE)
    except re.error:
        begin_re = re.compile(r"\bBEGIN\b", re.IGNORECASE)
        end_re   = re.compile(r"\bEND\b",   re.IGNORECASE)

    # Walk the proc body tracking depth
    depth        = 0
    cursor_depth = 0
    chunk_start  = 0   # relative to proc_start
    chunks: list[dict] = []
    chunk_idx   = 0

    for rel_idx, line in enumerate(proc_lines):
        depth, cursor_depth = compute_block_depth(
            line, depth, begin_re, end_re, cursor_depth,
        )
        lines_in_chunk = rel_idx - chunk_start + 1

        # Split condition: at or past target, at depth 1, NOT inside a cursor
        # scope, and not too small. The cursor_depth check prevents splitting
        # inside OPEN cursor...CLOSE cursor or FOR rec IN...END LOOP blocks.
        if (
            lines_in_chunk >= target_lines
            and depth == 1
            and cursor_depth == 0
            and lines_in_chunk >= min_lines
        ):
            # Look ahead up to 10 lines for a cleaner boundary
            # (prefer splitting after a COMMIT, END LOOP, or blank line)
            split_at = rel_idx
            for lookahead in range(1, min(11, total_lines - rel_idx)):
                la_line = proc_lines[rel_idx + lookahead].strip().upper()
                if (
                    la_line in ("", "COMMIT;", "COMMIT", "END LOOP;")
                    or re.match(r"END\s+LOOP", la_line)
                    or (rel_idx + lookahead - chunk_start) >= max_lines
                ):
                    split_at = rel_idx + lookahead
                    break

            abs_start = proc_start + chunk_start
            abs_end   = proc_start + split_at

            state_vars = extract_live_vars(lines, abs_start, abs_end)
            chunks.append({
                "chunk_id":   f"{proc_name}_c{chunk_idx}",
                "start_line": abs_start,
                "end_line":   abs_end,
                "line_count": abs_end - abs_start + 1,
                "state_vars": state_vars,
            })
            chunk_idx  += 1
            chunk_start = split_at + 1

    # Final chunk (remainder)
    if chunk_start <= total_lines - 1:
        abs_start = proc_start + chunk_start
        abs_end   = proc_end
        if abs_end > abs_start:
            state_vars = extract_live_vars(lines, abs_start, abs_end)
            chunks.append({
                "chunk_id":   f"{proc_name}_c{chunk_idx}",
                "start_line": abs_start,
                "end_line":   abs_end,
                "line_count": abs_end - abs_start + 1,
                "state_vars": state_vars,
            })

    # If chunking produced nothing (e.g. depth never reached 1), fall back to single chunk
    if not chunks:
        chunks = [{
            "chunk_id":   f"{proc_name}_c0",
            "start_line": proc_start,
            "end_line":   proc_end,
            "line_count": total_lines,
            "state_vars": {},
        }]

    return chunks


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--args", required=True)
    args_ns = parser.parse_args()
    params  = json.loads(args_ns.args)

    source_file = params["source_file"]
    start_line  = params["start_line"]
    end_line    = params["end_line"]
    proc_name   = params.get("proc_name", "proc")
    adapter     = params.get("adapter", {})
    target_lines = params.get("target_lines", 175)
    max_lines    = params.get("max_lines", 200)
    min_lines    = params.get("min_lines", 50)

    path = Path(source_file)
    if not path.exists():
        print(json.dumps({
            "error": f"File not found: {source_file}",
            "chunks": [], "is_single_chunk": True, "total_chunks": 0
        }))
        sys.exit(0)

    with open(path, encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    chunks = compute_chunks(
        lines=lines,
        proc_start=start_line,
        proc_end=end_line,
        adapter=adapter,
        target_lines=target_lines,
        max_lines=max_lines,
        min_lines=min_lines,
        proc_name=proc_name,
    )

    print(json.dumps({
        "chunks":          chunks,
        "is_single_chunk": len(chunks) == 1,
        "total_chunks":    len(chunks),
    }))


if __name__ == "__main__":
    main()
