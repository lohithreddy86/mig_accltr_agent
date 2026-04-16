# v10 Task 2: Per-Proc LLM Analysis

## Problem
Current A2+ sends file paths to one giant LLM call (80 tool calls, 35 turns).
LLM selectively reads some procs, skips others. Regex table extraction confuses
CTEs, views, cursor names with real tables. construct_map never enriched.

## Solution
Replace single giant LLM call with sequential per-proc LLM analysis.
A1 extracts proc bodies. A2 sends each to LLM one-by-one with rolling summary.
A3 resolves only REAL_TABLE entries via MCP. A4 synthesizes into final artifacts.

## Files Modified

| File | Change |
|------|--------|
| `models/analysis.py` | Add `TableRef`, `PerProcAnalysis` models |
| `config_loader.py` | Add per-proc fields to `AnalysisConfig` |
| `config.yaml` | Add per-proc config section |
| `agents/analysis/agent.py` | Rewrite run() with A2/A3/A4, comment out old path |
| `agents/analysis/semantic_analysis.py` | Comment out entirely |

## Files NOT Modified (downstream unchanged)
- `models/planning.py` — reads same artifacts
- `agents/planning/agent.py` — reads same artifacts
- `agents/orchestrator/agent.py` — reads same artifacts
- `agents/conversion/agent.py` — reads same artifacts
- `core/tool_executor.py` — INTELLIGENT_ANALYSIS_TOOLS left (harmless)
- All UI files

## Checkpoints

### CP1: Add models to analysis.py
- `TableRef(name, classification, used_as)` — per-table classification
- `PerProcAnalysis(...)` — structured output from each A2 LLM call

### CP2: Add config
- `analysis.per_proc_prompt_max_lines` (default 600)
- `analysis.rolling_summary_max_chars` (default 3000)

### CP3: Implement _a2_analyze_proc() + _a2_per_proc_loop()
- Read proc body from source file using start_line/end_line
- Build focused prompt with: body + adapter functions + rolling summary
- Parse structured JSON response into PerProcAnalysis
- Accumulate rolling summary

### CP4: Implement _a3_resolve_classified_tables()
- Collect all REAL_TABLE from A2 results
- Resolve via MCP (reuse existing _a2_resolve_tables logic)
- Zero CTE/view false positives

### CP5: Implement _a4_synthesize()
- Build manifest.json from PerProcAnalysis results
- Build table_registry.json from A3
- Build dependency_graph.json + conversion_order.json deterministically
- Build complexity_report.json from per-proc scores
- Build semantic_map.json from per-proc business_purpose/stage
- Enrich construct_map.json with discovered dialect functions

### CP6: Rewrite run() — wire new flow, comment out old
- Comment out run_intelligent_analysis import and call
- Comment out _unpack_intelligent_results
- Wire: A0 → A1 → A2 loop → A3 → verify tables → A4

### CP7: Comment out semantic_analysis.py

### CP8: Verify + rezip

## Artifact Schema Compatibility

Each artifact must match exactly what Planning reads:

| Artifact | Key fields Planning uses | Source in new flow |
|----------|--------------------------|-------------------|
| manifest.json | ProcEntry.{name,source_file,start/end_line,tables_read,tables_written,calls,unresolved_deps,has_cursor,has_dynamic_sql,dialect_functions_in_body} | A4 builds from PerProcAnalysis |
| table_registry.json | TableRegistryEntry.{source_name,trino_fqn,status,columns} | A3 MCP resolution |
| dependency_graph.json | edges, cycles, external_deps | A4 deterministic from calls |
| conversion_order.json | order list, has_cycles | A4 topological sort |
| complexity_report.json | ComplexityEntry.{name,score,rationale} | A4 from per-proc scores |
| semantic_map.json | proc_summaries, stages, discovered_dependencies | A4 from per-proc analysis |
| construct_map.json | trino_map, pyspark_map, unmapped | A4 enriches A0 map |
| dialect_profile.json | unchanged from A0 | Not touched |
