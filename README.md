# SQL Migration System

**Multi-agent system for migrating Oracle / DB2 / Teradata / SQL Server stored procedures AND standalone SQL queries to PySpark and Trino SQL on a Starburst Trino lakehouse.**

Built as an internal engineering tool for the HDFC Bank data platform team. Handles hundreds of stored procedures with heterogeneous complexity — from trivial single-SELECT procs to multi-thousand-line hierarchical cursor trees — with full crash recovery, human feedback loops, and a live monitoring UI. Also supports a **query mode** for converting standalone SQL queries (OBIEE reports, ad-hoc queries, views) where the full proc pipeline complexity is bypassed.

---

## Table of Contents

1. [What It Does](#what-it-does)
2. [Two Modes](#two-modes)
3. [Architecture](#architecture)
4. [Agent Reference](#agent-reference)
5. [Core Design Principles](#core-design-principles)
6. [Project Layout](#project-layout)
7. [Artifact Store](#artifact-store)
8. [Configuration Reference](#configuration-reference)
9. [Adapter System](#adapter-system)
10. [Conversion Strategies](#conversion-strategies)
11. [Loop Guards and Failure Handling](#loop-guards-and-failure-handling)
12. [Human Feedback Loop](#human-feedback-loop)
13. [Validation System](#validation-system)
14. [History Compaction](#history-compaction)
15. [Streamlit UI Guide](#streamlit-ui-guide)
16. [CLI Reference](#cli-reference)
17. [Integration Tests](#integration-tests)

---

## What It Does

Given a set of source SQL files and a README.docx describing the source system, the migration system:

1. **Parses the README** and selects a dialect adapter from the registry via an LLM tool call — the LLM reads 200 lines of source code and picks the best-fit adapter, verifying its patterns against the actual code
2. **Extracts structural metadata** — in proc mode: proc boundaries, table reads/writes, inter-proc call graphs. In query mode: splits on `;` into individual statements
3. **Analyzes each unit individually** via per-proc/per-query LLM calls — classifies tables (REAL_TABLE vs CTE_ALIAS vs CURSOR_NAME vs VIEW vs VARIABLE), discovers column-level function usage and data type mappings, assesses complexity
4. **Resolves source tables** against the live Trino lakehouse via MCP — only REAL_TABLE entries, zero false positives from CTE aliases
5. **Builds a dependency graph** and topological conversion order (proc mode) or sequential order (query mode)
6. **Selects a conversion strategy** per proc: TRINO_SQL, PYSPARK_DF, PYSPARK_PIPELINE, or MANUAL_SKELETON. Query mode forces TRINO_SQL
7. **Computes chunk boundaries** using adapter transaction patterns so large procs are never split mid-block. Queries are always single-chunk
8. **Fetches live Trino schema** per chunk and injects it alongside construct hints, data type mappings, and column-level analysis into every LLM call
9. **Converts each chunk** with a self-correction loop: syntax check → dialect remnant detection → targeted LLM fix. Query mode uses MCP EXPLAIN for verification instead of PySpark sandbox
10. **Assembles chunks** into final .py or .sql files with deduped imports, function wrappers, integration checks (proc mode). Query mode writes .sql directly
11. **Validates output** at one of three depth levels depending on data availability
12. **Checkpoints every state transition** — crash-safe, resume from exact point of failure

---

## Two Modes

Set `Source Type` in README Section 1 to control the pipeline mode.

| README Field | Value | Pipeline behavior |
|-------------|-------|-------------------|
| Source Type | `stored_procedures` (default) | Full pipeline: proc boundaries, dependency graph, chunking, retry/replan, C5 assembly |
| Source Type | `sql_queries` | Simplified: split on `;`, no deps, force TRINO_SQL, single chunk, MCP EXPLAIN verify |

Aliases accepted: `queries`, `sql`, `query`, `views`, `reports`, `obiee` all map to `sql_queries`. `procedures`, `procs`, `sp` map to `stored_procedures`.

### Query Mode Bypass Points

| Stage | Proc Mode | Query Mode |
|-------|-----------|------------|
| A1 | Proc boundary regex | Split on `;`, name from comments |
| A2 | Rolling summary + persistence/resume | No summary, no persistence |
| A4 | Dep graph + topological sort | Empty graph, sequential, all LOW |
| Planning | P1 LLM strategy + P1.5 grouping + P2 chunking | Force TRINO_SQL, 1 chunk each |
| Orchestrator | Retry + replan + freeze | No retry, mark PARTIAL on fail |
| Conversion | Full agentic loop (10 calls), sandbox exec | Small budget (4 calls), MCP EXPLAIN |
| C5 | Multi-chunk assembly | Skipped — writes .sql directly |

---

## Architecture

```
Source SQL Files + README.docx
         |
    +----+--------------------------------------------------------------+
    |  0 . Analysis Agent (A0-A4)                                       |
    |  A0 LLM+SBX  Parse README, select+validate adapter via tool call  |
    |              Build construct_map (fn mappings + type_mappings)     |
    |  A1 SANDBOX   Proc mode: boundary scan. Query mode: split on ;    |
    |  A2 LLM x N   Per-proc/query analysis: tables, calls, complexity, |
    |               column-level function usage, rolling summary (procs)|
    |  A3 MCP       Resolve REAL_TABLE only (no CTEs/views/cursors)     |
    |  A4 SYNTH     Merge into manifest, dep graph, semantic map,       |
    |               column_analysis.json, enriched construct_map        |
    +----+--------------------------------------------------------------+
         |
    +----+--------------------------------------------------------------+
    |  1 . Planning Agent (P1-P4)                                       |
    |  P1 LLM       Strategy per proc (or force TRINO_SQL in query mode)|
    |  P2 SANDBOX   Chunk boundaries (skipped in query mode)            |
    |  P3 MCP       Live schema per chunk (SELECT * LIMIT 0)           |
    |  P4 SANDBOX   Plan assembly: construct_hints + type_mappings      |
    +----+--------------------------------------------------------------+
         |
    +----+--------------------------------------------------------------+
    |  2 . Orchestrator  [zero LLM calls -- pure state machine]         |
    |  Dependency-gated dispatch . loop guards . checkpoint recovery     |
    |  Query mode: sequential dispatch, no retry/replan                 |
    +----+---------------------------------+----------------------------+
         |                                 |
    +----+------------------+    +---------+--------------------+
    |  3 . Conversion Agent |    |  4 . Validation Agent        |
    |  C1 SANDBOX  Extract  |    |  V1 MCP    Row counts        |
    |  C2 LLM      Convert  |    |  V2 MCP+SBX Sample data      |
    |     +type_mappings     |    |  V3 SANDBOX Dry-run Spark    |
    |     +column_analysis   +--->|  V4 LLM+MCP Semantic diff    |
    |  C3 SANDBOX  Validate  |    |  V5 SANDBOX Score            |
    |  C4 LLM      Correct   |    +------------------------------+
    |  C5 SANDBOX  Assemble  |
    |  (Query: MCP EXPLAIN)  |
    +------------------------+
```

---

## Agent Reference

### Analysis Agent (A0-A4)

Handles the entire setup-through-analysis pipeline. A0 absorbs what the former Detection Agent did (removed in v10). A1 performs deterministic boundary scanning (proc mode) or statement splitting (query mode). A2 sends each proc/query to the LLM individually with rolling summary. A3 resolves only real tables. A4 synthesizes into final artifacts.

**A0 — Setup**

| Step | Tool | What happens |
|------|------|-------------|
| A0.1 | SANDBOX | Parse README.docx → readme_signals.json (8 structured sections + source_type) |
| A0.2 | — | Read first 200 lines from source file (HEAD sample) |
| A0.3 | — | Build descriptions of all adapters in registry |
| A0.4 | LLM | LLM receives: 200 lines + README signals + adapter descriptions. Picks adapter via `load_adapter` tool call, validates against code, submits via `submit_adapter`. Falls back to README synthesis → generic if LLM fails |
| A0.5 | — | Build construct_map (fn mappings + type_mappings) + dialect_profile (includes source_type) |

**A1 — Boundary Scan / Statement Split**

| Mode | Tool | What happens |
|------|------|-------------|
| Proc | SANDBOX | `extract_structure.py` with validated adapter patterns → proc_inventory.json |
| Query | SANDBOX | `extract_structure.py` mode=split_statements → split on `;`, name from `-- Report: X` comments |
| Both | — | Individual .sql files written to `00_analysis/procs/{name}.sql` |

**A2 — Per-proc/query LLM Analysis (v10)**

| Step | Tool | What happens |
|------|------|-------------|
| A2 | LLM×N | Each proc/query body sent to LLM individually. LLM classifies tables (REAL_TABLE vs CTE_ALIAS vs CURSOR_NAME vs VARIABLE vs VIEW), discovers calls, assesses complexity, returns business purpose and **column-level function analysis** (column X in table Y uses NVL → map to COALESCE with DECIMAL type). Rolling summary carries context across procs (skipped in query mode). Per-proc persistence for crash-resume (skipped in query mode). Per-proc fallback to regex if LLM fails. |

**A3 — Table Resolution**

| Step | Tool | What happens |
|------|------|-------------|
| A3 | MCP | Only REAL_TABLE entries resolved against Trino. CTE aliases, cursor names, variables filtered out — zero false positives. Fail fast if real tables are MISSING. |

**A4 — Synthesis**

| Step | Tool | What happens |
|------|------|-------------|
| A4 | SANDBOX+deterministic | Merge per-proc results into: manifest.json, table_registry.json, dependency_graph.json (empty in query mode), conversion_order.json, complexity_report.json (all LOW in query mode), semantic_map.json, **column_analysis.json** (NEW). Enrich construct_map.json with discovered dialect functions + type_mappings. |

### Planning Agent (P1-P4)

Selects conversion strategies and assembles the master plan. Query mode skips P1 LLM, P1.5 module grouping, and P2 chunking — forces TRINO_SQL with single chunk per query.

| Step | Tool | Proc Mode | Query Mode |
|------|------|-----------|------------|
| P1 | LLM | Per-proc strategy selection | Force TRINO_SQL |
| P1.5 | LLM | Module grouping | Skipped |
| P2 | SANDBOX | Chunk boundaries (target 400, max 500 lines) | 1 chunk per query |
| P3 | MCP | Live schema fetch per chunk | Same (needed for column types) |
| P4 | SANDBOX | Plan assembly with construct_hints + loop_guards | Same (no write_semantics) |

Supports **targeted replan**: re-runs P1+P4 for a single failed proc with replan_notes from the Orchestrator (proc mode only).

### Orchestrator

Pure state machine — zero LLM calls.

**Proc mode:** PENDING → DISPATCHED → CHUNK_CONVERTING → CHUNK_DONE (× N) → ALL_CHUNKS_DONE → VALIDATING → VALIDATED | PARTIAL | FAILED → (replan?) → FROZEN | SKIPPED

**Query mode:** PENDING → DISPATCHED → CHUNK_CONVERTING → CHUNK_DONE → ALL_CHUNKS_DONE → VALIDATING → VALIDATED | PARTIAL. No retry, no replan, no freeze.

Loop guards (proc mode): max_chunk_retry (2) → trigger replan; frozen_after (3) replans → freeze proc. Every state transition writes checkpoint.json atomically.

### Conversion Agent (C1-C5)

| Step | Tool | Proc Mode | Query Mode |
|------|------|-----------|------------|
| C1 | SANDBOX | Extract lines[start:end] with Trino FQN annotations | Same |
| C2 | LLM | Convert with: construct_hints + **type_mappings** + schema_context + **column_analysis** + callee_signatures + state_vars + write_directives. Budget: 10 tool calls | Convert with: construct_hints + **type_mappings** + schema_context + **column_analysis**. Budget: 4 tool calls. Verify via MCP EXPLAIN |
| C3 | SANDBOX | Syntax check + dialect remnant scan + TODO extraction | Skipped (MCP EXPLAIN in C2) |
| C4 | LLM | Self-correction with exact error list. Max 2 attempts | 1 retry max |
| C5 | SANDBOX | Assembly: dedup imports, wrap function, integration check | Skipped — .sql written directly after C2 |

**Conversion prompt now includes four context blocks:**
- `CONSTRUCT MAPPINGS`: NVL → COALESCE, DECODE → CASE, etc.
- `DATA TYPE MAPPINGS`: NUMBER → DECIMAL, VARCHAR2 → VARCHAR, DATE → TIMESTAMP, etc.
- `TABLE SCHEMA`: Exact Trino column names and types from live MCP probe
- `COLUMN ANALYSIS`: Per-column function usage with source types and Trino equivalents

### Validation Agent (V1-V5)

| Level | Condition | What runs |
|-------|-----------|-----------|
| 1 STATIC_ONLY | Tables missing | Syntax only |
| 2 SCHEMA_PLUS_EXEC | Tables exist, no sample files | Dry-run against synthetic data |
| 3 FULL_RECONCILIATION | Tables + sample files | Dry-run + semantic diff vs real Trino |

---

## Core Design Principles

1. **LLM Never Sees Bulk Code** — only manifest metadata, bounded 400-500 line chunks, and construct coverage maps
2. **Zero Hardcoded Dialect Logic** — all patterns live in adapters/{dialect}.json, including type_mappings
3. **Orchestrator Has Zero LLM Calls** — pure state machine, auditable, crash-safe
4. **Fail Fast, Never Block** — frozen deps propagate immediately; never waits for a stuck proc
5. **Schema Context, Never Guessed** — exact Trino column names/types + data type mappings injected into every conversion call
6. **Adapters are Persistent Assets** — generated once, reused forever, improvable by developers
7. **Query Mode is a Simplification, Not a Separate Pipeline** — same stages, same artifacts, `is_query_mode` flag skips complexity at each branch point

---

## Project Layout

```
sql_migration/
├── config/
│   ├── config.yaml              All tunable parameters
│   └── .env.example             API keys template
├── adapters/
│   ├── oracle.json              Hand-crafted Oracle adapter (50+ fn mappings + 30 type_mappings)
│   ├── db2.json                 Hand-crafted DB2 adapter
│   ├── sqlserver.json           Hand-crafted SQL Server adapter
│   ├── teradata.json            Hand-crafted Teradata adapter
│   └── generic.json             Fallback adapter
├── sandbox_scripts/
│   ├── extraction/
│   │   ├── parse_readme.py      A0 (README parsing, source_type extraction)
│   │   ├── extract_structure.py A1/C1 (boundary scan, statement split, chunk extraction)
│   │   ├── build_graph.py       A4 (dependency graph, deep extraction)
│   │   ├── parse_and_scan.py    Intelligent analysis tools (v9, disabled)
│   │   └── compute_chunks.py    P2 (chunk boundaries)
│   ├── conversion/
│   │   ├── validate_conversion.py  C3
│   │   └── assemble_proc.py        C5
│   └── validation/
│       └── run_validation.py    V3
├── src/sql_migration/
│   ├── core/                    config_loader, artifact_store, llm_client,
│   │                            history_compaction, sandbox (with path translation),
│   │                            mcp_client, tool_executor, human_feedback, logger
│   ├── models/                  common, detection (ReadmeSignals, DialectProfile,
│   │                            ConstructMap with type_mappings, DialectAdapter),
│   │                            analysis (ProcEntry, PerProcAnalysis, TableRef),
│   │                            planning, pipeline
│   ├── agents/                  analysis, planning, orchestrator,
│   │                            conversion, validation
│   └── main.py                  CLI entry point
├── ui/
│   ├── app.py                   Streamlit entry
│   ├── ui_helpers.py            Shared helpers
│   └── pages/                   submit_job, pipeline_monitor,
│                                frozen_feedback, artifacts
├── tests/
│   └── test_integration.py      7 end-to-end tests
├── Dockerfile.sandbox           Podman container image
├── pyproject.toml
└── README.md
```

---

## Artifact Store

All inter-agent communication happens through the artifact store (never in-memory between runs).

```
artifacts/{run_id}/
├── 00_analysis/     dialect_profile.json, construct_map.json (with type_mappings),
│                    readme_signals.json, proc_inventory.json, manifest.json,
│                    table_registry.json, dependency_graph.json, complexity_report.json,
│                    conversion_order.json, semantic_map.json, column_analysis.json,
│                    proc_analysis_{name}.json (per-proc, for resume),
│                    procs/{name}.sql (individual extracted proc/query bodies)
├── 01_planning/     plan.json, strategy_map.json, chunk_plan.json, module_grouping.json
├── 02_orchestrator/ checkpoint.json, task_{proc}_{chunk}.json, replan_request_{proc}.json
├── 03_conversion/   proc_{name}.py/.sql, conversion_log_{name}.json, result_{proc}_{chunk}.json,
│                    callee_interface_{name}.json, test_{proc}_{chunk}.py
├── 04_validation/   validation_{name}.json, diff_{name}.json, manual_review_{name}.json
├── 05_developer_feedback/  developer_feedback_{proc}.json, feedback_resolved_{proc}.json
└── 06_samples/      {proc_name}/{table}.parquet
```

All writes are atomic (tmp → rename). Every JSON artifact is wrapped in a metadata envelope with run_id, written_at, agent, artifact_version.

---

## Configuration Reference

Key settings in config/config.yaml:

| Section.Key | Default | Purpose |
|-------------|---------|---------|
| llm.primary_model | gemini-2.5-flash | Main model for all agent calls |
| llm.compaction_model | gemini-2.0-flash | Smaller model for history compaction |
| analysis.head_sample_lines | 200 | Lines to read for A0 adapter validation |
| analysis.adapter_registry | oracle, db2, ... | Maps dialect_id to adapter JSON filename |
| analysis.per_proc_max_lines | 600 | Procs larger than this fall back to regex |
| analysis.rolling_summary_max_chars | 3000 | Truncate rolling summary to fit context |
| analysis.query_mode_max_statements | 500 | Max queries per file in query mode |
| planning.chunk_target_lines | 400 | Target lines per conversion chunk |
| planning.chunk_max_lines | 500 | Hard maximum per chunk |
| planning.loop_guards.max_chunk_retry | 2 | Chunk retry limit before replan |
| planning.loop_guards.frozen_after | 3 | Replans before freezing proc |
| agentic.conversion_budget_multiplier | 3 | Tool calls per correction attempt |
| agentic.conversion_budget_base | 4 | Base tool calls |
| conversion.max_chunk_self_corrections | 2 | C3/C4 self-correction attempts |
| conversion.query_mode_budget | 4 | Tool calls per query in query mode |
| conversion.query_mode_max_retries | 1 | Single retry for query conversion |
| validation.sample_row_count | 200 | Rows to sample per table |
| validation.scoring.row_deviation_fail | 0.30 | 30% deviation → FAIL |
| history_compaction.trigger_threshold_pct | 0.70 | Compact at 70% of token budget |
| history_compaction.keep_last_n_turns | 5 | Always keep last 5 turns verbatim |

---

## Adapter System

Adapters completely parameterise a source dialect. Required fields:

```json
{
  "dialect_id": "oracle",
  "dialect_display": "Oracle PL/SQL (11g-23c)",
  "proc_boundary": {
    "start_patterns": ["CREATE\\s+OR\\s+REPLACE\\s+PROCEDURE ...", "..."],
    "start_pattern": "combined regex used by extract_structure.py",
    "end_pattern": "END\\s+\\w*\\s*;",
    "statement_terminator": "^\\s*/\\s*$",
    "name_capture_group": 1
  },
  "cursor_pattern": "\\bCURSOR\\b",
  "exception_pattern": "\\bEXCEPTION\\b",
  "transaction_patterns": { "begin": "\\bBEGIN\\b", "end_block": "\\bEND\\b" },
  "dynamic_sql_patterns": ["EXECUTE\\s+IMMEDIATE"],
  "dialect_functions": ["NVL", "DECODE", "SYSDATE", "..."],
  "trino_mappings":  {"NVL": "COALESCE", "SYSDATE": "CURRENT_DATE", "...": "..."},
  "pyspark_mappings": {"NVL": "F.coalesce()", "SYSDATE": "F.current_date()", "...": "..."},
  "type_mappings": {
    "NUMBER": "DECIMAL", "VARCHAR2": "VARCHAR", "DATE": "TIMESTAMP",
    "CLOB": "VARCHAR", "BLOB": "VARBINARY", "BOOLEAN": "BOOLEAN", "...": "..."
  },
  "unsupported_constructs": ["CONNECT BY", "MODEL CLAUSE", "AUTONOMOUS_TRANSACTION"],
  "temp_table_pattern": "TMP_|TEMP_"
}
```

`type_mappings` maps source data types to Trino equivalents. Used by A2 (column-level analysis prompt) and injected into the Conversion Agent prompt as `DATA TYPE MAPPINGS`. This enables precise type-aware conversion — the LLM knows that `NUMBER(15,2)` should become `DECIMAL(15,2)` in Trino.

A mapping of null in `trino_mappings` means the construct is UNMAPPED — the LLM inserts a TODO comment instead of guessing.

**Available adapters:** oracle.json (30 type mappings, 50+ fn mappings), db2.json (25 types), sqlserver.json (29 types), teradata.json (23 types), generic.json (22 types). The A0 LLM selects from these via tool call.

---

## Conversion Strategies

| Strategy | When selected | Output |
|----------|---------------|--------|
| TRINO_SQL | Simple aggregations, no cursors. Forced in query mode | .sql with Trino SQL |
| PYSPARK_DF | Cursor loops, row-by-row DML | .py with DataFrame API |
| PYSPARK_PIPELINE | Complex multi-step transforms | .py with pipeline structure |
| MANUAL_SKELETON | unmapped > 6, NEEDS_MANUAL score | Scaffolded with TODO markers |

---

## Loop Guards and Failure Handling

### Proc Mode

```
Chunk fails
  └─ retry_count < max_chunk_retry (2)?
       YES → retry same chunk
       NO  → trigger replan

Replan triggered
  └─ replan_count < frozen_after (3)?
       YES → Planning Agent targeted replan, reset proc to PENDING
       NO  → FREEZE proc

Proc FROZEN
  └─ Write developer_feedback_{proc}.json
     Orchestrator polls for feedback_resolved_{proc}.json
```

If a callee proc is FROZEN, all dependent caller procs are also immediately FROZEN.

### Query Mode

No retry, no replan. Failed queries are marked PARTIAL immediately. The pipeline continues to the next query.

---

## Human Feedback Loop

FROZEN proc bundle (developer_feedback_{proc}.json) contains:
- manifest_entry: line count, cursor/dynamic SQL flags
- complexity_entry: score + rationale
- chunk_codes: all converted code that failed
- validation_reports: last validation result with fail_reason
- error_history: all errors with types and attempts
- manual_review_items: every UNMAPPED TODO with line number

Resolution actions:

| Action | Pipeline effect |
|--------|----------------|
| replan_with_notes | Proc reset to PENDING, Planning re-runs P1 with notes |
| manual_rewrite_provided | Developer code written directly, jumps to validation |
| override_pass | Marked PARTIAL, pipeline continues |
| mark_skip | Marked SKIPPED, downstream callers unblocked |

---

## Validation System

**Level 1 STATIC_ONLY**: Tables missing/empty. Syntax check only.

**Level 2 SCHEMA_PLUS_EXEC**: Tables present, no sample data files. Dry-run against synthetic/sampled rows using local Spark. All df.write calls intercepted by _WriteShim — nothing touches Trino.

**Level 3 FULL_RECONCILIATION**: Tables have data + README Section 6 sample files. Full dry-run + semantic diff: LLM generates equivalent Trino SQL, MCP runs it against real data, column schemas compared.

**Scoring rules:**
- FAIL: runtime error, missing column, row deviation > 30%
- PARTIAL: any UNMAPPED TODOs, level < 3, type mismatches, row deviation 10-30%
- PASS: everything else

---

## History Compaction

Incremental rolling-summary compaction (Anthropic Claude Code + Factory.ai anchor pattern):

1. Track token usage after each LLM call
2. At 70% of context budget: trigger compaction
3. Keep last 5 turns verbatim (anchor turns)
4. Summarise only newly-dropped span using gemini-2.0-flash
5. Replace old tool results with [cleared -- artifact: path]
6. Never re-summarise existing summaries

---

## Streamlit UI Guide

```bash
sql-migrate ui   # Launch at http://localhost:8501
```

**Submit Job**: Upload README.docx + SQL files + optional CSVs. Pipeline preview shows all agent steps before submission.

**Pipeline Monitor**: Live proc status with auto-refresh every 3 seconds. Agent Steps expander shows complete step reference. Per-proc drill-down: errors, validation result, converted code preview.

**Frozen Procs**: 4-tab review interface per FROZEN proc. Why It Failed / Converted Code / UNMAPPED Constructs (with AI Assist button) / Submit Resolution.

**Artifacts**: File browser grouped by agent. JSON preview with envelope unwrapping.

---

## CLI Reference

```bash
sql-migrate run --readme README.docx --sql procs.sql --run-id my_run_001
sql-migrate run --job-config jobs/my_run_001/job_config.json
sql-migrate run --readme README.docx --sql procs.sql --procs "sp_calc_interest,generate_report"
sql-migrate run --readme README.docx --sql procs.sql --dry-run
sql-migrate status --run-id my_run_001
sql-migrate ui
sql-migrate ui --port 8502
```

---

## Integration Tests

```bash
pytest tests/test_integration.py -v
```

7 sequential tests in ~18s using a shared temporary workspace. Real sandbox scripts on host, mock LLM + mock MCP.

| Test | What it covers |
|------|----------------|
| test_analysis_setup_and_extraction | A0: README parsed, adapter selected, boundaries extracted |
| test_analysis | A2-A4: procs analyzed per-proc, tables resolved, graph built |
| test_planning | P1-P4: strategies selected, loop guards baked in |
| test_conversion | C1-C5: chunk converted, assembled file written |
| test_validation | V1-V5: validation result written |
| test_orchestrator_full_run | All procs reach terminal state |
| test_checkpoint_crash_recovery | Resume → zero re-work |

---

## v10 Changelog

### Task 1: Detection Agent Removal
Detection Agent (D2-D6) removed entirely. Analysis Agent A0 phase absorbs adapter selection via LLM tool call. Pipeline reduced from 6 stages to 5: Analysis → Dep Gate → Planning → Preflight → Orchestration. All 5 adapter JSONs enriched to 50+ function mappings.

### Task 2: Per-Proc LLM Analysis
Replaced single giant 80-tool-call LLM session with sequential per-proc analysis. Each proc's full body sent to LLM individually with rolling summary. LLM classifies tables as REAL_TABLE/CTE_ALIAS/CURSOR_NAME/VARIABLE/VIEW — zero CTE false positives. Per-proc persistence for crash-resume. Individual proc .sql files extracted for debugging.

### Task 3: Query Mode + Column-Level Type Analysis
Added `source_type: sql_queries` README field. Query mode bypasses proc-specific complexity at every stage. Added `type_mappings` to all adapters (Oracle NUMBER → DECIMAL, etc). A2 prompt now requests column-level function analysis. New `column_analysis.json` artifact. Conversion prompt enhanced with DATA TYPE MAPPINGS and COLUMN ANALYSIS blocks.

### Bug Fixes
- AgenticConfig added to config (was crashing every conversion)
- Remnant detection skips comments (was infinite rejection loop)
- C5 chunk reload from disk on crash-resume (was silent data loss)
- Podman path translation layer (host paths → /source/, /artifacts/)
- README mount for files outside source_dir
- Missing table fail-fast with detailed error
- README mandatory field validation
- Dead `_histories` dict removed
- `per_proc_timeout_seconds` removed (global timeout applies)
- `chunking_hint` wired from A2 through ProcEntry to Planning
- `temp_tables` property wired into A3
- `context_fixes.py` uses `_chunk_codes` instead of removed `_histories`

---

## Environment Variables

| Variable | Required | Purpose |
|----------|----------|---------|
| GEMINI_API_KEY | Yes | Primary LLM |
| ANTHROPIC_API_KEY | No | Alternative model / UI AI Assist |
| TRINO_MCP_SERVER_URL | Yes | Trino MCP server URL |
| FEEDBACK_WEBHOOK_URL | No | Slack/Teams webhook for FROZEN notifications |
| SQL_MIGRATION_DRY_RUN | No | Set true to mock all LLM calls |
| SANDBOX_IMAGE | No | Override Podman image name |
| SANDBOX_DISABLED | No | Set true to run scripts directly (testing) |
| ARTIFACT_STORE_PATH | No | Override artifact store root |

---

## License

Internal tool. Not for external distribution.
