# Enhancement: Target Catalog Routing for Migrated Writes

## Context

Today the migration pipeline resolves every table referenced by an Oracle / DB2 / SQL Server / Teradata procedure against a single source Trino catalog (Starburst lakehouse) and emits converted PySpark / Trino SQL that uses bare or source-qualified table names for both reads and writes. Production needs this split:

- **Source tables (reads)** stay on the existing source catalog — status quo.
- **Target tables (writes — INSERT / UPDATE / DELETE / MERGE / CTAS / `saveAsTable` / `insertInto`)** must be routed to a configurable Starburst destination, defaulting to catalog `lz_lakehouse`, schema `lm_target_schema`. The schema and tables must be created if missing, with column types translated through the adapter's `type_mappings`. Converted code must reference the target FQN exactly.

Read-vs-write classification already exists in `TableRef.used_as` (analysis), and `ReadmeSignals.output_catalog` / `output_schema` are parsed but unused in resolution. The MCP client is intentionally read-only, so target creation requires a separate, narrowly-scoped admin client. No DDL generator exists today.

User-confirmed decisions:
1. Add a new `TrinoAdminClient` whitelisted to `CREATE SCHEMA/TABLE IF NOT EXISTS` against the configured target catalog/schema.
2. Tiered fallback for target column schema: paired source schema → A2 LLM-declared write columns → README Section 8 hint → fail loudly.
3. Config default (`lz_lakehouse` / `lm_target_schema`), overridable per-run by README Section 4.1.
4. Strict allow-list enforcement of write FQNs in C3.

---

## Implementation Steps

### 1. Config (`config/config.yaml`, `core/config_loader.py`)

Add block:
```yaml
target_routing:
  catalog: lz_lakehouse
  schema: lm_target_schema
  create_if_missing: true
  fail_if_exists_with_different_schema: true
  naming_strategy: passthrough     # passthrough | prefix
  prefix: ""
  admin_url_env: TRINO_ADMIN_URL
  admin_user_env: TRINO_ADMIN_USER
  table_format: PARQUET
```

In `src/sql_migration/core/config_loader.py`, add `TargetRoutingConfig` dataclass + `get_target_routing()`. Apply README override: if `ReadmeSignals.output_catalog` is non-empty, use it; if `output_schema[0]` is non-empty, use it. Resolution helper `effective_target(readme, cfg) -> (catalog, schema)` lives in same module so all agents call one place.

### 2. Models (`src/sql_migration/models/common.py`, `models/analysis.py`)

Extend `TableRegistryEntry` (keep `trino_fqn` for back-compat — Pydantic validator copies it into `source_fqn` and defaults `role="source"` for legacy artifacts):
```python
role: Literal["source","target","both"] = "source"
target_catalog: Optional[str] = None
target_schema: Optional[str] = None
target_fqn: Optional[str] = None
target_status: Literal["EXISTS","NEEDS_CREATE","CREATED","CREATE_FAILED","SKIPPED"] = "EXISTS"
target_ddl: Optional[str] = None
target_columns: Optional[list[ColumnSpec]] = None
target_create_failure_reason: Optional[str] = None
```
Add new types: `ColumnSpec(name, trino_type, nullable, source_type)` and `TargetTableSpec(target_fqn, columns, source_table_ref, ddl_sql)`.

Extend `ChunkInfo` (`models/common.py` ~line 120) with `target_context: dict[str, TargetTableSpec] = {}`.

In `models/analysis.py`, add `TableRegistry.source_tables()` and `target_tables()` filter helpers.

Extend `PerProcAnalysis` (`models/analysis.py` lines 110–176) with `declared_write_columns: dict[str, list[ColumnSpec]] = {}` so A2 can capture explicit `INSERT INTO X (col1, col2) ...` projections.

### 3. A2 prompt extension (`agents/analysis/agent.py` ~line 200+)

Add a section to the per-proc analysis prompt asking the LLM to extract, for every table where `used_as in {write, both}`, the column list visible in `INSERT INTO` clauses or implied by the `SELECT` projection of an `INSERT … SELECT`. Returned in `declared_write_columns`. Already passes through `PerProcAnalysis` per-proc persistence so resume keeps it. Falls back silently to empty dict for procs the LLM can't analyze.

### 4. A3 role-aware resolution (`agents/analysis/agent.py` A3 step)

- Aggregate `used_as` across all procs per real table → role.
- For role in {target, both}: compute `mapped = readme.table_mapping.get(name.upper(), name)`; apply `naming_strategy.prefix`; `target_fqn = f"{cat}.{schema}.{mapped}"`. Call `mcp.desc_table(target_fqn)`. On hit → set `target_status=EXISTS`, capture columns. On miss → `target_status=NEEDS_CREATE` (NOT fatal — current source-side fail-fast rule unchanged for source-only tables).
- For role=both: resolve source schema as today AND set target_fqn / NEEDS_CREATE.
- Persist into `00_analysis/table_registry.json` via existing artifact_store helpers (atomic write).

### 5. Type translation (`src/sql_migration/deterministic/type_translator.py` — new)

```python
def translate(source_type: str, mappings: dict[str, str]) -> str
def build_column_spec(src_col: dict, mappings: dict) -> ColumnSpec
```
Single source of truth = adapter `type_mappings` already loaded into the adapter context. Handles parametric types (`NUMBER(15,2)` → `DECIMAL(15,2)`); pure deterministic, no LLM.

### 6. Preflight: target DDL plan + creation (new agent)

**New file:** `src/sql_migration/agents/preflight/target_prep.py`
```python
def build_target_ddl_plan(registry, adapter, cfg) -> list[TargetTableSpec]
def write_plan_artifact(plan, run_id) -> Path   # 01_planning/target_ddl_plan.json
def apply_plan(plan, admin_client, cfg) -> TargetReadyReport   # 02_orchestrator/target_tables_ready.json
```
Column resolution order per target table:
1. Paired source-table schema (when role=both, or when an INSERT-SELECT references a known source).
2. `PerProcAnalysis.declared_write_columns` (A2 extraction).
3. README Section 8 explicit DDL hint.
4. Otherwise → `target_status=CREATE_FAILED`, `target_create_failure_reason="no_schema_source"`. Preflight surfaces all such tables in the report and returns non-zero (pipeline halts unless `create_if_missing=false`).

**New file:** `sandbox_scripts/preflight/generate_target_ddl.py` — deterministic emitter:
`CREATE TABLE IF NOT EXISTS {fqn} ({cols}) WITH (format='PARQUET')`. Runs in the existing Podman sandbox.

### 7. Trino admin client (`src/sql_migration/core/trino_admin.py` — new)

```python
class TrinoAdminClient:
    def __init__(self, url, user, allowed_catalog, allowed_schema, table_format)
    def ensure_schema(self) -> None         # CREATE SCHEMA IF NOT EXISTS
    def ensure_table(self, ddl: str) -> None  # CREATE TABLE IF NOT EXISTS
    def _guard(self, sql: str) -> None      # regex whitelist
```
Uses the `trino` python client (add to `pyproject.toml` deps). `_guard` allows ONLY `^\s*CREATE (SCHEMA|TABLE) IF NOT EXISTS\s+lz_lakehouse\.lm_target_schema(\.|\s)` (parameterised by configured target). Any other statement raises. New env vars `TRINO_ADMIN_URL`, `TRINO_ADMIN_USER`. **MCP read-only invariant untouched** — `mcp_client.py` is not modified.

### 8. Orchestrator wiring (`src/sql_migration/agents/orchestrator/agent.py`)

After A3 completes, before chunking/conversion dispatch:
1. `target_prep.build_target_ddl_plan(...)` → write artifact.
2. If `create_if_missing=false`: stop after writing plan; emit warning; mark all NEEDS_CREATE as SKIPPED for downstream filtering.
3. Else: instantiate `TrinoAdminClient`, call `ensure_schema()`, then `ensure_table()` per spec. Update registry entries to `CREATED` or `CREATE_FAILED`. Write `02_orchestrator/target_tables_ready.json`.
4. If any `CREATE_FAILED`: freeze affected procs (reuse existing FROZEN handling) with `developer_feedback_{proc}.json` listing the missing target tables.
5. Checkpoint (`02_orchestrator/checkpoint.json`) so a re-run with the same `--run-id` skips already-CREATED tables.

### 9. P3 schema + target context assembly (`agents/planning/agent.py` `_p3_fetch_schemas()`)

For every chunk:
- Source schemas: existing `mcp.desc_table` path for read tables.
- Target schemas: lookup `TableRegistryEntry.target_fqn` + `target_columns` for each write table referenced in the chunk; populate `chunk.target_context: dict[bare_name -> TargetTableSpec]`.

### 10. Conversion prompt (`src/sql_migration/deterministic/prompt_builder.py`)

Add `_format_target_context(target_context)`:
```
TARGET TABLE LOCATIONS (every write MUST use these exact FQNs):
  ORDERS -> lz_lakehouse.lm_target_schema.ORDERS
    columns: id BIGINT, amount DECIMAL(18,2), order_date TIMESTAMP, ...
  CUSTOMERS -> lz_lakehouse.lm_target_schema.CUSTOMERS
    columns: ...
```
Render inside `build_full_prompt()` and `build_repair_prompt()` (lines 28–136) right after `_format_schema_context`. Strengthen the existing write-directive: "Every INSERT / UPDATE / DELETE / MERGE / CTAS / `saveAsTable` / `insertInto` MUST reference one of the FQNs listed above; do not shorten, do not substitute, do not invent." For repair prompts, surface the C3 violation list verbatim.

### 11. C3 strict allow-list (`src/sql_migration/core/trino_sql_validator.py`)

Add `validate_write_targets(converted: str, allowed_fqns: set[str], language: Literal["sql","pyspark"]) -> list[Violation]`. Regex-scan for:
- SQL: `INSERT INTO`, `UPDATE`, `DELETE FROM`, `MERGE INTO`, `CREATE TABLE … AS`.
- PySpark: `.saveAsTable(...)`, `.insertInto(...)`, `.write...save(...)`, `.writeTo(...)`.

Reject the chunk on any target outside `allowed_fqns`. C3 failure feeds the existing self-correction loop (max 2 attempts) — repair prompt re-emphasizes `TARGET TABLE LOCATIONS`. After max retries → standard replan/freeze path.

### 12. LEVEL_2 dry-run write shim (`sandbox_scripts/validation/run_pyspark.py`)

Add `_WriteShim` (currently absent): monkey-patch `DataFrameWriter.saveAsTable`, `insertInto`, `save`, `writeTo`. When the table matches `lz_lakehouse.lm_target_schema.*`, redirect to local parquet at `06_samples/{proc}/target_{table}.parquet`. Sandbox is already `--network=none`; the shim guarantees semantic success without needing real Trino. LEVEL_3 unchanged — if real reconciliation is requested, MCP read-only path validates row counts post-execution.

### 13. Query mode (`agents/analysis/agent.py` query path)

Same role detection logic. Pure SELECT → no preflight, no target context. CTAS / INSERT-SELECT → identical full path (preflight, prompt block, C3 enforcement). Existing `query_mode_budget=4` retained.

### 14. Dependency

Add `trino>=0.330.0` to `pyproject.toml` `[project] dependencies`. Already have `httpx` if a thin HTTP wrapper is preferred, but the `trino` client is far simpler for SSO/JWT-style auth common in Starburst.

### 15. Tests (`tests/test_integration.py`)

Add cases:
- `test_target_table_detection`: A2/A3 mark `INSERT INTO ORDERS` as role=target with target_fqn populated.
- `test_target_ddl_plan_generated`: `01_planning/target_ddl_plan.json` exists with translated types (Oracle `NUMBER(15,2)` → Trino `DECIMAL(15,2)`).
- `test_prompt_target_block`: built prompt contains `TARGET TABLE LOCATIONS` for chunks with writes.
- `test_c3_rejects_unqualified_write`: planted `INSERT INTO ORDERS` (no FQN) → C3 returns Violation; repair loop kicks in.
- `test_readme_overrides_config_target`: README `output_catalog=alt_catalog` wins over `lz_lakehouse` default.
- `test_back_compat_legacy_registry`: load old `table_registry.json` with only `trino_fqn` → validator fills `role="source"`, run resumes.
- Mock the new `TrinoAdminClient` in tests; do NOT add a real Trino dep to test fixtures.

### 16. Docs

Update `README.md` (Architecture diagram adds a Preflight box; Adapter System notes `type_mappings` is now also used for target DDL; Artifact Store adds `01_planning/target_ddl_plan.json` and `02_orchestrator/target_tables_ready.json`). Update `CLAUDE.md` with target-routing concept, `trino_admin.py` location, allow-list enforcement convention.

---

## Critical Files (Modify or Add)

**Modify:**
- `config/config.yaml`
- `src/sql_migration/core/config_loader.py`
- `src/sql_migration/models/common.py`  (TableRegistryEntry, ChunkInfo, ColumnSpec, TargetTableSpec)
- `src/sql_migration/models/analysis.py`  (PerProcAnalysis.declared_write_columns, TableRegistry helpers)
- `src/sql_migration/agents/analysis/agent.py`  (A2 prompt extension, A3 role-aware resolution)
- `src/sql_migration/agents/planning/agent.py`  (`_p3_fetch_schemas` populates target_context)
- `src/sql_migration/agents/orchestrator/agent.py`  (preflight invocation between A3 and dispatch)
- `src/sql_migration/deterministic/prompt_builder.py`  (`_format_target_context`, write directives)
- `src/sql_migration/core/trino_sql_validator.py`  (validate_write_targets)
- `sandbox_scripts/validation/run_pyspark.py`  (`_WriteShim`)
- `pyproject.toml`  (trino client dep)
- `tests/test_integration.py`
- `README.md`, `CLAUDE.md`

**Add:**
- `src/sql_migration/core/trino_admin.py`
- `src/sql_migration/deterministic/type_translator.py`
- `src/sql_migration/agents/preflight/target_prep.py`
- `sandbox_scripts/preflight/generate_target_ddl.py`

---

## Reuse (do not duplicate)

- `core/artifact_store.py` atomic-write helpers — for every new artifact.
- Adapter `type_mappings` from `adapters/{dialect}.json` — single source of truth for type translation; `type_translator.py` only formats, never invents mappings.
- `PerProcAnalysis.real_tables_written()` / `real_tables_read()` (`models/analysis.py` lines 160–167) — already filters by `used_as`; reuse instead of re-implementing role aggregation.
- Existing FROZEN proc + developer-feedback flow — used to surface preflight failures rather than inventing a new error channel.
- `mcp_client.desc_table` for target existence check — read-only path.

---

## Risks and Mitigations

1. **Target table with no derivable schema** → tiered fallback (paired source / A2-extracted columns / README hint); if all four fail, mark `CREATE_FAILED` and halt preflight with a clear actionable report. Never generate guessed DDL.
2. **MCP read-only invariant** → completely preserved. All DDL goes through `TrinoAdminClient` with regex whitelist hard-bound to the configured catalog/schema; `mcp_client.py` untouched.
3. **Legacy artifact compatibility** → Pydantic model validator on `TableRegistryEntry` defaults `role="source"` and copies legacy `trino_fqn` forward. Existing in-flight runs resume cleanly.
4. **Permission/auth for admin client** → read env vars at construction; fail fast with a clear error if missing AND `create_if_missing=true`. `create_if_missing=false` lets users still run the pipeline in report-only mode without admin creds.
5. **Naming collision** (table `ORDERS` exists in `lz_lakehouse.lm_target_schema` with a different schema) → `fail_if_exists_with_different_schema: true` triggers preflight failure instead of silently writing into a mismatched table.
6. **Sandbox attempting real network write** → `_WriteShim` + existing `--network=none` give belt-and-braces protection.

---

## Verification

End-to-end:
1. `pip install -e ".[dev]"` (pulls new `trino` dep). Set `TRINO_ADMIN_URL` / `TRINO_ADMIN_USER` in `config/.env`.
2. Drop a sample Oracle proc with `INSERT INTO orders SELECT … FROM source.txn` plus an UPDATE on `customers` into a fresh test fixture.
3. `sql-migrate run --readme test_readme.docx --sql sample_proc.sql --run-id verify_target_001`.
4. Inspect `artifacts/verify_target_001/00_analysis/table_registry.json` — `orders` and `customers` rows have `role` ∈ {target, both}, `target_fqn` populated.
5. Inspect `artifacts/verify_target_001/01_planning/target_ddl_plan.json` — DDL strings present, types translated via adapter mapping (e.g., `NUMBER(15,2)` → `DECIMAL(15,2)`).
6. Inspect `artifacts/verify_target_001/02_orchestrator/target_tables_ready.json` — every entry `CREATED` or `EXISTS`.
7. Inspect `artifacts/verify_target_001/03_conversion/proc_*.{py,sql}` — all writes use `lz_lakehouse.lm_target_schema.*`.
8. Manually plant `INSERT INTO orders` (no FQN) into a chunk's expected output via mock; confirm C3 rejects and repair re-runs.
9. Confirm via Starburst UI that `lz_lakehouse.lm_target_schema.orders` and `.customers` exist with the expected columns.
10. `pytest tests/test_integration.py -v` — all existing tests + 6 new tests pass.
11. Re-run `sql-migrate run … --run-id verify_target_001` (resume): preflight skips already-CREATED tables, no DDL re-issued.
12. Toggle `target_routing.create_if_missing: false` + delete the schema, re-run: pipeline produces `target_ddl_plan.json`, halts cleanly with actionable report, no admin client calls made.
