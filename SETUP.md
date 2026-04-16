# Setup Guide

Step-by-step instructions to get the SQL Migration System running from scratch.

---

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Python | 3.10+ | 3.11 / 3.12 also work fine |
| Podman | 4.0+ | Docker also works — change `sandbox.runtime` in config.yaml |
| Java | 11 or 17 | Required by PySpark inside the sandbox container |
| Git | any | For cloning |

Check your versions:

```bash
python --version       # Python 3.10.x / 3.11.x / 3.12.x
podman --version       # podman version 4.x or 5.x
java -version          # openjdk 11 or 17
```

---

## Step 1 — Clone and Install

```bash
# Clone the repo
git clone https://your-git-server/sql-migration.git
cd sql-migration

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate          # Linux/macOS
# .venv\Scripts\activate           # Windows

# Install with dev extras
pip install -e ".[dev]"

# Verify the CLI entry point works
sql-migrate --help
```

Expected output:
```
Usage: sql-migrate [OPTIONS] COMMAND [ARGS]...

  SQL Migration System -- migrate Oracle/DB2/Teradata SQL to PySpark/Trino.

Options:
  --help  Show this message and exit.

Commands:
  run     Run the full migration pipeline.
  status  Show current pipeline status for a run.
  ui      Launch the Streamlit monitoring UI.
```

---

## Step 2 — Configure Secrets

```bash
cp config/.env.example config/.env
```

Open `config/.env` and fill in:

```bash
# REQUIRED: Enterprise LLM Proxy (primary setup)
# Base URL of your LiteLLM proxy (up to /v1 — NOT including /chat/completions)
LLM_API_BASE=https://genai-proxy-uat.your-org.com/UAT/litellm/v1
# Bearer token for the proxy
LLM_API_KEY=sk-your-proxy-key

# REQUIRED: Trino MCP Server
# URL of your mcp-trino server (e.g. http://localhost:8080/mcp)
TRINO_MCP_SERVER_URL=http://your-trino-mcp-server:8080/mcp

# ALTERNATIVE: Direct API keys (only if NOT using an enterprise proxy)
# GEMINI_API_KEY=your_gemini_api_key_here
# ANTHROPIC_API_KEY=your_anthropic_api_key_here

# OPTIONAL: Notification webhook for FROZEN proc alerts
FEEDBACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz
```

**Enterprise proxy setup (recommended):**
The system is designed for enterprise LLM proxies (LiteLLM, Azure OpenAI, etc.).
Set `LLM_API_BASE` to your proxy URL. The proxy handles auth, rate limiting,
and model routing. `config.yaml` uses bare model names (e.g. `gemini-2.5-pro`)
and the proxy resolves them to the correct backend.

In `config.yaml`, `ssl_verify: false` is already set for internal certificate proxies.

**Direct API key setup (alternative):**
If not using a proxy, leave `LLM_API_BASE` empty in `.env` and set your
provider's API key directly (e.g. `GEMINI_API_KEY`). Update `config.yaml`:
```yaml
llm:
  api_base: ""           # Empty = LiteLLM uses default provider routing
  api_key_env: "GEMINI_API_KEY"
  ssl_verify: true
```

---

## Step 3 — Review config.yaml

The `config/config.yaml` file contains all tunable parameters with comments. Defaults are production-ready. Key things to review for your environment:

```yaml
llm:
  primary_model: gemini-2.5-pro   # Bare name — proxy resolves to backend
  # Per-agent overrides (leave empty to use primary_model):
  agent_models:
    conversion: ""   # e.g. "gemini-2.5-flash" for faster conversion

mcp:
  server_url: "${TRINO_MCP_SERVER_URL}"   # Resolved from .env
  schema_probe_query: "SELECT * FROM {table} LIMIT 0"

paths:
  artifact_store: ./artifacts     # Change to shared NAS path for team use
  adapters_dir: ./adapters
  logs_dir: ./logs

# v9: These operational thresholds are all config-driven:
dependency_gate:
  cli_auto_action: stub_todo   # "stub_todo" | "abort" | "skip_callers"

preflight:
  critical_threshold: 50   # Risk score >= this → CRITICAL
  high_threshold: 30
  medium_threshold: 15

global_error_aggregator:
  cross_proc_threshold: 3   # Circuit breaker trips after 3 procs show same error
  max_failed_procs: 5       # Hard pause after 5 procs fail
```

---

## Step 4 — Build the Sandbox Container

The sandbox container runs all deterministic processing (SQL parsing, structural extraction, code validation, dry-run execution). Build it once; scripts are mounted at runtime so you don't need to rebuild when scripts change.

```bash
podman build -t sql-migration-sandbox:latest -f Dockerfile.sandbox .
```

This will take 3–5 minutes on first run (downloads Python, OpenJDK, PySpark, etc.).

Verify the build:
```bash
podman run --rm sql-migration-sandbox:latest \
  python3 -c "import sqlglot, networkx, pandas, pyspark; print('Sandbox OK')"
```

Expected: `Sandbox OK`

**If you're using Docker instead of Podman:**

Edit `config/config.yaml`:
```yaml
sandbox:
  runtime: docker    # default is podman
```

---

## Step 5 — Verify MCP Connection

The MCP server provides read-only access to your Trino lakehouse. Verify it's reachable:

```bash
python -c "
import sys; sys.path.insert(0, 'src')
from sql_migration.core.mcp_client import MCPClientSync
mcp = MCPClientSync()
tables = mcp.list_tables('your_catalog.your_schema')
print(f'Connected. Found {len(tables)} tables.')
"
```

If connection fails, check:
- `TRINO_MCP_SERVER_URL` is correct in config/.env
- MCP server is running and accessible from your machine
- Firewall rules allow the connection

---

## Step 6 — Fill in the README Template

The README.docx is the primary signal for dialect detection. Fill it in before running your first migration.

Download the template from the `docs/` folder (or ask your team lead for the current version). Fill in all 8 sections:

| Section | What to fill in | Why it matters |
|---------|----------------|----------------|
| 1. Tech Stack | Database vendor, version, client tools | Helps tiebreak dialect |
| **2. Proc Declaration Syntax** | Exact pattern + end pattern + example | Drives proc boundary detection |
| **3. Custom Functions** | Your org's custom functions + Trino equivalents | Reduces UNMAPPED constructs |
| 4. Transaction Patterns | BEGIN/COMMIT/ROLLBACK syntax | Used by chunk boundary computation |
| **5. Sample Proc** | A representative 30-50 line proc | Used to validate the adapter (D6) |
| **6. Sample Data** | Input CSV + expected output CSV | Enables LEVEL_3 validation |
| 7. Unsupported Constructs | Anything that can't migrate | Pre-flags MANUAL_SKELETON procs |
| 8. Table Mapping | Old name -> Trino FQN for renamed tables | Prevents MISSING table status |

**Sections 2, 3, 5, and 6 have the most impact on migration quality.** A well-filled README can bring README quality score above 0.80, which skips the LLM classification step (D5) entirely and makes dialect detection fully deterministic.

README quality scoring (shown in logs after D1):
- `readme_quality_score: 0.85` → D5 skipped, README_PRIMARY resolution
- `readme_quality_score: 0.60` → LLM used for classification, LLM_PRIMARY resolution
- `readme_quality_score: < 0.30` → FALLBACK, generic adapter, warning emitted

---

## Step 7 — Run Your First Migration

```bash
sql-migrate run \
  --readme path/to/README.docx \
  --sql path/to/procedures.sql \
  --run-id test_run_001
```

The system will print stage-by-stage progress:

```
⚙️  SQL Migration — Run: test_run_001

🔍  Stage 1/5 — Dialect Detection & Adapter Generation...
     Dialect: oracle  (confidence: 0.94  resolution: README_PRIMARY)
🔬  Stage 2/5 — Intelligent Analysis (LLM-driven)...
     Procs found: 23  Lines: 4821
📐  Stage 3/5 — Planning...
     Total chunks: 31
🎯  Stage 4-5/5 — Orchestrating conversion + validation...

──────────────────────────────────────────────────
✅  Migration complete — Run ID: test_run_001
   Validated : 18
   Partial   : 3
   Frozen    : 2
   Skipped   : 0
   Artifacts : ./artifacts/test_run_001
```

Check status anytime with:
```bash
sql-migrate status --run-id test_run_001
```

---

## Step 8 — Launch the UI

```bash
sql-migrate ui
# Opens at http://localhost:8501
```

Or with a custom port:
```bash
sql-migrate ui --port 8502
```

Navigate to:
- **📊 Pipeline Monitor** → enter run ID → watch live progress
- **🔴 Frozen Procs** → review and resolve any FROZEN procedures

---

## Step 9 — Handle Frozen Procs

Frozen procs have exhausted all automatic retry and replan cycles. Open the UI → 🔴 Frozen Procs and for each proc:

1. Read the **Why It Failed** tab — check error history and validation report
2. Look at the **Converted Code** tab — see what was generated
3. If there are UNMAPPED constructs, check the **UNMAPPED Constructs** tab and use the AI Assist button to get translation suggestions
4. Choose a **resolution action**:
   - **Replan with notes** — tell the Planning Agent what strategy to try (e.g. "Use PYSPARK_PIPELINE and implement hierarchical traversal with recursive DataFrame joins")
   - **Manual rewrite** — paste your own corrected code
   - **Override pass** — accept as partial output
   - **Mark skip** — exclude from migration

After submitting a resolution, the Orchestrator picks it up on its next poll cycle (every 2 seconds by default) and the proc resumes.

---

## Step 10 — Review Output Artifacts

Converted files are at `artifacts/04_conversion/`:

```bash
ls artifacts/04_conversion/
# proc_sp_calc_interest.py
# proc_generate_report.sql
# conversion_log_sp_calc_interest.json
# ...
```

Validation results are at `artifacts/05_validation/`:

```bash
cat artifacts/05_validation/validation_sp_calc_interest.json | python -m json.tool
# {
#   "proc_name": "sp_calc_interest",
#   "outcome": "VALIDATED",
#   "validation_level": 3,
#   "row_deviation_pct": 0.02,
#   ...
# }
```

Any UNMAPPED constructs needing review are at `artifacts/05_validation/manual_review_{proc}.json`.

---

## Configuration for Large Migrations (100+ procs)

For large batches, tune these settings in config.yaml:

```yaml
run:
  # Process a subset first to validate setup
  procs_filter: ["sp_calc_interest", "generate_report"]

llm:
  # Increase timeout for large complex procs
  timeout_seconds: 180

planning:
  # Smaller chunks = more LLM calls but better quality
  chunk_target_lines: 150

sandbox:
  # More CPUs for faster dry-run validation
  timeout_seconds: 600

# Tune the circuit breaker for large batches
global_error_aggregator:
  cross_proc_threshold: 5   # More tolerance before tripping
  max_failed_procs: 20      # Allow more failures before hard pause

# Adjust conversion budget for complex procs
conversion:
  max_chunk_self_corrections: 3   # More retries per chunk (budget = 3×5+10 = 25)
```

For a batch of 200+ procs, consider running in stages:
```bash
# Stage 1: Simple procs first
sql-migrate run --readme README.docx --sql all_procs.sql \
  --procs "sp_simple_1,sp_simple_2,sp_simple_3" \
  --run-id batch_001_simple

# Stage 2: Complex procs
sql-migrate run --readme README.docx --sql all_procs.sql \
  --procs "sp_complex_cursor,sp_hierarchical" \
  --run-id batch_001_complex
```

---

## Dry Run Mode (Testing / CI)

Run the full pipeline with all LLM calls mocked:

```bash
sql-migrate run \
  --readme README.docx \
  --sql test_procs.sql \
  --dry-run \
  --run-id ci_test_001
```

In dry-run mode:
- All LLM calls return mock responses
- Sandbox scripts still run (real file parsing, real syntax checks)
- MCP calls still run against Trino
- Useful for validating pipeline wiring without consuming API tokens

---

## Running Integration Tests

The integration test suite runs all 7 tests against a temp workspace in ~18 seconds:

```bash
pytest tests/test_integration.py -v
```

Tests use:
- Real sandbox scripts (on host, no Podman)
- Mock LLM responses (deterministic JSON fixtures)
- Mock MCP (returns 0 rows → LEVEL_1 validation)

All 7 tests should pass. If any fail, it indicates a broken import or model schema mismatch — check the error output for `ValidationError` or `ImportError`.

---

## Updating the System

### Adding a new source dialect

The Detection Agent auto-generates adapters via D5 when encountering an unknown dialect. For better quality, create a hand-crafted adapter:

```bash
cp adapters/oracle.json adapters/my_dialect.json
# Edit adapters/my_dialect.json
# Set dialect_id, proc_boundary patterns, function mappings

# Test the adapter against sample code
python sandbox_scripts/extraction/extract_signals.py \
  --args '{"mode": "validate_adapter", "source_file": "sample.sql", "adapter": {...}}'
```

### Updating sandbox scripts

Sandbox scripts are not baked into the container image. They're mounted read-only at runtime from the host `./sandbox_scripts/` directory. You can update any sandbox script and the change takes effect on the next run immediately — no container rebuild needed.

### Updating agent prompts

LLM prompts are built in the `models/` layer by the `build_llm_*_input()` functions. Edit those functions to change what context is sent to the LLM. The system is designed so that no prompt text lives in agent code — all prompt construction is centralised in the model builders.

---

## Troubleshooting

### "No proc boundaries found" after Analysis

The adapter's `proc_boundary.start_pattern` didn't match any procedures in your SQL files. Check:
1. `readme_signals.json` → Section 2 correctly parsed?
2. `adapters/{dialect}.json` → `proc_boundary.start_pattern` correct?
3. Run the extraction script directly to debug:

```bash
python sandbox_scripts/extraction/extract_structure.py \
  --args '{"sql_file_paths": ["your_file.sql"], "adapter": {...}, "procs_to_skip": [], "table_mapping": {}}'
```

### "Adapter validation failed" (D6 fails)

The adapter's `proc_boundary.start_pattern` doesn't match the sample proc in README Section 5. Either:
1. Fix the sample proc in the README (make it simpler)
2. Fix the regex pattern in the adapter seed
3. Lower the validation strictness in config.yaml: `detection.adapter_validation_threshold: 0.5`

### LLM calls timing out

Increase timeout in config.yaml:
```yaml
llm:
  timeout_seconds: 240
  retry_attempts: 5
```

For Gemini: check your quota at https://aistudio.google.com/

### Proc keeps failing with "REJECTED: Code contains unconverted dialect functions"

The agentic conversion loop's `submit_result` detected remnant dialect functions
(e.g. NVL, DECODE, SYSDATE) in the converted code. This means the LLM is copying
source-dialect constructs without converting them. Check:
1. `construct_map.json` → are the mappings correct?
2. `plan.json` → is `construct_hints` populated for this proc's chunks?
3. Try adding explicit examples to the adapter's `pyspark_mappings` / `trino_mappings`
4. To disable remnant checking (e.g. if function names collide with target identifiers):
   ```yaml
   conversion:
     sandbox_validation:
       run_remnant_check: false
   ```

### PySpark dry-run fails with "Table not found"

The parquet sample data wasn't created (V2 step failed) or the table name in the proc doesn't match the parquet filename. Check:
1. `artifacts/07_samples/{proc_name}/` — are .parquet files present?
2. Verify the table names in `table_registry.json` match what the converted code uses

### Streamlit UI shows "No checkpoint found"

Either the pipeline hasn't started yet, or it's writing artifacts to a different path. Check:
1. Run ID is correct
2. `ARTIFACT_STORE_PATH` env var (if set) points to the right directory
3. Check `artifacts/03_orchestrator/checkpoint.json` exists

---

## Architecture Decision Log

Key decisions made during design:

**Why LiteLLM instead of LangChain?**
LangChain's loop/agent model fights this architecture. We need explicit, inspectable control over every LLM call, conversation history, and tool invocation. LiteLLM is a thin API wrapper that gives unified model access without imposing an agent framework.

**Why Podman for sandbox execution?**
Network isolation (`--network=none`) is non-negotiable for safety — sandbox scripts must not be able to exfiltrate code or make outbound network calls. Podman's rootless containers are also easier to run in enterprise environments without Docker daemon.

**Why file-based inter-agent communication?**
Crash recovery is the primary reason. Every agent output is a durable artifact. If the pipeline crashes mid-run, `sql-migrate run` with the same `--run-id` resumes exactly where it left off — the Orchestrator reads `checkpoint.json` and skips already-completed procs.

**Why no raw code to the LLM until C2?**
LLM costs scale with token count. Sending 10,000-line proc files to every agent (Detection, Analysis, Planning) would be prohibitively expensive and wouldn't improve quality. The Analysis Agent uses tool-calling to read only the lines it needs (read_lines, parse_file). The LLM only sees full chunk code during C2 conversion where line-by-line understanding is essential.

**Why agentic conversion instead of text-based C2→C3→C4?**
In v7, the conversion pipeline was: C2 (LLM generates code as text) → C3 (Python validates) → C4 (LLM fixes errors from C3). This had three problems: (1) fence-stripping was fragile, (2) errors were described in text instead of shown directly, (3) the LLM couldn't explore alternatives. In v9, the LLM drives its own write→execute→fix cycle via tool calls (run_pyspark, validate_sql, query_trino, submit_result). Code goes from structured tool call arguments directly into the sandbox — no parsing, no fences, no text extraction. The LLM sees real execution errors and decides how to fix them.

**Why is the Orchestrator zero-LLM?**
The Orchestrator enforces business rules: loop guards, dependency ordering, failure routing, human feedback application. These are deterministic decisions that must be auditable and reproducible. An LLM orchestrator would be unpredictable in failure scenarios — exactly where predictability matters most.
