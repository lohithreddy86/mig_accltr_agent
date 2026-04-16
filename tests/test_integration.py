"""
test_integration.py
===================
End-to-end integration test: wires all 6 agents with mock LLM + mock MCP.
Verifies the full pipeline runs without errors on a minimal Oracle SQL fixture.

Run with:  pytest tests/test_integration.py -v
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Make src importable
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

ORACLE_SQL = """\
CREATE OR REPLACE PROCEDURE calc_interest(p_date IN DATE) AS
  CURSOR c_accts IS
    SELECT acct_id, NVL(balance, 0) bal FROM accounts WHERE status = 'A';
BEGIN
  FOR rec IN c_accts LOOP
    UPDATE ledger
       SET amount   = DECODE(rec.bal, 0, 0, rec.bal * 0.05),
           upd_date = SYSDATE
     WHERE acct_id = rec.acct_id;
  END LOOP;
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN ROLLBACK; RAISE;
END calc_interest;
/

CREATE OR REPLACE PROCEDURE generate_report(p_month IN NUMBER) AS
  v_total NUMBER := 0;
BEGIN
  SELECT SUM(NVL(amount, 0)) INTO v_total
    FROM ledger
   WHERE EXTRACT(MONTH FROM upd_date) = p_month;
  calc_interest(SYSDATE);
END generate_report;
/
"""


@pytest.fixture(scope="module")
def workspace(tmp_path_factory):
    """Create a temporary workspace with all required files."""
    ws = tmp_path_factory.mktemp("integration_workspace")

    # Write SQL file
    sql_path = ws / "procedures.sql"
    sql_path.write_text(ORACLE_SQL, encoding="utf-8")

    # Write a minimal README (no docx — parse_readme handles missing gracefully)
    readme_path = ws / "README.docx"
    # Write an empty file — parse_readme will return quality_score=0 and continue
    readme_path.write_bytes(b"")

    # Override artifact store path
    from sql_migration.core.config_loader import get_config
    get_config.cache_clear()
    cfg = get_config()
    cfg.paths.artifact_store = str(ws / "artifacts")
    for field in type(cfg.paths.subdirs).model_fields:
        os.makedirs(
            os.path.join(cfg.paths.artifact_store, getattr(cfg.paths.subdirs, field)),
            exist_ok=True,
        )

    return {
        "ws":          ws,
        "sql_path":    str(sql_path),
        "readme_path": str(readme_path),
        "run_id":      "integration_test_001",
    }


@pytest.fixture(scope="module")
def store(workspace):
    from sql_migration.core.artifact_store import ArtifactStore
    return ArtifactStore(run_id=workspace["run_id"])


@pytest.fixture(scope="module")
def mock_sandbox(workspace):
    """Sandbox that runs scripts directly on the host (no Podman)."""
    import subprocess
    from sql_migration.core.sandbox import SandboxResult

    class DirectSandbox:
        def __init__(self):
            self.source_dir = str(Path(workspace["sql_path"]).parent)

        def run(self, script, args=None, **kw):
            scripts_root = Path(__file__).resolve().parents[1] / "sandbox_scripts"
            script_path  = scripts_root / script
            result = subprocess.run(
                [sys.executable, str(script_path),
                 "--args", json.dumps(args or {})],
                capture_output=True, text=True, timeout=60,
            )
            return SandboxResult(
                returncode=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
                duration_s=0.1,
                script=script,
            )

    return DirectSandbox()


@pytest.fixture(scope="module")
def mock_mcp():
    mcp = MagicMock()
    mcp.list_tables.return_value = ["accounts", "ledger"]
    mcp.desc_table.return_value = [
        {"Column": "acct_id", "Type": "bigint",        "Null": "NO"},
        {"Column": "balance", "Type": "decimal(18,2)", "Null": "YES"},
        {"Column": "status",  "Type": "varchar",       "Null": "YES"},
    ]
    mcp.table_row_count.return_value = 0   # → LEVEL_1 validation
    mcp.sample_rows.return_value     = []
    mcp.run_query.return_value       = []
    return mcp


# ---------------------------------------------------------------------------
# LLM mock responses
# ---------------------------------------------------------------------------

DIALECT_CLASSIFICATION_RESPONSE = {
    "dialect_id":       "oracle",
    "dialect_display":  "Oracle 19c PL/SQL",
    "confidence":       0.95,
    "confidence_reason": "CREATE OR REPLACE PROCEDURE and NVL confirm Oracle PL/SQL",
    "evidence":         ["CREATE OR REPLACE PROCEDURE", "NVL(", "EXCEPTION WHEN OTHERS"],
    "contradictions":   [],
    "custom_extensions": [],
    "adapter_seed": {
        "proc_boundary": {
            "start_pattern":     r"CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+(\w+)",
            "end_pattern":       r"END\s+\w*\s*;",
            "name_capture_group": 1,
        },
        "cursor_pattern":        r"\bCURSOR\b",
        "exception_pattern":     r"\bEXCEPTION\b",
        "transaction_patterns": {
            "begin":    r"\bBEGIN\b",
            "commit":   r"\bCOMMIT\b",
            "rollback": r"\bROLLBACK\b",
            "end_block": r"\bEND\b",
        },
        "dynamic_sql_patterns":   ["EXECUTE\\s+IMMEDIATE"],
        "dialect_functions":      ["NVL", "DECODE", "SYSDATE", "RAISE_APPLICATION_ERROR"],
        "trino_mappings": {
            "NVL":    "COALESCE",
            "DECODE": "CASE WHEN ... END",
            "SYSDATE": "CURRENT_TIMESTAMP",
            "RAISE_APPLICATION_ERROR": None,
        },
        "pyspark_mappings": {
            "NVL":    "F.coalesce()",
            "SYSDATE": "F.current_timestamp()",
        },
        "unsupported_constructs":  [],
        "temp_table_pattern":      "TMP_|TEMP_",
        "confidence":              0.95,
    },
}

COMPLEXITY_RESPONSE = {
    "scores": [
        {"name": "calc_interest",  "score": "MEDIUM",
         "rationale": "Cursor loop over accounts",
         "priority_flag": True, "shared_utility_candidate": False},
        {"name": "generate_report","score": "LOW",
         "rationale": "Simple aggregation calling shared utility",
         "priority_flag": False, "shared_utility_candidate": False},
    ]
}

STRATEGY_RESPONSE = {
    "strategies": [
        {"name": "calc_interest",   "strategy": "PYSPARK_DF",
         "rationale": "Cursor loop needs DataFrame API",
         "shared_utility": False, "estimated_chunks": 1},
        {"name": "generate_report", "strategy": "TRINO_SQL",
         "rationale": "Simple aggregation",
         "shared_utility": False, "estimated_chunks": 1},
    ],
    "shared_utilities": [],
    "notes": "",
}

PYSPARK_CONVERTED = """\
from pyspark.sql import functions as F
df = spark.table("hdfc.core.accounts")
df_filtered = df.filter(F.col("status") == "A")
df_calc = df_filtered.withColumn("amount", F.coalesce(F.col("balance"), F.lit(0)) * 0.05)
df_calc.write.mode("overwrite").saveAsTable("hdfc.core.ledger")
"""

SQL_CONVERTED = """\
SELECT SUM(COALESCE(amount, 0)) AS total
FROM hdfc.core.ledger
WHERE EXTRACT(MONTH FROM upd_date) = 1
"""


def make_llm_responder():
    """Return a call() mock that routes by content."""
    calls = [0]

    def call(messages, expect_json=True, **kwargs):
        calls[0] += 1
        content = " ".join(m.get("content", "") for m in messages).lower()

        if "classify" in content or "adapter" in content or "dialect" in content:
            return DIALECT_CLASSIFICATION_RESPONSE
        if "complexity" in content or "score" in content:
            return COMPLEXITY_RESPONSE
        if "strategy" in content or "conversion strategy" in content:
            return STRATEGY_RESPONSE
        if "equivalent trino sql" in content or "semantic" in content:
            return "SELECT COALESCE(balance, 0) AS balance FROM accounts"

        # Default: return valid PySpark (used for conversion)
        return PYSPARK_CONVERTED

    def call_with_history(system_prompt, user_message, history, **kw):
        content = (system_prompt + user_message).lower()
        if "trino_sql" in content or "trino sql" in content:
            return SQL_CONVERTED, history
        return PYSPARK_CONVERTED, history

    return call, call_with_history


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

class TestFullPipeline:

    def test_analysis_setup_and_extraction(self, workspace, store, mock_sandbox, mock_mcp):
        """A0+A1: Analysis setup (README + adapter) and boundary extraction."""
        from sql_migration.agents.analysis import AnalysisAgent
        from sql_migration.models import AnalysisAgentInput

        agent = AnalysisAgent(store=store, sandbox=mock_sandbox, mcp=mock_mcp)
        call_fn, _ = make_llm_responder()
        agent.llm.call = call_fn
        # Mock call_with_tools for A0 adapter selection
        agent.llm.call_with_tools = lambda **kwargs: {
            "tool_calls": [{
                "id": "tc_1",
                "function": {
                    "name": "submit_adapter",
                    "arguments": json.dumps({
                        "adapter": {
                            "dialect_id": "oracle",
                            "dialect_display": "Oracle PL/SQL",
                            "proc_boundary": {
                                "start_pattern": r"(?:CREATE\s+(?:OR\s+REPLACE\s+)?(?:PROCEDURE|FUNCTION)\s+(?:[\w$.]+\.)?|^\s*(?:PROCEDURE|FUNCTION)\s+)(\w+)",
                                "end_pattern": r"END\s+\w*\s*;",
                                "name_capture_group": 1,
                            },
                            "cursor_pattern": r"\bCURSOR\b",
                            "exception_pattern": r"\bEXCEPTION\b",
                            "transaction_patterns": {"begin": r"\bBEGIN\b", "commit": r"\bCOMMIT\b", "rollback": r"\bROLLBACK\b", "end_block": r"\bEND\b"},
                            "dialect_functions": ["NVL", "DECODE", "SYSDATE"],
                            "trino_mappings": {"NVL": "COALESCE", "DECODE": "CASE", "SYSDATE": "CURRENT_DATE"},
                            "confidence": 0.95,
                        },
                        "validation_notes": "Oracle adapter matches code",
                    })
                }
            }]
        }

        output = agent.run(AnalysisAgentInput(
            readme_path=workspace["readme_path"],
            sql_file_paths=[workspace["sql_path"]],
            run_id=workspace["run_id"],
        ))

        assert output.manifest.total_procs >= 1
        assert store.exists("analysis", "dialect_profile.json")
        assert store.exists("analysis", "construct_map.json")
        assert store.exists("analysis", "readme_signals.json")
        assert store.exists("analysis", "proc_inventory.json")
        assert store.exists("analysis", "manifest.json")

    def test_analysis(self, workspace, store, mock_sandbox, mock_mcp):
        """A2–A5: Analysis extracts proc structure and resolves tables."""
        from sql_migration.agents.analysis import AnalysisAgent
        from sql_migration.models import AnalysisAgentInput

        agent = AnalysisAgent(store=store, sandbox=mock_sandbox, mcp=mock_mcp)
        call_fn, _ = make_llm_responder()
        agent.llm.call = call_fn
        # Mock call_with_tools for A0
        agent.llm.call_with_tools = lambda **kwargs: {
            "tool_calls": [{
                "id": "tc_1",
                "function": {
                    "name": "submit_adapter",
                    "arguments": json.dumps({
                        "adapter": {
                            "dialect_id": "oracle",
                            "dialect_display": "Oracle PL/SQL",
                            "proc_boundary": {
                                "start_pattern": r"(?:CREATE\s+(?:OR\s+REPLACE\s+)?(?:PROCEDURE|FUNCTION)\s+(?:[\w$.]+\.)?|^\s*(?:PROCEDURE|FUNCTION)\s+)(\w+)",
                                "end_pattern": r"END\s+\w*\s*;",
                                "name_capture_group": 1,
                            },
                            "cursor_pattern": r"\bCURSOR\b",
                            "exception_pattern": r"\bEXCEPTION\b",
                            "transaction_patterns": {"begin": r"\bBEGIN\b", "commit": r"\bCOMMIT\b", "rollback": r"\bROLLBACK\b", "end_block": r"\bEND\b"},
                            "dialect_functions": ["NVL", "DECODE", "SYSDATE"],
                            "trino_mappings": {"NVL": "COALESCE", "DECODE": "CASE", "SYSDATE": "CURRENT_DATE"},
                            "confidence": 0.95,
                        },
                        "validation_notes": "test",
                    })
                }
            }]
        }

        output = agent.run(AnalysisAgentInput(
            readme_path=workspace["readme_path"],
            sql_file_paths=[workspace["sql_path"]],
            run_id=workspace["run_id"],
        ))

        assert output.manifest.total_procs >= 1
        proc_names = output.manifest.proc_names()
        assert "calc_interest" in proc_names or len(proc_names) >= 1

        ci = output.manifest.get_proc("calc_interest")
        if ci:
            assert ci.has_cursor  # CURSOR keyword definitely present

        assert len(output.conversion_order.order) >= 1
        assert store.exists("analysis", "manifest.json")
        assert store.exists("analysis", "complexity_report.json")
        assert store.exists("analysis", "conversion_order.json")

    def test_planning(self, workspace, store, mock_sandbox, mock_mcp):
        """P1–P4: Planning produces a complete plan."""
        from sql_migration.agents.planning import PlanningAgent
        from sql_migration.models import PlanningAgentInput

        agent = PlanningAgent(store=store, sandbox=mock_sandbox, mcp=mock_mcp)
        call_fn, _ = make_llm_responder()
        agent.llm.call = call_fn

        output = agent.run(PlanningAgentInput(
            manifest_path="", complexity_report_path="",
            construct_map_path="", conversion_order_path="",
            run_id=workspace["run_id"],
        ))

        assert output.plan.total_procs() >= 1
        assert output.plan.total_chunks() >= 1
        assert len(output.plan.conversion_order) >= 1
        assert store.exists("planning", "plan.json")

        # Every proc in the plan has loop guards baked in
        for proc_name, proc_plan in output.plan.procs.items():
            assert proc_plan.loop_guards.frozen_after > 0
            assert len(proc_plan.chunks) > 0

    def test_conversion(self, workspace, store, mock_sandbox):
        """C1–C5: Conversion converts chunks and assembles."""
        from sql_migration.agents.conversion import ConversionAgent
        from sql_migration.models import DispatchTask
        from sql_migration.models.common import ConversionStrategy

        agent = ConversionAgent(store=store, sandbox=mock_sandbox)
        call_fn, call_hist = make_llm_responder()
        agent.llm.call = call_fn
        agent.llm.call_with_history = call_hist

        plan_data = store.read("planning", "plan.json")
        procs = plan_data.get("procs", {})
        assert procs, "Plan has no procs"

        proc_name = list(procs.keys())[0]
        proc_plan = procs[proc_name]
        chunk     = proc_plan["chunks"][0]
        strategy  = ConversionStrategy(proc_plan["strategy"])

        task = DispatchTask(
            proc_name=proc_name,
            chunk_id=chunk["chunk_id"],
            chunk_index=0,
            total_chunks=len(proc_plan["chunks"]),
            source_file=proc_plan["source_file"],
            start_line=chunk["start_line"],
            end_line=chunk["end_line"],
            strategy=strategy,
            construct_hints=chunk.get("construct_hints", {}),
            schema_context=chunk.get("schema_context", {}),
            state_vars=chunk.get("state_vars", {}),
        )

        result = agent.run_chunk(task)

        assert result.status == "SUCCESS", \
            f"Conversion failed: {result.errors}"
        assert result.converted_code
        assert store.exists("conversion", f"proc_{proc_name}.py") or \
               store.exists("conversion", f"proc_{proc_name}.sql"), \
               "Assembled proc file not written"

    def test_validation(self, workspace, store, mock_sandbox, mock_mcp):
        """V1–V5: Validation produces a result file."""
        from sql_migration.agents.validation import ValidationAgent

        agent = ValidationAgent(store=store, sandbox=mock_sandbox, mcp=mock_mcp)

        plan_data = store.read("planning", "plan.json")
        proc_name = list(plan_data.get("procs", {}).keys())[0]

        result = agent.run(proc_name, "")

        assert result.proc_name == proc_name
        assert result.outcome.value in ("PASS", "PARTIAL", "FAIL")
        # With mock_mcp returning 0 rows → LEVEL_1 → should be PARTIAL
        assert result.validation_level.value >= 1
        assert store.exists("validation", f"validation_{proc_name}.json")

    def test_orchestrator_full_run(self, workspace, store, mock_sandbox, mock_mcp):
        """Full orchestrator run: all procs reach terminal state."""
        from sql_migration.agents.conversion  import ConversionAgent
        from sql_migration.agents.validation  import ValidationAgent
        from sql_migration.agents.orchestrator import OrchestratorAgent
        from sql_migration.core.human_feedback import HumanFeedbackHandler
        from sql_migration.models.common import ValidationOutcome, ValidationLevel
        from sql_migration.models import ValidationResult

        conv_agent = ConversionAgent(store=store, sandbox=mock_sandbox)
        val_agent  = ValidationAgent(store=store, sandbox=mock_sandbox, mcp=mock_mcp)

        call_fn, call_hist = make_llm_responder()
        conv_agent.llm.call = call_fn
        conv_agent.llm.call_with_history = call_hist

        def run_conv(task):
            return conv_agent.run_chunk(task)

        def run_val(proc_name, path):
            return val_agent.run(proc_name, path)

        fb   = HumanFeedbackHandler(store)
        orch = OrchestratorAgent(
            store=store, feedback=fb,
            run_conversion=run_conv,
            run_validation=run_val,
        )
        ck = orch.run(workspace["run_id"])

        assert ck.is_complete(), \
            f"Pipeline not complete: {[(n, s.status.value) for n,s in ck.procs.items()]}"
        total_terminal = (
            ck.validated_count + ck.partial_count +
            ck.frozen_count   + ck.skipped_count
        )
        assert total_terminal == ck.total_procs, \
            f"Not all procs terminal: {total_terminal} / {ck.total_procs}"
        assert store.exists("orchestrator", "checkpoint.json")

    def test_checkpoint_crash_recovery(self, workspace, store, mock_sandbox, mock_mcp):
        """Restarting the orchestrator should not re-run already completed procs."""
        from sql_migration.agents.conversion  import ConversionAgent
        from sql_migration.agents.validation  import ValidationAgent
        from sql_migration.agents.orchestrator import OrchestratorAgent
        from sql_migration.core.human_feedback import HumanFeedbackHandler

        conv_calls = [0]

        call_fn, call_hist = make_llm_responder()

        def counting_conv(task):
            conv_calls[0] += 1
            conv_agent = ConversionAgent(store=store, sandbox=mock_sandbox)
            conv_agent.llm.call = call_fn
            conv_agent.llm.call_with_history = call_hist
            return conv_agent.run_chunk(task)

        val_agent = ValidationAgent(store=store, sandbox=mock_sandbox, mcp=mock_mcp)

        fb   = HumanFeedbackHandler(store)
        orch = OrchestratorAgent(
            store=store, feedback=fb,
            run_conversion=counting_conv,
            run_validation=lambda n, p: val_agent.run(n, p),
        )
        ck2 = orch.run(workspace["run_id"])

        # Should complete immediately — checkpoint shows all procs already done
        assert ck2.is_complete()
        assert conv_calls[0] == 0, \
            f"Expected 0 re-conversion calls on resume, got {conv_calls[0]}"
