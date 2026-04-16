"""
config_loader.py
================
Single source of truth for all configuration.
Reads config.yaml + .env, provides typed access throughout the codebase.

Usage:
    from sql_migration.core.config_loader import get_config
    cfg = get_config()
    model = cfg.llm.primary_model
    registry = cfg.analysis.adapter_registry
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field, model_validator


# ---------------------------------------------------------------------------
# Locate project root and config files
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parents[3]  # src/sql_migration/core → root
_CONFIG_FILE  = _PROJECT_ROOT / "config" / "config.yaml"
_ENV_FILE     = _PROJECT_ROOT / "config" / ".env"
_ENV_EXAMPLE  = _PROJECT_ROOT / "config" / ".env.example"


# ---------------------------------------------------------------------------
# Pydantic models for each config section
# ---------------------------------------------------------------------------

class PathsSubdirsConfig(BaseModel):
    analysis:     str = "00_analysis"
    planning:     str = "01_planning"
    orchestrator: str = "02_orchestrator"
    conversion:   str = "03_conversion"
    validation:   str = "04_validation"
    feedback:     str = "05_developer_feedback"
    samples:      str = "06_samples"
    provenance:   str = "07_provenance"       # v6: Artifact lineage tracking


class PathsConfig(BaseModel):
    artifact_store:  str = "./artifacts"
    adapters_dir:    str = "./adapters"
    sandbox_scripts: str = "./sandbox_scripts"
    logs_dir:        str = "./logs"
    temp_dir:        str = "/tmp/sql_migration"
    subdirs:         PathsSubdirsConfig = Field(default_factory=PathsSubdirsConfig)

    def resolve(self) -> "PathsConfig":
        """Resolve relative paths against project root."""
        root = _PROJECT_ROOT
        self.artifact_store  = str(root / self.artifact_store)
        self.adapters_dir    = str(root / self.adapters_dir)
        self.sandbox_scripts = str(root / self.sandbox_scripts)
        self.logs_dir        = str(root / self.logs_dir)
        return self

    def artifact_subdir(self, agent: str, run_id: str = "") -> Path:
        """Return the agent-specific subdirectory under the artifact store.

        When run_id is provided the path is isolated per run:
            {artifact_store}/{run_id}/{agent_subdir}
        Without run_id (legacy / convenience) returns the unscoped path.
        Prefer using ArtifactStore directly — it always passes run_id.
        """
        subdir = getattr(self.subdirs, agent, agent)
        base = Path(self.artifact_store)
        if run_id:
            base = base / run_id
        return base / subdir


class LLMAgentModels(BaseModel):
    analysis:     str = ""
    planning:     str = ""
    orchestrator: str = ""
    conversion:   str = ""
    validation:   str = ""


class LLMConfig(BaseModel):
    primary_model:      str = "gemini-2.5-pro"
    agent_models:       LLMAgentModels = Field(default_factory=LLMAgentModels)
    temperature:        float = 0.1
    max_tokens:         int   = 8192
    timeout_seconds:    int   = 120
    retry_attempts:     int   = 3
    retry_backoff_base: int   = 2
    json_mode:          bool  = True

    # Enterprise proxy settings (optional — leave empty to use LiteLLM defaults)
    api_base:           str   = ""    # e.g. "https://genai-proxy.hdfc.internal/UAT/litellm/v1"
    api_key_env:        str   = ""    # Name of env var holding the API key, e.g. "LLM_API_KEY"
    ssl_verify:         bool  = True  # Set false for enterprise proxies with internal certs

    @model_validator(mode="after")
    def resolve_env_refs(self) -> "LLMConfig":
        """Resolve ${ENV_VAR} references in api_base."""
        if self.api_base and self.api_base.startswith("${"):
            var_name = self.api_base.strip("${}")
            self.api_base = os.environ.get(var_name, "")
        return self

    def model_for_agent(self, agent: str) -> str:
        """Return the model to use for a given agent, with override support."""
        override = getattr(self.agent_models, agent, "")
        return override if override else self.primary_model

    def get_api_key(self) -> str | None:
        """Resolve the API key from the named env var (if configured)."""
        if self.api_key_env:
            return os.environ.get(self.api_key_env)
        return None


class SandboxMountConfig(BaseModel):
    type:    str = "bind"
    source:  str
    target:  str
    options: str = "rw"


class SandboxConfig(BaseModel):
    enabled:         bool   = True
    runtime:         str    = "podman"
    image:           str    = "sql-migration-sandbox:latest"
    cpu_limit:       str    = "2.0"
    memory_limit:    str    = "4g"
    timeout_seconds: int    = 300
    base_mounts:     list[SandboxMountConfig] = Field(default_factory=list)
    container_env:   dict[str, str]           = Field(default_factory=dict)

    @model_validator(mode="after")
    def resolve_env_vars_in_mounts(self) -> "SandboxConfig":
        """Expand ${VAR} placeholders in mount source paths."""
        for mount in self.base_mounts:
            mount.source = os.path.expandvars(mount.source)
        return self


class MCPToolsConfig(BaseModel):
    list_schemas:   str = "list_schemas"
    list_tables:    str = "list_tables"
    describe_table: str = "describe_table"
    run_query:      str = "run_query"


class MCPConfig(BaseModel):
    server_url:         str = ""
    transport:          str = "sse"
    connection_timeout: int = 30
    query_timeout:      int = 60
    max_retries:        int = 3
    tools:              MCPToolsConfig   = Field(default_factory=MCPToolsConfig)
    blocked_keywords:   list[str]        = Field(default_factory=lambda: [
        "INSERT","UPDATE","DELETE","DROP","CREATE","ALTER","TRUNCATE","MERGE"
    ])
    sample_row_limit:   int = 200
    schema_probe_query: str = "SELECT * FROM {table} LIMIT 0"

    @model_validator(mode="after")
    def load_server_url_from_env(self) -> "MCPConfig":
        # Resolve ${ENV_VAR} placeholders in server_url
        if self.server_url and self.server_url.startswith("${"):
            var_name = self.server_url.strip("${}")
            self.server_url = os.environ.get(var_name, "")
        # Fallback: if still empty, try the standard env var
        if not self.server_url:
            self.server_url = os.environ.get("TRINO_MCP_SERVER_URL", "")
        return self


class AnalysisDeepExtractConfig(BaseModel):
    min_line_count:      int  = 300
    has_dynamic_sql:     bool = True
    has_unsupported_min: int  = 1


class AnalysisComplexityConfig(BaseModel):
    cursor_nesting_high_threshold: int = 2
    unmapped_constructs_medium:    int = 1
    unmapped_constructs_manual:    int = 6


class AnalysisConfig(BaseModel):
    # A0 setup — adapter selection
    head_sample_lines:       int            = 200
    adapter_registry:        dict[str, str] = Field(default_factory=lambda: {
        "oracle":    "oracle.json",
        "db2":       "db2.json",
        "sqlserver": "sqlserver.json",
        "teradata":  "teradata.json",
        "hive":      "hive.json",
        "snowflake": "snowflake.json",
        "generic":   "generic.json",
    })

    # A2 per-proc LLM analysis (v10 Task 2)
    per_proc_max_lines:          int  = 600    # Skip LLM for procs larger than this (use regex)
    rolling_summary_max_chars:   int  = 3000   # Truncate rolling summary to fit context
    # per_proc_timeout_seconds not needed — global llm.timeout_seconds (120s) applies
    # per_proc_timeout_seconds:    int  = 60

    # Query mode (source_type=sql_queries)
    query_mode_max_statements:   int  = 500    # Abort if file has more statements than this

    # A1+ analysis config (fallback path)
    deep_extract_triggers: AnalysisDeepExtractConfig = Field(
        default_factory=AnalysisDeepExtractConfig
    )
    complexity_rules:      AnalysisComplexityConfig  = Field(
        default_factory=AnalysisComplexityConfig
    )
    cycle_handling:              str = "flag_and_continue"
    missing_dep_max_retries:     int = 2
    missing_dep_action:          str = "stub_and_continue"


class PlanningLoopGuardsConfig(BaseModel):
    max_chunk_retry:  int = 2
    max_replan_depth: int = 3
    frozen_after:     int = 3


class ModuleGroupingConfig(BaseModel):
    """Config for P1.5 module grouping step."""
    enabled:                  bool = True
    max_procs_per_module:     int  = 8
    merge_write_conflicts:    bool = True   # Force procs writing same table into same module


class PlanningConfig(BaseModel):
    chunk_target_lines:           int = 400
    chunk_max_lines:              int = 500
    chunk_min_lines:              int = 80
    strategy_rules:               dict[str, int] = Field(default_factory=lambda: {
        "unmapped_medium_threshold": 3,
        "unmapped_manual_threshold": 6,
    })
    shared_utility_min_callers:   int = 3
    loop_guards:                  PlanningLoopGuardsConfig = Field(
        default_factory=PlanningLoopGuardsConfig
    )
    module_grouping:              ModuleGroupingConfig = Field(
        default_factory=ModuleGroupingConfig
    )


class HumanFeedbackOrchestratorConfig(BaseModel):
    mode:                    str = "file"
    pause_timeout:           int = 3600
    feedback_file_pattern:   str = "developer_feedback_{proc_name}.json"
    resolved_file_pattern:   str = "feedback_resolved_{proc_name}.json"


class OrchestratorConfig(BaseModel):
    checkpoint_on_every_state_change: bool  = True
    max_concurrent_procs:             int   = 1
    dispatch_poll_interval:           int   = 2
    human_feedback: HumanFeedbackOrchestratorConfig = Field(
        default_factory=HumanFeedbackOrchestratorConfig
    )


class AgenticConfig(BaseModel):
    """Budget and cost config for the agentic tool-calling loop in Conversion Agent."""
    # Budget formula: budget = max_chunk_self_corrections * multiplier + base
    conversion_budget_multiplier: int = 3    # tool calls per correction attempt
    conversion_budget_base:       int = 4    # base tool calls (initial convert + submit)
    # Turn formula: max_turns = max_chunk_self_corrections * multiplier + base
    conversion_turns_multiplier:  int = 3    # LLM turns per correction attempt
    conversion_turns_base:        int = 4    # base turns
    # Per-tool cost weights (higher = burns budget faster)
    tool_costs: dict[str, int] = Field(default_factory=lambda: {
        "run_pyspark":   3,   # Slow: PySpark startup + execution
        "validate_sql":  1,   # Fast: sqlglot parse
        "query_trino":   1,   # Fast: MCP read-only query
        "submit_result": 0,   # Free: terminal
    })


class ConversionSandboxValidationConfig(BaseModel):
    run_remnant_check:     bool = True   # Check for unconverted dialect functions at submit
    run_test_execution:    bool = True   # Run integration test on assembled multi-chunk output


class ConversionConfig(BaseModel):
    max_chunk_self_corrections: int = 2
    # Query mode
    query_mode_max_retries:     int = 1   # Single retry for query conversion
    query_mode_budget:          int = 4   # Tool calls per query
    sandbox_validation:         ConversionSandboxValidationConfig = Field(
        default_factory=ConversionSandboxValidationConfig
    )


class ValidationScoringConfig(BaseModel):
    row_deviation_fail: float = 0.30
    row_deviation_warn: float = 0.10
    max_todos_for_pass: int   = 0


class ValidationSyntheticConfig(BaseModel):
    add_null_row:        bool = True
    add_boundary_values: bool = True
    null_row_count:      int  = 1


class ValidationSemanticDiffConfig(BaseModel):
    enabled:            bool      = True
    column_status_fail: list[str] = Field(default_factory=lambda: ["MISSING_IN_TRINO"])
    column_status_warn: list[str] = Field(default_factory=lambda: ["TYPE_CHANGED","EXTRA_IN_TRINO"])
    column_status_ok:   list[str] = Field(default_factory=lambda: ["MATCH","RENAMED","TYPE_WIDENED"])


class ValidationConfig(BaseModel):
    levels:               dict[int, str] = Field(default_factory=lambda: {
        1: "static_analysis_only",
        2: "schema_check_plus_synthetic_exec",
        3: "full_reconciliation_with_semantic_diff",
    })
    min_rows_for_level_3: int  = 50
    sample_row_count:     int  = 200
    scoring:              ValidationScoringConfig      = Field(default_factory=ValidationScoringConfig)
    synthetic_data:       ValidationSyntheticConfig    = Field(default_factory=ValidationSyntheticConfig)
    semantic_diff:        ValidationSemanticDiffConfig = Field(default_factory=ValidationSemanticDiffConfig)


class HumanFeedbackGlobalConfig(BaseModel):
    bundle_includes: list[str] = Field(default_factory=lambda: [
        "manifest_entry", "complexity_report_entry", "all_chunk_converted_code",
        "all_validation_reports", "error_history", "manual_review_items",
    ])
    valid_actions: list[str] = Field(default_factory=lambda: [
        "manual_rewrite_provided", "mark_skip", "override_pass", "replan_with_notes",
    ])


class LoggingConfig(BaseModel):
    level:             str  = "INFO"
    format:            str  = "json"
    per_agent_files:   bool = True
    include_prompts:   bool = False
    include_responses: bool = False


class RunConfig(BaseModel):
    run_id_prefix:         str       = "migration"
    dry_run:               bool      = False
    stop_on_first_frozen:  bool      = False
    procs_filter:          list[str] = Field(default_factory=list)


class DependencyGateConfig(BaseModel):
    enabled:              bool = True
    cli_auto_action:      str  = "stub_todo"   # "stub_todo" | "abort" | "skip_callers"
    min_callers_to_gate:  int  = 1


class WriteSemanticsConfig(BaseModel):
    enabled:                    bool  = True
    low_confidence_threshold:   float = 0.5
    default_write_mode:         str   = "append"


class PreflightConfig(BaseModel):
    enabled:              bool = True
    critical_threshold:   int  = 50
    high_threshold:       int  = 30
    medium_threshold:     int  = 15
    pause_on_critical:    bool = True


class GlobalErrorAggregatorConfig(BaseModel):
    enabled:              bool = True
    cross_proc_threshold: int  = 3
    max_failed_procs:     int  = 5


class ProvenanceConfig(BaseModel):
    enabled:                    bool  = True
    low_confidence_threshold:   float = 0.6


# ---------------------------------------------------------------------------
# Root config model
# ---------------------------------------------------------------------------

class AppConfig(BaseModel):
    paths:           PathsConfig              = Field(default_factory=PathsConfig)
    llm:             LLMConfig                = Field(default_factory=LLMConfig)
    sandbox:         SandboxConfig            = Field(default_factory=SandboxConfig)
    mcp:             MCPConfig                = Field(default_factory=MCPConfig)
    agentic:         AgenticConfig            = Field(default_factory=AgenticConfig)
    analysis:        AnalysisConfig           = Field(default_factory=AnalysisConfig)
    planning:        PlanningConfig           = Field(default_factory=PlanningConfig)
    orchestrator:    OrchestratorConfig       = Field(default_factory=OrchestratorConfig)
    conversion:      ConversionConfig         = Field(default_factory=ConversionConfig)
    validation:      ValidationConfig         = Field(default_factory=ValidationConfig)
    human_feedback:  HumanFeedbackGlobalConfig = Field(default_factory=HumanFeedbackGlobalConfig)
    logging:         LoggingConfig            = Field(default_factory=LoggingConfig)
    run:             RunConfig                = Field(default_factory=RunConfig)
    dependency_gate: DependencyGateConfig     = Field(default_factory=DependencyGateConfig)
    write_semantics: WriteSemanticsConfig     = Field(default_factory=WriteSemanticsConfig)
    preflight:       PreflightConfig          = Field(default_factory=PreflightConfig)
    global_error_aggregator: GlobalErrorAggregatorConfig = Field(
        default_factory=GlobalErrorAggregatorConfig
    )
    provenance:      ProvenanceConfig         = Field(default_factory=ProvenanceConfig)


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base."""
    result = base.copy()
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = val
    return result


@lru_cache(maxsize=1)
def get_config() -> AppConfig:
    """
    Load and cache the application configuration.
    Call once at startup; returns the same object on subsequent calls.
    Reload by calling get_config.cache_clear() then get_config().
    """
    # Load .env first so os.environ is populated before YAML is parsed
    env_file = _ENV_FILE if _ENV_FILE.exists() else _ENV_EXAMPLE
    load_dotenv(env_file, override=False)  # Don't override already-exported shell vars

    # Load base config
    raw = _load_yaml(_CONFIG_FILE)

    # Allow environment-variable overrides for top-level scalar fields:
    # SQL_MIGRATION_LOG_LEVEL → logging.level
    # SQL_MIGRATION_DRY_RUN  → run.dry_run
    env_overrides: dict[str, Any] = {}
    if (v := os.environ.get("SQL_MIGRATION_LOG_LEVEL")):
        env_overrides.setdefault("logging", {})["level"] = v
    if (v := os.environ.get("SQL_MIGRATION_DRY_RUN")):
        env_overrides.setdefault("run", {})["dry_run"] = v.lower() == "true"
    if (v := os.environ.get("SANDBOX_IMAGE")):
        env_overrides.setdefault("sandbox", {})["image"] = v
    if (v := os.environ.get("ARTIFACT_STORE_PATH")):
        # Override the artifact store root — useful for shared NAS deployments
        env_overrides.setdefault("paths", {})["artifact_store"] = v

    merged = _deep_merge(raw, env_overrides)

    config = AppConfig(**merged)
    config.paths.resolve()       # Resolve relative → absolute paths
    return config


def get_artifact_path(agent: str, filename: str, run_id: str = "") -> Path:
    """
    Convenience: return absolute path for an artifact file.

    DEPRECATED for pipeline use — prefer ArtifactStore.path(agent, filename)
    which always scopes to the correct run_id.
    This function is retained only for ad-hoc scripts and tests that need
    a path without constructing a full ArtifactStore instance.
    """
    cfg = get_config()
    directory = cfg.paths.artifact_subdir(agent, run_id=run_id)
    directory.mkdir(parents=True, exist_ok=True)
    return directory / filename