"""
agent.py  (Analysis Agent — v10)
==================================
v10 Task 1: Absorbs Detection Agent's setup work into A0 phase.
v10 Task 2: Replaces single giant LLM call with per-proc sequential analysis.

Flow:
  A0  SETUP     — Parse README, select+validate adapter via LLM tool call,
                   build construct_map + dialect_profile. (1 LLM call)
  A1  SANDBOX   — Deterministic boundary scan via extract_structure.py,
                   writes proc_inventory.json with boundaries per proc.
  A2  LLM×N     — Per-proc LLM analysis. Each proc's full body sent to LLM
                   individually with rolling summary from previous procs.
                   LLM classifies tables (REAL_TABLE vs CTE vs VIEW vs VARIABLE),
                   discovers calls, assesses complexity, writes business purpose.
                   (N LLM calls, one per proc)
  A3  MCP       — Resolve only REAL_TABLE entries against Trino. Zero CTE/view
                   false positives. Fail fast if real tables are missing.
  A4  SYNTHESIS — Merge per-proc results into 8 artifacts: manifest, table_registry,
                   dependency_graph, conversion_order, complexity_report,
                   semantic_map, construct_map (enriched), dialect_profile.

  Fallback: if per-proc LLM fails for one proc, that proc gets regex-only
  analysis via _a2_fallback_from_regex(). Other procs still get LLM analysis.

Inputs:  readme_path + sql_file_paths + run_id
Outputs: AnalysisAgentOutput consumed by Planning Agent.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from sql_migration.core.artifact_store import ArtifactStore
from sql_migration.core.config_loader import get_config
from sql_migration.core.llm_client import LLMClient
from sql_migration.core.logger import get_logger
from sql_migration.core.mcp_client import MCPClientSync
from sql_migration.core.sandbox import Sandbox
from sql_migration.models import (
    AnalysisAgentInput,
    AnalysisAgentOutput,
    ComplexityReport,
    ConversionOrder,
    DependencyGraph,
    DialectAdapter,
    DialectProfile,
    Manifest,
    ProcEntry,
    TableRegistry,
    build_llm_complexity_input,
    parse_llm_complexity_output,
)
from sql_migration.models.detection import (
    AdapterRegistryDescription,
    AdapterSeedProcBoundary,
    AdapterSeedTransactionPatterns,
    ConstructMap,
    ReadmeSignals,
)
from sql_migration.models.analysis import (
    PerProcAnalysis,
    PipelineConfig,
    TableRef,
)
from sql_migration.models.common import TableRegistryEntry, TableStatus, ColumnInfo

log = get_logger("analysis")


class AnalysisExtractionError(RuntimeError):
    """
    Raised when A1 structural extraction produces results that would silently
    corrupt every downstream agent.
    """
    pass


def _count_lines(filepath: str) -> int:
    """Count lines in a file without loading it fully into memory."""
    try:
        with open(filepath, encoding="utf-8", errors="replace") as f:
            return sum(1 for _ in f)
    except Exception:
        return 0


class AnalysisAgent:
    """
    Analysis Agent — v10.
    A0 setup (README + adapter) → A1 boundary scan → intelligent/fallback analysis.
    """

    def __init__(
        self,
        store:   ArtifactStore,
        sandbox: Sandbox,
        mcp:     MCPClientSync | None = None,
    ) -> None:
        self.store   = store
        self.sandbox = sandbox
        self.mcp     = mcp or MCPClientSync()
        self.llm     = LLMClient(agent="analysis")
        self.cfg     = get_config().analysis
        self.global_cfg = get_config()

    # =========================================================================
    # Public entrypoint
    # =========================================================================

    def run(self, agent_input: AnalysisAgentInput) -> AnalysisAgentOutput:
        run_id = agent_input.run_id
        log.info("analysis_start", run_id=run_id, files=len(agent_input.sql_file_paths))

        # ── A0: Setup (absorbs Detection) ─────────────────────────────────
        if agent_input.pipeline_config:
            readme_signals = self._build_readme_signals_from_config(
                agent_input.pipeline_config)
            log.info("a0_config_from_ui",
                     source_engine=readme_signals.source_engine,
                     declared_dialect=readme_signals.declared_dialect,
                     source_type=readme_signals.source_type)
        else:
            readme_signals = self._a0_parse_readme(agent_input.readme_path)
            log.info("a0_readme_parsed",
                     quality_score=readme_signals.quality_score,
                     declared_dialect=readme_signals.declared_dialect)
        self.store.write("analysis", "readme_signals.json", readme_signals.model_dump())

        adapter = self._a0_select_and_validate_adapter(
            readme_signals, agent_input.sql_file_paths)
        _persist_adapter(adapter, self.global_cfg.paths.adapters_dir)
        log.info("a0_adapter_ready",
                 dialect=adapter.dialect_id,
                 confidence=adapter.confidence,
                 hand_crafted=adapter.hand_crafted)

        construct_map = _build_construct_map(adapter.dialect_id, adapter)
        self.store.write("analysis", "construct_map.json", construct_map.model_dump())

        dialect_profile = self._a0_build_dialect_profile(
            readme_signals, adapter, run_id)
        self.store.write("analysis", "dialect_profile.json", dialect_profile.model_dump())
        log.info("a0_complete",
                 dialect=dialect_profile.dialect_id,
                 resolution=dialect_profile.resolution)

        # ── A1: Deterministic boundary scan ────────────────────────────────
        if dialect_profile.is_ssis_mode:
            # SSIS mode: Parser already produced one .sql per task.
            # Each file = one manifest entry. No splitting, no boundary scan.
            raw_procs = []
            all_tables: list[str] = []
            for fpath in agent_input.sql_file_paths:
                p = Path(fpath)
                text = p.read_text(encoding="utf-8", errors="replace")
                lines = text.splitlines()
                # Derive task name from filename (Parser uses pkg__task_id.sql)
                name = p.stem
                raw_procs.append({
                    "name": name,
                    "source_file": str(p),
                    "start_line": 1,
                    "end_line": len(lines),
                    "line_count": len(lines),
                    "_extracted_path": str(p),
                })
            log.info("a1_complete_ssis_mode",
                     tasks_found=len(raw_procs), files=len(agent_input.sql_file_paths))

        elif dialect_profile.is_query_mode:
            # Query mode: split on ; instead of proc boundary regex
            raw_procs, all_tables = self._a1_split_statements(
                agent_input.sql_file_paths, dialect_profile)
            log.info("a1_complete_query_mode",
                     queries_found=len(raw_procs), tables_found=len(all_tables))

            # Cap: abort if too many statements
            max_stmts = self.cfg.query_mode_max_statements
            if len(raw_procs) > max_stmts:
                raise AnalysisExtractionError(
                    f"Query mode: found {len(raw_procs)} statements, exceeds "
                    f"max of {max_stmts}. Split into smaller files or increase "
                    f"analysis.query_mode_max_statements in config."
                )
            # Skip _verify_extraction — no proc boundary validation for queries
        else:
            raw_procs, all_tables = self._a1_extract_structure(
                agent_input.sql_file_paths, dialect_profile, adapter)
            log.info("a1_complete", procs_found=len(raw_procs), tables_found=len(all_tables))

            self._verify_extraction(
                raw_procs=raw_procs,
                sql_file_paths=agent_input.sql_file_paths,
                fallback_mode=dialect_profile.fallback_mode,
                adapter=adapter,
            )

        # Write proc inventory (lightweight — just boundaries, no analysis)
        self.store.write("analysis", "proc_inventory.json", {
            "procs": [{
                "name": p["name"],
                "source_file": p["source_file"],
                "start_line": p["start_line"],
                "end_line": p["end_line"],
                "line_count": p["line_count"],
            } for p in raw_procs],
            "total_procs": len(raw_procs),
            "total_lines": sum(p["line_count"] for p in raw_procs),
        })

        # Extract and write individual proc .sql files for A2 + debugging
        procs_dir = Path(self.store.path("analysis", "procs"))
        procs_dir.mkdir(parents=True, exist_ok=True)
        for p in raw_procs:
            body = self._read_proc_body(p["source_file"], p["start_line"], p["end_line"])
            if body.strip():
                proc_path = procs_dir / f"{p['name']}.sql"
                proc_path.write_text(body, encoding="utf-8")
                p["_extracted_path"] = str(proc_path)
        log.info("a1_proc_files_written",
                 count=len([p for p in raw_procs if p.get("_extracted_path")]),
                 dir=str(procs_dir))

        # ── A2: Per-proc LLM analysis (v10 Task 2) ───────────────────────
        # Each proc body sent to LLM individually with rolling summary.
        # LLM classifies tables (REAL_TABLE vs CTE vs VIEW), discovers
        # calls, assesses complexity — with full SQL understanding.
        proc_analyses = self._a2_per_proc_loop(raw_procs, adapter, dialect_profile)
        log.info("a2_complete",
                 procs_analyzed=len(proc_analyses),
                 total_real_tables=len(set(
                     t for pa in proc_analyses for t in pa.all_real_tables)))

        # ── A3: Resolve classified tables via MCP ─────────────────────
        # Only REAL_TABLE entries are sent to Trino — no CTE aliases,
        # no cursor names, no variables. Zero false positives.
        table_registry = self._a3_resolve_classified_tables(
            proc_analyses, dialect_profile)
        self.store.write("analysis", "table_registry.json", table_registry.model_dump())
        log.info("a3_complete",
                 resolved=len([e for e in table_registry.entries.values()
                               if e.status == TableStatus.EXISTS_IN_TRINO]),
                 missing=len(table_registry.missing_tables()),
                 temp=len([e for e in table_registry.entries.values()
                           if e.status == TableStatus.TEMP_TABLE]))

        # Fail fast if real tables are missing from Trino
        verify_procs = [{
            "name": pa.name,
            "tables_read": pa.real_tables_read,
            "tables_written": pa.real_tables_written,
        } for pa in proc_analyses]
        self._verify_table_registry(table_registry, verify_procs, dialect_profile)

        # ── A4: Synthesize into final artifacts ───────────────────────
        # Merge per-proc results into the 8 artifacts Planning expects.
        # Enrich construct_map with newly discovered dialect functions.
        return self._a4_synthesize(
            proc_analyses=proc_analyses,
            table_registry=table_registry,
            construct_map=construct_map,
            dialect_profile=dialect_profile,
            adapter=adapter,
            run_id=run_id,
        )

        # ── OLD: Intelligent analysis (v9 — replaced by A2 per-proc loop)
        # from sql_migration.agents.analysis.semantic_analysis import run_intelligent_analysis
        #
        # analysis_data = run_intelligent_analysis(
        #     store=self.store, sandbox=self.sandbox, mcp=self.mcp,
        #     sql_file_paths=agent_input.sql_file_paths, run_id=run_id,
        #     readme_signals=readme_signals.model_dump(),
        #     dialect_profile=dialect_profile.model_dump(),
        #     adapter=adapter.model_dump(),
        # )
        #
        # if "error" in analysis_data and "manifest" not in analysis_data:
        #     return self._run_regex_fallback(
        #         agent_input, dialect_profile, adapter, raw_procs, all_tables, run_id)
        #
        # return self._unpack_intelligent_results(
        #     analysis_data, agent_input, dialect_profile, adapter,
        #     raw_procs, all_tables, run_id)

    # =========================================================================
    # A0 — Setup phase (replaces Detection Agent)
    # =========================================================================

    def _build_readme_signals_from_config(self, cfg: PipelineConfig) -> ReadmeSignals:
        """Build ReadmeSignals directly from structured Streamlit UI inputs."""
        udf_impls = []
        if cfg.udf_file_path:
            udf_impls = self._parse_udf_file(cfg.udf_file_path)

        return ReadmeSignals(
            source_engine=cfg.source_engine,
            declared_dialect=cfg.declared_dialect,
            source_type=cfg.source_type,
            output_format=cfg.output_format,
            input_catalog=cfg.input_catalog,
            input_schema=cfg.input_schemas,
            udf_implementations=udf_impls,
            quality_score=1.0,
        )

    def _parse_udf_file(self, file_path: str) -> list[dict[str, str]]:
        """
        Read an uploaded UDF file and split into [{name, code}] blocks.

        Handles:
          - Python style: def function_name(...)
          - SQL style: CREATE [OR REPLACE] FUNCTION/PROCEDURE name
        """
        import re

        path = Path(file_path)
        if not path.exists():
            log.warning("udf_file_not_found", path=file_path)
            return []

        text = path.read_text(encoding="utf-8", errors="replace")
        if not text.strip():
            return []

        udfs = []
        # Split on function/procedure boundaries
        pattern = r'(?=\bdef\s+\w+\s*\(|(?i)CREATE\s+(?:OR\s+REPLACE\s+)?(?:FUNCTION|PROCEDURE)\s+)'
        blocks = re.split(pattern, text)

        for block in blocks:
            block = block.strip()
            if not block:
                continue
            # Extract function/procedure name
            match = re.match(
                r'(?:def\s+(\w+)|(?i)CREATE\s+(?:OR\s+REPLACE\s+)?(?:FUNCTION|PROCEDURE)\s+(\S+))',
                block
            )
            if match:
                name = match.group(1) or match.group(2)
                name = name.strip('"[]`')
                udfs.append({"name": name, "code": block})

        log.info("udf_file_parsed", path=file_path, udfs_found=len(udfs))
        return udfs

    def _a0_parse_readme(self, readme_path: str) -> ReadmeSignals:
        """Call parse_readme.py in sandbox → readme_signals. Fails fast on missing mandatory fields."""
        # v10-fix: If README is not under source_dir (and thus not mounted
        # at /source/ in Podman), mount its parent directory explicitly.
        extra_mounts = None
        effective_readme_path = readme_path

        if (self.sandbox.cfg.enabled
                and self.sandbox.source_dir
                and readme_path):
            readme_abs = str(Path(readme_path).resolve())
            source_abs = str(self.sandbox.source_dir.resolve())
            if not readme_abs.startswith(source_abs):
                # README is outside source_dir — add explicit mount
                readme_dir = str(Path(readme_path).resolve().parent)
                readme_name = Path(readme_path).name
                extra_mounts = [{
                    "source": readme_dir,
                    "target": "/readme",
                    "options": "ro",
                }]
                effective_readme_path = f"/readme/{readme_name}"

        result = self.sandbox.run(
            script="extraction/parse_readme.py",
            args={"readme_path": effective_readme_path},
            extra_mounts=extra_mounts,
        )
        result.raise_on_failure()
        raw = result.stdout_json

        # Fail fast if mandatory fields are missing
        if raw.get("_missing_mandatory"):
            raise AnalysisExtractionError(raw["_parse_error"])

        return ReadmeSignals(**raw)

    def _a0_select_and_validate_adapter(
        self,
        readme_signals: ReadmeSignals,
        sql_file_paths: list[str],
    ) -> DialectAdapter:
        """
        LLM-driven adapter selection and validation.

        1. Read first 200 lines from source file (HEAD sample)
        2. Build descriptions of all adapters in registry
        3. LLM call with: 200 lines + readme_signals + adapter descriptions
        4. LLM picks adapter via load_adapter tool call → gets full JSON
        5. LLM validates adapter against code, adjusts if needed
        6. If no adapter matches: LLM builds one from scratch
        """
        head_sample = self._read_head_lines(sql_file_paths[0],
                                             self.cfg.head_sample_lines)
        registry_descriptions = self._describe_adapter_registry()

        # Build the LLM prompt
        system_prompt = (
            "You are a SQL dialect analyzer for a SQL migration system. "
            "You have two tools:\n"
            "  load_adapter(filename) — load a dialect adapter JSON from the registry\n"
            "  submit_adapter(adapter, validation_notes) — submit the final validated adapter\n\n"
            "WORKFLOW:\n"
            "1. Read the code sample and README signals.\n"
            "2. Review the available adapter descriptions.\n"
            "3. If one matches, call load_adapter to load it. Verify it against the code.\n"
            "4. If it's a good fit (patterns match, functions covered), submit it as-is.\n"
            "5. If it needs adjustments (missing patterns, wrong functions), modify and submit.\n"
            "6. If NO adapter matches, build a new adapter from scratch and submit it.\n\n"
            "The adapter JSON must have: dialect_id, dialect_display, proc_boundary "
            "(with start_pattern, end_pattern, name_capture_group), cursor_pattern, "
            "exception_pattern, transaction_patterns, dialect_functions, "
            "trino_mappings, confidence.\n"
            "Output ONLY tool calls — no prose."
        )

        user_content = (
            f"=== SOURCE CODE (first 200 lines) ===\n{head_sample}\n\n"
            f"=== README SIGNALS ===\n"
            f"Source engine: {readme_signals.source_engine}\n"
            f"Declared dialect: {readme_signals.declared_dialect}\n"
            f"Proc start example: {readme_signals.proc_start_example}\n"
            f"Proc end example: {readme_signals.proc_end_example}\n"
            f"Has packages: {readme_signals.has_packages}\n"
            f"Exception keyword: {readme_signals.exception_keyword}\n"
            f"Script delimiter: {readme_signals.script_delimiter}\n"
            f"Custom functions: {readme_signals.function_names[:15]}\n\n"
            f"=== AVAILABLE ADAPTERS IN REGISTRY ===\n"
        )
        for desc in registry_descriptions:
            user_content += (
                f"\n  {desc.filename}: {desc.dialect_display}\n"
                f"    Patterns: {desc.start_patterns_summary}\n"
                f"    Functions (sample): {desc.dialect_functions_sample[:8]}\n"
                f"    Confidence: {desc.confidence}, Hand-crafted: {desc.hand_crafted}\n"
            )
        user_content += "\n\nSelect and validate the best adapter for this code."

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ]

        # Tool definitions
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "load_adapter",
                    "description": "Load a dialect adapter JSON from the registry by filename.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "filename": {"type": "string",
                                         "description": "Adapter filename e.g. 'oracle.json'"}
                        },
                        "required": ["filename"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "submit_adapter",
                    "description": "Submit the validated/adjusted adapter.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "adapter": {"type": "object",
                                        "description": "Complete DialectAdapter JSON"},
                            "validation_notes": {"type": "string",
                                                  "description": "What was verified/changed"}
                        },
                        "required": ["adapter"]
                    }
                }
            },
        ]

        # Run the tool-calling loop (max 5 rounds)
        adapter = None
        loaded = None  # Track adapter loaded from registry for merge
        for _ in range(5):
            response = self.llm.call_with_tools(messages=messages, tools=tools)

            # LiteLLM returns ModelResponse — tool calls at choices[0].message
            message = response.choices[0].message
            if not message.tool_calls:
                # No tool calls — LLM may have returned text. Try to parse as adapter.
                log.warning("a0_no_tool_call", response_text=str(message.content or "")[:200])
                break

            for tc in message.tool_calls:
                fn_name = tc.function.name
                fn_args = tc.function.arguments
                if isinstance(fn_args, str):
                    try:
                        fn_args = json.loads(fn_args)
                    except json.JSONDecodeError:
                        fn_args = {}
                tc_id = tc.id

                if fn_name == "load_adapter":
                    filename = fn_args.get("filename", "")
                    loaded = self._load_adapter_from_registry(filename)
                    if loaded:
                        tool_result = json.dumps(loaded.model_dump(), default=str)
                    else:
                        tool_result = json.dumps({"error": f"Adapter '{filename}' not found in registry"})

                    # Serialize assistant message with tool calls for history
                    messages.append({
                        "role": "assistant", "content": None,
                        "tool_calls": [{
                            "id": tc_id, "type": "function",
                            "function": {"name": fn_name, "arguments": json.dumps(fn_args) if isinstance(fn_args, dict) else fn_args},
                        }],
                    })
                    messages.append({"role": "tool", "tool_call_id": tc_id, "content": tool_result})

                elif fn_name == "submit_adapter":
                    adapter_data = fn_args.get("adapter", {})
                    notes = fn_args.get("validation_notes", "")
                    log.info("a0_adapter_submitted", notes=notes[:200])
                    try:
                        adapter = DialectAdapter(**adapter_data)
                        # Merge loaded adapter's mappings — LLM often drops
                        # entries when reconstructing large mapping dicts.
                        if loaded:
                            merged = 0
                            for key, val in loaded.trino_mappings.items():
                                if key not in adapter.trino_mappings or adapter.trino_mappings[key] is None:
                                    adapter.trino_mappings[key] = val
                                    merged += 1
                            for key, val in loaded.pyspark_mappings.items():
                                if key not in adapter.pyspark_mappings or adapter.pyspark_mappings[key] is None:
                                    adapter.pyspark_mappings[key] = val
                                    merged += 1
                            # Also merge dialect_functions list
                            existing_fns = {f.upper() for f in adapter.dialect_functions}
                            for fn in loaded.dialect_functions:
                                if fn.upper() not in existing_fns:
                                    adapter.dialect_functions.append(fn)
                                    merged += 1
                            # Merge type_mappings
                            if loaded.type_mappings:
                                for key, val in loaded.type_mappings.items():
                                    if key not in (adapter.type_mappings or {}):
                                        if adapter.type_mappings is None:
                                            adapter.type_mappings = {}
                                        adapter.type_mappings[key] = val
                            if merged:
                                log.info("a0_adapter_merged_from_registry",
                                         merged_entries=merged,
                                         adapter=loaded.dialect_id)
                    except Exception as e:
                        log.warning("a0_adapter_parse_failed", error=str(e)[:200])
                    break  # Terminal tool call

            if adapter:
                break

        # Fallback: if LLM didn't produce an adapter, try README synthesis → generic
        if adapter is None:
            log.warning("a0_llm_adapter_failed",
                        msg="Falling back to README synthesis or generic adapter")
            adapter = self._synthesize_adapter_from_readme(readme_signals)

        return adapter

    def _read_head_lines(self, filepath: str, n: int = 200) -> str:
        """Read first n lines from a file."""
        try:
            lines = []
            with open(filepath, encoding="utf-8", errors="replace") as f:
                for i, line in enumerate(f):
                    if i >= n:
                        break
                    lines.append(line)
            return "".join(lines)
        except Exception as e:
            log.warning("head_read_failed", file=filepath, error=str(e)[:100])
            return ""

    def _describe_adapter_registry(self) -> list[AdapterRegistryDescription]:
        """Build lightweight descriptions of all adapters in the registry."""
        adapters_dir = Path(self.global_cfg.paths.adapters_dir)
        registry = self.cfg.adapter_registry
        descriptions = []

        for dialect_id, filename in registry.items():
            path = adapters_dir / filename
            if not path.exists():
                continue
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                pb = data.get("proc_boundary", {})
                patterns = pb.get("start_patterns", [])
                if not patterns and pb.get("start_pattern"):
                    patterns = [pb["start_pattern"]]
                patterns_summary = "; ".join(p[:60] for p in patterns[:3])
                if len(patterns) > 3:
                    patterns_summary += f" (+{len(patterns)-3} more)"

                descriptions.append(AdapterRegistryDescription(
                    filename=filename,
                    dialect_id=data.get("dialect_id", dialect_id),
                    dialect_display=data.get("dialect_display", filename),
                    start_patterns_summary=patterns_summary,
                    dialect_functions_sample=data.get("dialect_functions", [])[:10],
                    confidence=data.get("confidence", 0.0),
                    hand_crafted=data.get("hand_crafted", False),
                ))
            except Exception as e:
                log.debug("adapter_describe_failed", file=filename, error=str(e)[:100])

        return descriptions

    def _load_adapter_from_registry(self, filename: str) -> DialectAdapter | None:
        """Load an adapter JSON from the adapters directory."""
        path = Path(self.global_cfg.paths.adapters_dir) / filename
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return DialectAdapter(**data)
        except Exception as e:
            log.warning("adapter_load_failed", path=str(path), error=str(e)[:100])
            return None

    def _synthesize_adapter_from_readme(self, readme: ReadmeSignals) -> DialectAdapter:
        """Build a minimal adapter from README hints. Last resort before generic."""
        example_upper = (readme.proc_start_example or "").upper().strip()
        is_package_style = readme.has_packages or (
            example_upper and "PROCEDURE" in example_upper and "CREATE" not in example_upper
        )

        if is_package_style:
            start_pat = (
                r"(?:CREATE\s+(?:OR\s+REPLACE\s+)?(?:PROCEDURE|FUNCTION)\s+(?:\w+\.)?"
                r"|^\s*(?:PROCEDURE|FUNCTION)\s+)(\w+)"
            )
        elif readme.proc_start_example:
            start_pat = r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:PROCEDURE|FUNCTION)\s+(?:\w+\.)?(\w+)"
        else:
            start_pat = r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:PROCEDURE|FUNCTION)\s+(\w+)"

        if readme.proc_end_example and "/" in readme.proc_end_example:
            end_pat = r"END\s+\w*\s*;?\s*/$"
        elif "/" in (readme.script_delimiter or ""):
            end_pat = r"END\s+\w*\s*;?\s*/$"
        else:
            end_pat = r"\bEND\b\s*;"

        fns = readme.function_names
        trino_map = {
            f.get("name", ""): f.get("suggested_equivalent") or None
            for f in readme.custom_functions if f.get("name")
        }

        dialect_id = (readme.declared_dialect or "unknown").lower().replace(" ", "_").replace("/", "_")

        return DialectAdapter(
            dialect_id=dialect_id,
            dialect_display=readme.source_engine or readme.declared_dialect or "Unknown",
            proc_boundary=AdapterSeedProcBoundary(
                start_pattern=start_pat,
                end_pattern=end_pat,
                name_capture_group=1,
            ),
            cursor_pattern=r"\bCURSOR\b",
            exception_pattern=readme.exception_keyword or r"\bEXCEPTION\b",
            transaction_patterns=AdapterSeedTransactionPatterns(
                begin=readme.transaction_begin or r"\bBEGIN\b",
                commit=readme.transaction_commit or r"\bCOMMIT\b",
                rollback=readme.transaction_rollback or r"\bROLLBACK\b",
                end_block=r"\bEND\b",
            ),
            dialect_functions=fns,
            trino_mappings=trino_map,
            pyspark_mappings={},
            unsupported_constructs=readme.problematic_construct_names,
            temp_table_pattern=readme.temp_table_pattern,
            confidence=readme.quality_score * 0.8,  # Discount since not validated
            hand_crafted=False,
            generated_at=datetime.now(timezone.utc).isoformat(),
            validated=False,
        )

    def _a0_build_dialect_profile(
        self, readme: ReadmeSignals, adapter: DialectAdapter, run_id: str
    ) -> DialectProfile:
        """Assemble dialect_profile from readme + validated adapter."""
        return DialectProfile(
            dialect_id=adapter.dialect_id,
            dialect_display=adapter.dialect_display,
            confidence=adapter.confidence,
            resolution=(
                "ADAPTER_REGISTRY" if adapter.hand_crafted
                else "LLM_VALIDATED" if adapter.validated
                else "README_SYNTHESIZED"
            ),
            adapter_path=f"{adapter.dialect_id}.json",
            procs_to_skip=readme.procs_to_skip,
            unsupported_constructs=adapter.unsupported_constructs,
            table_mapping=readme.table_mapping,
            proc_io_tables=readme.proc_io_tables,
            input_catalog=readme.input_catalog,
            input_schema=readme.input_schema,
            output_catalog=readme.output_catalog,
            output_schema=readme.output_schema,
            output_format=readme.output_format,
            source_type=readme.source_type,
            udf_implementations=readme.udf_implementations,
            temp_table_pattern=adapter.temp_table_pattern,
            has_db_links=readme.has_db_links,
            fallback_mode=(adapter.dialect_id == "generic"),
            warnings=[],
            readme_quality_score=readme.quality_score,
            run_id=run_id,
        )

    # =========================================================================
    # A1 — Structural extraction (deterministic boundary scan)
    # =========================================================================

    def _a1_extract_structure(
        self,
        sql_file_paths: list[str],
        dialect_profile: DialectProfile,
        adapter: DialectAdapter,
    ) -> tuple[list[dict], list[str]]:
        """
        Run extract_structure.py in sandbox with validated adapter.
        Returns raw proc dicts and all table names found.
        """
        result = self.sandbox.run(
            script="extraction/extract_structure.py",
            args={
                "sql_file_paths": sql_file_paths,
                "adapter":        adapter.model_dump(),
                "procs_to_skip":  dialect_profile.procs_to_skip,
                "table_mapping":  dialect_profile.table_mapping,
            },
        )
        result.raise_on_failure()
        data = result.stdout_json

        # v10-fix (Bug 2): In Podman mode, sandbox received translated paths
        # (e.g. /source/oracle.sql). The script sets source_file to that
        # container path. Fix back to host paths so downstream code (A2
        # _read_proc_body, C1 fallback, Planning manifest) sees host paths.
        host_path_map = {Path(p).name: p for p in sql_file_paths}
        for proc in data["procs"]:
            container_name = Path(proc.get("source_file", "")).name
            if container_name in host_path_map:
                proc["source_file"] = host_path_map[container_name]

        return data["procs"], data["all_tables"]

    def _a1_split_statements(
        self,
        sql_file_paths: list[str],
        dialect_profile: DialectProfile,
    ) -> tuple[list[dict], list[str]]:
        """
        Query mode: split SQL files on ; into individual statements.
        Each statement becomes a "proc" entry with a generated name.
        """
        result = self.sandbox.run(
            script="extraction/extract_structure.py",
            args={
                "mode":           "split_statements",
                "sql_file_paths": sql_file_paths,
                "table_mapping":  dialect_profile.table_mapping,
            },
        )
        result.raise_on_failure()
        data = result.stdout_json

        # Fix container paths back to host paths (same as _a1_extract_structure)
        host_path_map = {Path(p).name: p for p in sql_file_paths}
        for proc in data["procs"]:
            container_name = Path(proc.get("source_file", "")).name
            if container_name in host_path_map:
                proc["source_file"] = host_path_map[container_name]

        if not data["procs"]:
            log.warning("a1_zero_queries_found",
                        files=sql_file_paths)

        return data["procs"], data["all_tables"]

    # =========================================================================
    # Table registry verification — fail fast on missing tables
    # =========================================================================

    def _verify_table_registry(
        self,
        table_registry: TableRegistry,
        procs: list,
        dialect_profile: DialectProfile,
    ) -> None:
        """
        Fail fast if any non-temp tables are MISSING from Trino.

        Missing tables mean:
          - Planning P3 cannot fetch schema → no column names/types for Conversion
          - Conversion LLM guesses column names → likely wrong
          - Validation drops to level 1 (syntax only) → no real correctness check

        Args:
            table_registry: resolved table registry from A2 or intelligent analysis
            procs: list of ProcEntry objects or raw dicts with tables_read/tables_written
            dialect_profile: for input_catalog/input_schema context in error message
        """
        missing = table_registry.missing_tables()
        if not missing:
            return

        total = len(table_registry.entries)
        temp_count = sum(
            1 for e in table_registry.entries.values()
            if e.status == TableStatus.TEMP_TABLE
        )
        resolved = total - len(missing) - temp_count

        # Build reverse map: missing table → which procs reference it
        table_to_procs: dict[str, list[str]] = {t: [] for t in missing}
        for proc in procs:
            if isinstance(proc, dict):
                name = proc.get("name", "?")
                all_tables = (proc.get("tables_read", [])
                              + proc.get("tables_written", []))
            else:
                name = proc.name
                all_tables = (proc.tables_read or []) + (proc.tables_written or [])

            for t in all_tables:
                if t in table_to_procs:
                    table_to_procs[t].append(name)

        # Format error
        lines = [
            f"Table resolution failed: {len(missing)} of {total} tables not found in Trino.",
            "",
            f"  Resolved:  {resolved} tables",
            f"  Temp:      {temp_count} tables (skipped — matched temp_table_pattern)",
            f"  MISSING:   {len(missing)} tables",
            "",
            "Missing tables and the procs that reference them:",
        ]
        for table_name in sorted(missing):
            referencing = table_to_procs.get(table_name, [])
            if referencing:
                lines.append(f"  • {table_name}  ← used by: {', '.join(sorted(set(referencing)))}")
            else:
                lines.append(f"  • {table_name}")

        lines.extend([
            "",
            "Current config:",
            f"  Input Catalog: {dialect_profile.input_catalog or '(empty)'}",
            f"  Input Schema:  {', '.join(dialect_profile.input_schema) if dialect_profile.input_schema else '(empty)'}",
            f"  Table Mapping: {len(dialect_profile.table_mapping)} entries",
            "",
            "Fix one of:",
            "  1. Verify Input Catalog and Input Schema in README Section 4 match your Trino lakehouse",
            "  2. Add source→trino table renames in README Section 4.2 (Table Mapping)",
            "  3. Ensure the tables exist in Trino before running the migration",
        ])

        error_msg = "\n".join(lines)
        log.error("table_resolution_failed",
                  missing=len(missing), total=total, resolved=resolved,
                  tables=missing[:10])  # Log first 10

        # Honour dependency_gate.cli_auto_action: stub_todo lets the pipeline
        # proceed with MISSING tables marked for downstream stub/TODO handling
        # instead of aborting the whole run. The original behaviour (abort)
        # remains the default for any other value.
        try:
            from sql_migration.core.config_loader import get_config
            gate_cfg = getattr(get_config(), "dependency_gate", None)
            action = getattr(gate_cfg, "cli_auto_action", None) if gate_cfg else None
        except Exception:
            action = None

        if action == "stub_todo":
            log.warning("table_resolution_missing_stubbed",
                        action=action, missing=len(missing),
                        msg="Continuing despite missing tables (cli_auto_action=stub_todo).")
            return

        raise AnalysisExtractionError(error_msg)

    # =========================================================================
    # A2 — Per-proc LLM analysis (v10 Task 2)
    # =========================================================================

    def _read_proc_body(self, source_file: str, start_line: int, end_line: int) -> str:
        """Read a proc's body from the source file using line boundaries."""
        try:
            with open(source_file, encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
            return "".join(lines[start_line:end_line + 1])
        except Exception as e:
            log.warning("proc_body_read_failed",
                        file=source_file, start=start_line, error=str(e)[:100])
            return ""

    def _a2_per_proc_loop(
        self,
        raw_procs: list[dict],
        adapter: DialectAdapter,
        dialect_profile: DialectProfile,
    ) -> list[PerProcAnalysis]:
        """
        A2: Analyze each proc/query individually via LLM.
        Sequential — rolling summary carries forward cross-proc context.

        Query mode (source_type=sql_queries): skips rolling summary and
        persistence/resume — queries are independent and fast.

        Persistence (proc mode only): each PerProcAnalysis is written to
          artifacts/00_analysis/proc_analysis_{name}.json
        after completion. On restart, existing analyses are loaded from disk.
        """
        results: list[PerProcAnalysis] = []
        rolling_summary = ""
        adapter_functions = adapter.dialect_functions[:50]
        known_proc_names = [p["name"] for p in raw_procs]
        is_query = dialect_profile.is_query_mode
        type_mappings = adapter.type_mappings or {}

        for i, proc_info in enumerate(raw_procs):
            name = proc_info["name"]
            log.info("a2_proc_start",
                     proc=name, index=i+1, total=len(raw_procs),
                     lines=proc_info["line_count"],
                     mode="query" if is_query else "proc")

            # ── Resume: check if already analyzed (proc mode only) ─────────
            if not is_query:
                existing = self._a2_load_persisted(name)
                if existing:
                    log.info("a2_proc_resumed", proc=name)
                    results.append(existing)
                    rolling_summary += f"\n{existing.summary}"
                    if len(rolling_summary) > self.cfg.rolling_summary_max_chars:
                        rolling_summary = rolling_summary[-self.cfg.rolling_summary_max_chars:]
                    continue

            # ── Read proc body from extracted file or original source ──────
            extracted_path = proc_info.get("_extracted_path", "")
            if extracted_path and Path(extracted_path).exists():
                body = Path(extracted_path).read_text(encoding="utf-8", errors="replace")
            else:
                body = self._read_proc_body(
                    proc_info["source_file"],
                    proc_info["start_line"],
                    proc_info["end_line"],
                )

            if not body.strip():
                log.warning("a2_empty_body", proc=name)
                analysis = PerProcAnalysis(
                    name=name,
                    source_file=proc_info["source_file"],
                    start_line=proc_info["start_line"],
                    end_line=proc_info["end_line"],
                    line_count=proc_info["line_count"],
                    summary=f"{name}: empty body — skipped",
                )
                results.append(analysis)
                if not is_query:
                    self._a2_persist(analysis)
                continue

            # Skip LLM for very large procs — use regex signals from A1 instead
            # (queries should never be this large, but guard anyway)
            if proc_info["line_count"] > self.cfg.per_proc_max_lines:
                log.info("a2_proc_too_large_for_llm",
                         proc=name, lines=proc_info["line_count"])
                analysis = self._a2_fallback_from_regex(proc_info)
                results.append(analysis)
                if not is_query:
                    self._a2_persist(analysis)
                    rolling_summary += f"\n{name}: large proc ({proc_info['line_count']} lines), regex-only analysis."
                continue

            analysis = self._a2_analyze_single_proc(
                proc_info=proc_info,
                proc_body=body,
                adapter_functions=adapter_functions,
                known_proc_names=known_proc_names,
                rolling_summary=rolling_summary if not is_query else "",
                source_type=dialect_profile.source_type,
                type_mappings=type_mappings,
            )
            results.append(analysis)

            # Persist (proc mode only — queries are fast, no checkpoint needed)
            if not is_query:
                self._a2_persist(analysis)

            # Accumulate rolling summary (proc mode only — queries are independent)
            if not is_query:
                rolling_summary += f"\n{analysis.summary}"
                max_chars = self.cfg.rolling_summary_max_chars
                if len(rolling_summary) > max_chars:
                    rolling_summary = rolling_summary[-max_chars:]

            log.info("a2_proc_done",
                     proc=name,
                     tables=len(analysis.tables),
                     real_tables=len(analysis.all_real_tables),
                     calls=len(analysis.calls),
                     complexity=analysis.complexity_score)

        return results

    def _a2_persist(self, analysis: PerProcAnalysis) -> None:
        """Write one per-proc analysis result to the artifact store."""
        self.store.write(
            "analysis",
            f"proc_analysis_{analysis.name}.json",
            analysis.model_dump(),
        )

    def _a2_load_persisted(self, proc_name: str) -> PerProcAnalysis | None:
        """Load a previously persisted per-proc analysis (for resume)."""
        try:
            if self.store.exists("analysis", f"proc_analysis_{proc_name}.json"):
                data = self.store.read("analysis", f"proc_analysis_{proc_name}.json")
                return PerProcAnalysis(**data)
        except Exception:
            pass
        return None

    def _a2_analyze_single_proc(
        self,
        proc_info: dict,
        proc_body: str,
        adapter_functions: list[str],
        known_proc_names: list[str],
        rolling_summary: str,
        source_type: str = "stored_procedures",
        type_mappings: dict[str, str] | None = None,
    ) -> PerProcAnalysis:
        """
        Send one proc/query to LLM with focused analysis prompt.
        Returns structured PerProcAnalysis.
        """
        name = proc_info["name"]
        is_query = source_type == "sql_queries"
        unit_label = "SQL query" if is_query else "stored procedure"
        target_label = "Trino SQL" if is_query else "PySpark/Trino SQL"

        type_map_block = ""
        if type_mappings:
            type_map_block = "\nDATA TYPE MAPPINGS (source → Trino):\n" + "\n".join(
                f"  {k} → {v}" for k, v in sorted(type_mappings.items())
            )

        if is_query:
            # ── Query mode prompt: focused on SQL rewriting ────────────────
            system_prompt = f"""You are a senior SQL analyst examining a SQL query for migration to Trino SQL.

Analyze this query and return a JSON object with these fields:

{{
  "tables": [
    {{"name": "TABLE_NAME", "classification": "REAL_TABLE|CTE_ALIAS|VIEW|TEMP_TABLE|SYSTEM_DUMMY", "used_as": "read|write|both", "notes": ""}}
  ],
  "dialect_functions_in_body": ["NVL", "DECODE", "..."],
  "has_dynamic_sql": false,
  "has_unsupported": [],
  "complexity_score": "LOW",
  "complexity_rationale": "Simple query",
  "business_purpose": "Detailed description of what this query does, including what each output column represents and how it is derived from source data. For example: 'Extracts customer contact info: CUST_MOBILE_NO is derived by taking the last 9 digits of N_F_MOBILE, CUST_VPA_HANDLE extracts the domain from V_F_FILLER_1 email field.'",
  "pipeline_stage": "extraction|transformation|loading|utility",
  "column_functions": [
    {{"column": "balance", "table": "accounts", "source_type": "NUMBER(15,2)", "functions_applied": ["NVL"], "expression": "NVL(balance, 0)", "trino_equivalent": "COALESCE(CAST(balance AS DECIMAL(15,2)), 0)"}}
  ],
  "summary": "One sentence summary"
}}

CLASSIFICATION RULES for tables:
- REAL_TABLE: Actual database table (FROM, JOIN, INSERT INTO, UPDATE, DELETE, MERGE)
- CTE_ALIAS: Defined in WITH clause — NOT a real table
- VIEW: Known view reference
- TEMP_TABLE: Matches temp patterns (GTT_, TMP_, #, SESSION.)
- SYSTEM_DUMMY: System or dummy tables used only for computed expressions — not a real data table, do not resolve

TABLE NAME FORMAT:
- Preserve the EXACT table reference as it appears in the source SQL
- Include any schema prefix: "DBO.ACCOUNTS" not just "ACCOUNTS"
- Include catalog if present: "LAKEHOUSE.CORE.ACCOUNTS"
- Do NOT strip, lowercase, or reformat the name
- This is critical — downstream MCP resolution depends on the exact format

COLUMN_FUNCTIONS RULES:
- For each column that has a dialect function applied to it, emit an entry
- Include the source data type if known from the column name or context
- Include the full source expression and the equivalent Trino SQL expression
- Focus on columns where type conversion or function mapping is needed
{type_map_block}

Return ONLY valid JSON. No prose, no markdown."""

        else:
            # ── Proc mode prompt: full stored procedure analysis ──────────
            system_prompt = f"""You are a senior SQL analyst examining a stored procedure for migration to PySpark/Trino SQL.

Analyze this procedure and return a JSON object with these fields:

{{
  "tables": [
    {{"name": "TABLE_NAME", "classification": "REAL_TABLE|CTE_ALIAS|CURSOR_NAME|VARIABLE|VIEW|TEMP_TABLE|SYSTEM_DUMMY", "used_as": "read|write|both", "notes": ""}}
  ],
  "calls": ["other_proc_name"],
  "unresolved_deps": ["unknown_function_not_in_adapter_or_known_procs"],
  "has_cursor": true/false,
  "has_dynamic_sql": true/false,
  "has_exception_handler": true/false,
  "has_unsupported": ["CONNECT BY", "..."],
  "dialect_functions_in_body": ["NVL", "DECODE", "..."],
  "complexity_score": "LOW|MEDIUM|HIGH|NEEDS_MANUAL",
  "complexity_rationale": "Evidence: 3 cursors, 2 dynamic SQL, ...",
  "business_purpose": "Detailed description of what this proc does, including what each output column represents and how it is derived from source data",
  "pipeline_stage": "extraction|transformation|loading|utility",
  "key_business_rules": ["rule1", "rule2"],
  "recommended_pyspark_approach": "DataFrame joins with window functions",
  "chunking_hint": "Natural split at line N after COMMIT",
  "column_functions": [
    {{"column": "balance", "table": "accounts", "source_type": "NUMBER(15,2)", "functions_applied": ["NVL"], "expression": "NVL(balance, 0)", "trino_equivalent": "COALESCE(CAST(balance AS DECIMAL(15,2)), 0)"}}
  ],
  "summary": "Compact 2-3 sentence summary for next proc's context"
}}

CLASSIFICATION RULES for tables:
- REAL_TABLE: Actual database table (FROM, JOIN, INSERT INTO, UPDATE, DELETE, MERGE)
- CTE_ALIAS: Defined in WITH clause — NOT a real table
- CURSOR_NAME: Declared as CURSOR — NOT a real table
- VARIABLE: Table name in a variable (dynamic SQL) — flag but don't resolve
- VIEW: Known view reference
- TEMP_TABLE: Matches temp patterns (GTT_, TMP_, #, SESSION.)
- SYSTEM_DUMMY: System or dummy tables used only for computed expressions — not a real data table, do not resolve

TABLE NAME FORMAT:
- Preserve the EXACT table reference as it appears in the source SQL
- Include any schema prefix: "DBO.ACCOUNTS" not just "ACCOUNTS"
- Include catalog if present: "LAKEHOUSE.CORE.ACCOUNTS"
- Do NOT strip, lowercase, or reformat the name
- This is critical — downstream MCP resolution depends on the exact format

COLUMN_FUNCTIONS RULES:
- For each column that has a dialect function applied to it, emit an entry
- Include the source data type if known or inferrable from context
- Include the full source expression and the equivalent Trino expression
- Focus on columns where type conversion or function mapping is needed
{type_map_block}

COMPLEXITY RULES:
- LOW: Simple SELECT/INSERT, no cursors, < 100 lines
- MEDIUM: 1-2 cursors OR dynamic SQL OR 100-300 lines
- HIGH: Nested cursors OR 3+ dynamic SQL OR 300+ lines OR complex MERGE
- NEEDS_MANUAL: 6+ unmapped constructs OR CONNECT BY OR MODEL clause

Return ONLY valid JSON. No prose, no markdown."""

        known_names_str = ", ".join(known_proc_names[:30])
        adapter_fns_str = ", ".join(adapter_functions[:40])

        if is_query:
            user_content = f"""Query: {name} (lines {proc_info['start_line']}-{proc_info['end_line']}, {proc_info['line_count']} lines)

ADAPTER DIALECT FUNCTIONS (these are known Oracle/source functions, not unresolved):
{adapter_fns_str}

--- SOURCE QUERY ---
{proc_body}
--- END SOURCE QUERY ---"""
        else:
            user_content = f"""Procedure: {name} (lines {proc_info['start_line']}-{proc_info['end_line']}, {proc_info['line_count']} lines)

KNOWN PROCS IN THIS FILE (calls to these are internal, not unresolved):
{known_names_str}

ADAPTER DIALECT FUNCTIONS (these are known, not unresolved):
{adapter_fns_str}

{"CONTEXT FROM PREVIOUS PROCS:" + chr(10) + rolling_summary if rolling_summary else ""}

--- SOURCE CODE ---
{proc_body}
--- END SOURCE CODE ---"""

        # ── SSIS enrichment: append task_context to prompt ────────────────
        if source_type == "ssis_package":
            try:
                task_ctx = self.store.read("parser", "task_context.json")
                ctx = task_ctx.get(name, {})
                if ctx:
                    ssis_lines = ["\n\nSSIS TASK CONTEXT (replicate this logic in PySpark):"]
                    if ctx.get("transformation_chain"):
                        ssis_lines.append(
                            f"  Transformation chain: {json.dumps(ctx['transformation_chain'], indent=2)}")
                    if ctx.get("column_mappings"):
                        ssis_lines.append(
                            f"  Column mappings (source→destination): {json.dumps(ctx['column_mappings'])}")
                    if ctx.get("destination"):
                        ssis_lines.append(
                            f"  Destination table: {json.dumps(ctx['destination'])}")
                    if ctx.get("ssis_expressions"):
                        ssis_lines.append(
                            f"  SSIS Expressions to convert: {json.dumps(ctx['ssis_expressions'])}")
                    if ctx.get("is_stored_proc_call"):
                        ssis_lines.append(
                            f"  Note: This is a stored procedure CALL — body not in package")
                    if ctx.get("non_sql"):
                        ssis_lines.append(
                            f"  Note: Non-SQL task — record as placeholder, no conversion needed")
                    user_content += "\n".join(ssis_lines)
            except Exception:
                pass

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ]

        try:
            raw = self.llm.call(messages=messages, expect_json=True)
            if isinstance(raw, str):
                raw = json.loads(raw)

            # Parse tables
            tables = []
            for t in raw.get("tables", []):
                tables.append(TableRef(
                    name=t.get("name", ""),
                    classification=t.get("classification", "REAL_TABLE"),
                    used_as=t.get("used_as", "read"),
                    notes=t.get("notes", ""),
                ))

            return PerProcAnalysis(
                name=name,
                source_file=proc_info["source_file"],
                start_line=proc_info["start_line"],
                end_line=proc_info["end_line"],
                line_count=proc_info["line_count"],
                tables=tables,
                calls=raw.get("calls", []),
                unresolved_deps=raw.get("unresolved_deps", []),
                has_cursor=raw.get("has_cursor", False),
                has_dynamic_sql=raw.get("has_dynamic_sql", False),
                has_exception_handler=raw.get("has_exception_handler", False),
                has_unsupported=raw.get("has_unsupported", []),
                dialect_functions_in_body=raw.get("dialect_functions_in_body", []),
                complexity_score=raw.get("complexity_score", "MEDIUM"),
                complexity_rationale=raw.get("complexity_rationale", ""),
                business_purpose=raw.get("business_purpose", ""),
                pipeline_stage=raw.get("pipeline_stage", ""),
                key_business_rules=raw.get("key_business_rules", []),
                recommended_pyspark_approach=raw.get("recommended_pyspark_approach", ""),
                chunking_hint=raw.get("chunking_hint", ""),
                column_functions=raw.get("column_functions", []),
                summary=raw.get("summary", f"{name}: analyzed"),
            )

        except Exception as e:
            log.warning("a2_llm_failed", proc=name, error=str(e)[:200])
            return self._a2_fallback_from_regex(proc_info)

    def _a2_fallback_from_regex(self, proc_info: dict) -> PerProcAnalysis:
        """Build a PerProcAnalysis from regex-extracted data when LLM fails."""
        # Use the tables_read/tables_written from extract_structure.py
        # These have the CTE confusion problem but are better than nothing
        tables = []
        for t in proc_info.get("tables_read", []):
            tables.append(TableRef(name=t, classification="REAL_TABLE", used_as="read"))
        for t in proc_info.get("tables_written", []):
            tables.append(TableRef(name=t, classification="REAL_TABLE", used_as="write"))

        return PerProcAnalysis(
            name=proc_info["name"],
            source_file=proc_info["source_file"],
            start_line=proc_info["start_line"],
            end_line=proc_info["end_line"],
            line_count=proc_info["line_count"],
            tables=tables,
            calls=proc_info.get("calls", []),
            unresolved_deps=proc_info.get("unresolved_deps", []),
            has_cursor=proc_info.get("has_cursor", False),
            has_dynamic_sql=proc_info.get("has_dynamic_sql", False),
            has_exception_handler=proc_info.get("has_exception_handler", False),
            has_unsupported=proc_info.get("has_unsupported", []),
            complexity_score="MEDIUM",
            complexity_rationale="Regex fallback — no LLM analysis",
            summary=f"{proc_info['name']}: regex-only ({proc_info['line_count']} lines)",
        )

    # =========================================================================
    # A3 — Resolve classified tables via MCP
    # =========================================================================

    def _a3_resolve_classified_tables(
        self,
        proc_analyses: list[PerProcAnalysis],
        dialect_profile: DialectProfile,
    ) -> TableRegistry:
        """
        Resolve only REAL_TABLE entries against Trino MCP.
        CTE aliases, cursor names, variables are excluded — zero false positives.
        Temp tables are marked without MCP resolution.
        """
        # Collect unique real tables and temp tables across all procs
        real_tables = set()
        temp_tables = set()
        for pa in proc_analyses:
            real_tables.update(pa.all_real_tables)
            temp_tables.update(pa.temp_tables)

        log.info("a3_table_classification",
                 real=len(real_tables),
                 temp=len(temp_tables),
                 filtered_out=sum(
                     len([t for t in pa.tables
                          if t.classification not in ("REAL_TABLE", "TEMP_TABLE")])
                     for pa in proc_analyses))

        # Reuse existing MCP resolution logic for real tables only
        all_tables = sorted(real_tables | temp_tables)
        return self._a2_resolve_tables(
            all_tables=all_tables,
            table_mapping=dialect_profile.table_mapping,
            temp_table_pattern=dialect_profile.temp_table_pattern,
            input_catalog=dialect_profile.input_catalog,
            input_schemas=dialect_profile.input_schema,
        )

    # =========================================================================
    # A4 — Synthesis (merge per-proc results into final artifacts)
    # =========================================================================

    def _a4_synthesize(
        self,
        proc_analyses: list[PerProcAnalysis],
        table_registry: TableRegistry,
        construct_map: ConstructMap,
        dialect_profile: DialectProfile,
        adapter: DialectAdapter,
        run_id: str,
    ) -> AnalysisAgentOutput:
        """
        Merge all per-proc LLM results into the 8+ artifacts that Planning expects.
        Same schemas as before — downstream agents don't change.

        Query mode: dependency graph is empty, complexity all LOW,
        conversion order is sequential.
        """
        is_query = dialect_profile.is_query_mode

        # ── Build manifest from per-proc results ──────────────────────────
        procs = []
        for pa in proc_analyses:
            entry = ProcEntry(
                name=pa.name,
                source_file=pa.source_file,
                start_line=pa.start_line,
                end_line=pa.end_line,
                line_count=pa.line_count,
                tables_read=pa.real_tables_read,
                tables_written=pa.real_tables_written,
                calls=pa.calls,
                unresolved_deps=pa.unresolved_deps,
                has_cursor=pa.has_cursor,
                has_dynamic_sql=pa.has_dynamic_sql,
                has_exception_handler=pa.has_exception_handler,
                has_unsupported=pa.has_unsupported,
                dialect_functions_in_body=pa.dialect_functions_in_body,
                chunking_hint=pa.chunking_hint,
            )
            procs.append(entry)

        manifest = Manifest(
            procs=procs,
            source_files=sorted(set(pa.source_file for pa in proc_analyses)),
            dialect_id=dialect_profile.dialect_id,
        )
        self.store.write("analysis", "manifest.json", manifest.model_dump())
        log.info("a4_manifest_written",
                 total_procs=manifest.total_procs,
                 total_lines=manifest.total_lines)

        # ── Build dependency graph ────────────────────────────────────────
        if is_query:
            # Queries are independent — no deps, sequential order
            dep_graph = DependencyGraph()
            conv_order = ConversionOrder(
                order=[pa.name for pa in proc_analyses])
        elif dialect_profile.is_ssis_mode:
            # SSIS: dependency graph from Parser's scheduling_graph
            try:
                sched = self.store.read("parser", "scheduling_graph.json")
                sched_edges = [tuple(e) for e in sched.get("edges", [])]
                sched_order = sched.get("conversion_order", [])
                # Filter to only include tasks that are in our manifest
                known = {pa.name for pa in proc_analyses}
                filtered_edges = [(a, b) for a, b in sched_edges
                                  if a in known and b in known]
                filtered_order = [n for n in sched_order if n in known]
                # Append any tasks not in the scheduling graph order
                for pa in proc_analyses:
                    if pa.name not in filtered_order:
                        filtered_order.append(pa.name)
                dep_graph = DependencyGraph(edges=filtered_edges)
                conv_order = ConversionOrder(order=filtered_order)
                log.info("a4_ssis_graph_from_parser",
                         edges=len(filtered_edges),
                         order=len(filtered_order))
            except Exception as e:
                log.warning("a4_ssis_scheduling_graph_fallback",
                            error=str(e)[:200])
                dep_graph = DependencyGraph()
                conv_order = ConversionOrder(
                    order=[pa.name for pa in proc_analyses])
        else:
            known_names = {pa.name for pa in proc_analyses}
            edges = []
            external_deps: dict[str, list[str]] = {}
            for pa in proc_analyses:
                for callee in pa.calls:
                    if callee in known_names:
                        edges.append((pa.name, callee))
                if pa.unresolved_deps:
                    external_deps[pa.name] = pa.unresolved_deps

            dep_result = self.sandbox.run(
                script="extraction/build_graph.py",
                args={
                    "mode": "dependency_graph",
                    "procs": [{"name": pa.name, "calls": pa.calls} for pa in proc_analyses],
                },
            )
            if dep_result.success:
                dep_data = dep_result.stdout_json
                dep_graph = DependencyGraph(
                    edges=[tuple(e) for e in dep_data.get("edges", [])],
                    cycles=dep_data.get("cycles", []),
                    external_deps={**dep_data.get("external_deps", {}), **external_deps},
                )
                conv_order = ConversionOrder(
                    order=dep_data.get("conversion_order", [pa.name for pa in proc_analyses]),
                    has_cycles=dep_data.get("has_cycles", False),
                    cycle_warning=dep_data.get("cycle_warning", ""),
                )
            else:
                log.warning("a4_graph_build_failed", stderr=dep_result.stderr[:200])
                dep_graph = DependencyGraph(
                    edges=edges, external_deps=external_deps)
                conv_order = ConversionOrder(
                    order=[pa.name for pa in proc_analyses])

        self.store.write("analysis", "dependency_graph.json", dep_graph.model_dump())
        self.store.write("analysis", "conversion_order.json", conv_order.model_dump())
        log.info("a4_graph_written",
                 edges=len(dep_graph.edges),
                 cycles=len(dep_graph.cycles),
                 order=len(conv_order.order))

        # ── Build complexity report ───────────────────────────────────────
        from sql_migration.models.analysis import ComplexityEntry, ComplexityReport
        from sql_migration.models.common import ComplexityScore

        score_map = {
            "LOW": ComplexityScore.LOW,
            "MEDIUM": ComplexityScore.MEDIUM,
            "HIGH": ComplexityScore.HIGH,
            "NEEDS_MANUAL": ComplexityScore.NEEDS_MANUAL,
        }
        complexity_entries = []
        for pa in proc_analyses:
            if is_query:
                # Queries are always LOW — no cursors, no chunking
                complexity_entries.append(ComplexityEntry(
                    name=pa.name,
                    score=ComplexityScore.LOW,
                    rationale="SQL query — single statement",
                ))
            else:
                complexity_entries.append(ComplexityEntry(
                    name=pa.name,
                    score=score_map.get(pa.complexity_score, ComplexityScore.MEDIUM),
                    rationale=pa.complexity_rationale,
                ))
        complexity_report = ComplexityReport(scores=complexity_entries)
        self.store.write("analysis", "complexity_report.json",
                         complexity_report.model_dump())

        # ── Build semantic map ────────────────────────────────────────────
        from sql_migration.models.semantic import (
            PipelineSemanticMap, ProcSemanticEntry, PipelineStage,
            DiscoveredDependency,
        )

        proc_summaries = []
        for pa in proc_analyses:
            proc_summaries.append(ProcSemanticEntry(
                proc_name=pa.name,
                business_purpose=pa.business_purpose,
                stage=pa.pipeline_stage,
                key_business_rules=pa.key_business_rules,
                recommended_pyspark_approach=pa.recommended_pyspark_approach,
            ))

        stages_map: dict[str, list[str]] = {}
        for pa in proc_analyses:
            stage = pa.pipeline_stage or "utility"
            stages_map.setdefault(stage, []).append(pa.name)
        stages = [
            PipelineStage(stage_name=s, procs=p)
            for s, p in stages_map.items()
        ]

        discovered = []
        if not is_query:
            dep_callers: dict[str, list[str]] = {}
            for pa in proc_analyses:
                for dep in pa.unresolved_deps:
                    dep_callers.setdefault(dep, []).append(pa.name)
            for dep_name, callers in dep_callers.items():
                discovered.append(DiscoveredDependency(
                    name=dep_name,
                    called_by=callers,
                    resolution="UNRESOLVED",
                ))

        unit_label = "queries" if is_query else "procedures"
        semantic_map = PipelineSemanticMap(
            pipeline_intent=(
                f"Migration of {len(proc_analyses)} Oracle {unit_label}. "
                f"Stages: {', '.join(stages_map.keys())}."
            ),
            proc_summaries=proc_summaries,
            procs_analyzed=len(proc_analyses),
            stages=stages,
            discovered_dependencies=discovered,
        )
        self.store.write("analysis", "semantic_map.json", semantic_map.model_dump())

        # ── Write column_analysis.json (both modes) ───────────────────────
        # Aggregate column-level function usage from all per-proc analyses.
        # Used by Conversion Agent for precise function+type mapping.
        all_column_fns = []
        for pa in proc_analyses:
            for cf in pa.column_functions:
                cf_copy = dict(cf)
                cf_copy["source_proc"] = pa.name
                all_column_fns.append(cf_copy)

        if all_column_fns:
            self.store.write("analysis", "column_analysis.json", {
                "total_entries": len(all_column_fns),
                "entries": all_column_fns,
            })
            log.info("a4_column_analysis_written",
                     entries=len(all_column_fns))

        # ── Enrich construct_map with newly discovered functions ──────────
        discovered_fns = set()
        for pa in proc_analyses:
            for fn in pa.dialect_functions_in_body:
                fn_upper = fn.upper()
                if (fn_upper not in construct_map.trino_map
                        and fn_upper not in construct_map.pyspark_map
                        and fn_upper not in construct_map.unmapped):
                    discovered_fns.add(fn_upper)

        if discovered_fns:
            construct_map.unmapped.extend(sorted(discovered_fns))
        # Always rewrite construct_map to include type_mappings
        self.store.write("analysis", "construct_map.json",
                         construct_map.model_dump())
        if discovered_fns:
            log.info("a4_construct_map_enriched",
                     new_unmapped=len(discovered_fns),
                     total_unmapped=len(construct_map.unmapped))

        log.info("a4_synthesis_complete", run_id=run_id,
                 mode="query" if is_query else "proc")

        return AnalysisAgentOutput(
            manifest=manifest,
            table_registry=table_registry,
            dependency_graph=dep_graph,
            conversion_order=conv_order,
            complexity_report=complexity_report,
        )

    # =========================================================================
    # OLD: Intelligent analysis result unpacking (v9 — replaced by A4)
    # Kept commented for reference.
    # =========================================================================

    # def _unpack_intelligent_results(
    #     self, analysis_data, agent_input, dialect_profile, adapter,
    #     raw_procs, all_tables, run_id,
    # ) -> AnalysisAgentOutput:
    #     """Unpack LLM intelligent analysis results into artifact models."""
    #     manifest_data = analysis_data.get("manifest", {})
    #     manifest_data.setdefault("dialect_id", dialect_profile.dialect_id)
    #     manifest_data.setdefault("source_files", agent_input.sql_file_paths)
    #     procs = []
    #     for p in manifest_data.get("procs", []):
    #         try:
    #             procs.append(ProcEntry(**p))
    #         except Exception as e:
    #             log.warning("proc_entry_parse_error",
    #                         proc=p.get("name", "?"), error=str(e)[:100])
    #     if not procs:
    #         return self._run_regex_fallback(
    #             agent_input, dialect_profile, adapter, raw_procs, all_tables, run_id)
    #     manifest = Manifest(
    #         procs=procs,
    #         source_files=manifest_data.get("source_files", agent_input.sql_file_paths),
    #         dialect_id=manifest_data.get("dialect_id", ""),
    #     )
    #     self.store.write("analysis", "manifest.json", manifest.model_dump())
    #     table_reg_data = analysis_data.get("table_registry", {})
    #     try:
    #         table_registry = TableRegistry(**table_reg_data)
    #     except Exception as e:
    #         table_registry = TableRegistry()
    #     self.store.write("analysis", "table_registry.json", table_registry.model_dump())
    #     self._verify_table_registry(table_registry, procs, dialect_profile)
    #     dep_graph_data = analysis_data.get("dependency_graph", {})
    #     semantic_data = analysis_data.get("semantic_map", {})
    #     discovered = semantic_data.get("discovered_dependencies", [])
    #     if discovered:
    #         ext_deps = dep_graph_data.setdefault("external_deps", {})
    #         for dep in discovered:
    #             dep_name = dep.get("name", "")
    #             if not dep_name:
    #                 continue
    #             for caller in dep.get("called_by", []):
    #                 if caller not in ext_deps:
    #                     ext_deps[caller] = []
    #                 if dep_name not in ext_deps[caller]:
    #                     ext_deps[caller].append(dep_name)
    #     try:
    #         dep_graph = DependencyGraph(**dep_graph_data)
    #     except Exception:
    #         dep_graph = DependencyGraph()
    #     self.store.write("analysis", "dependency_graph.json", dep_graph.model_dump())
    #     conv_order_data = analysis_data.get("conversion_order", {})
    #     try:
    #         conv_order = ConversionOrder(**conv_order_data)
    #     except Exception:
    #         conv_order = ConversionOrder(order=[p.name for p in procs])
    #     self.store.write("analysis", "conversion_order.json", conv_order.model_dump())
    #     complexity_data = analysis_data.get("complexity_report", {})
    #     try:
    #         complexity_report = ComplexityReport(**complexity_data)
    #     except Exception:
    #         complexity_report = ComplexityReport()
    #     self.store.write("analysis", "complexity_report.json", complexity_report.model_dump())
    #     from sql_migration.models.semantic import PipelineSemanticMap
    #     try:
    #         semantic_map = PipelineSemanticMap(**semantic_data)
    #     except Exception:
    #         semantic_map = PipelineSemanticMap(analysis_notes="Partial semantic map")
    #     self.store.write("analysis", "semantic_map.json", semantic_map.model_dump())
    #     return AnalysisAgentOutput(
    #         manifest=manifest, table_registry=table_registry,
    #         dependency_graph=dep_graph, conversion_order=conv_order,
    #         complexity_report=complexity_report,
    #     )

    # =========================================================================
    # Regex fallback — the original A1→A5 pipeline
    # v10 Task 2: No longer called from run(). Kept for debugging and as
    # emergency fallback if per-proc LLM analysis needs to be disabled.
    # Individual proc fallbacks are handled by _a2_fallback_from_regex().
    # =========================================================================

    def _run_regex_fallback(
        self, agent_input, dialect_profile, adapter,
        raw_procs, all_tables, run_id,
    ) -> AnalysisAgentOutput:
        """
        Original regex-based analysis pipeline. Called when intelligent
        analysis fails. raw_procs and all_tables already extracted by A1.
        """
        log.info("regex_fallback_start", run_id=run_id)

        # A2: Table resolution
        table_registry = self._a2_resolve_tables(
            all_tables, dialect_profile.table_mapping, dialect_profile.temp_table_pattern,
            input_catalog=dialect_profile.input_catalog,
            input_schemas=dialect_profile.input_schema,
        )
        self.store.write("analysis", "table_registry.json", table_registry.model_dump())

        # Fail fast if tables are missing from Trino
        self._verify_table_registry(table_registry, raw_procs, dialect_profile)

        # A3: Dependency graph
        dep_graph, conv_order = self._a3_build_graph(raw_procs)
        self.store.write("analysis", "dependency_graph.json", dep_graph.model_dump())
        self.store.write("analysis", "conversion_order.json", conv_order.model_dump())

        # A4: Deep extraction
        procs = self._a4_deep_extract(raw_procs, adapter, agent_input.sql_file_paths)

        # Build manifest
        manifest = Manifest(
            procs=procs,
            source_files=agent_input.sql_file_paths,
            dialect_id=dialect_profile.dialect_id,
        )
        self.store.write("analysis", "manifest.json", manifest.model_dump())

        # A5: Complexity scoring
        complexity_report = self._a5_score_complexity(
            procs, dialect_profile.unsupported_constructs, dep_graph.cycles
        )
        self.store.write("analysis", "complexity_report.json", complexity_report.model_dump())

        # Empty semantic map (no semantic understanding in fallback)
        from sql_migration.models.semantic import PipelineSemanticMap
        empty_map = PipelineSemanticMap(analysis_notes="Regex fallback — no semantic analysis")
        self.store.write("analysis", "semantic_map.json", empty_map.model_dump())

        log.info("analysis_complete", run_id=run_id, method="regex_fallback")

        return AnalysisAgentOutput(
            manifest=manifest,
            table_registry=table_registry,
            dependency_graph=dep_graph,
            conversion_order=conv_order,
            complexity_report=complexity_report,
        )

    # =========================================================================
    # A1 verification (unchanged from v9)
    # =========================================================================

    def _verify_extraction(
        self, raw_procs, sql_file_paths, fallback_mode, adapter,
    ) -> None:
        """Sanity-check the boundary extraction result."""
        from pathlib import Path as _Path

        if not raw_procs:
            detail = (
                "proc_boundary.start_pattern found 0 proc declarations in all source files.\n"
                f"  Adapter dialect:    {adapter.dialect_id}\n"
                f"  Adapter start_pat:  {adapter.proc_boundary.start_pattern}\n"
                f"  Source files:       {sql_file_paths}\n"
                "Fix: check adapter proc_boundary patterns against your source files."
            )
            log.error("a1_zero_procs_extracted", detail=detail)
            raise AnalysisExtractionError(f"A1 extracted 0 procs.\n{detail}")

        total_file_lines = sum(_count_lines(fp) for fp in sql_file_paths)
        warn_threshold = max(500, total_file_lines // 4)

        line_counts = [p.get("line_count", 0) for p in raw_procs if p.get("line_count", 0) > 0]
        median_size = 0
        if len(line_counts) >= 3:
            sorted_lc = sorted(line_counts)
            mid = len(sorted_lc) // 2
            median_size = (
                sorted_lc[mid] if len(sorted_lc) % 2 == 1
                else (sorted_lc[mid - 1] + sorted_lc[mid]) // 2
            )

        for p in raw_procs:
            lc = p.get("line_count", 0)
            if lc > warn_threshold:
                log.warning("a1_suspicious_proc_size",
                            proc=p["name"], line_count=lc,
                            total_file_lines=total_file_lines)
            elif median_size > 0 and lc > 3 * median_size and lc > 50:
                log.warning("a1_proc_size_outlier",
                            proc=p["name"], line_count=lc, median_size=median_size)

        if total_file_lines > 0:
            covered_lines = sum(p.get("line_count", 0) for p in raw_procs)
            coverage_pct = covered_lines / total_file_lines
            if coverage_pct < 0.40:
                log.warning("a1_low_file_coverage",
                            procs_found=len(raw_procs), coverage_pct=round(coverage_pct, 2))

        if fallback_mode:
            log.warning("a1_running_with_fallback_adapter", procs_found=len(raw_procs))

        log.info("a1_verification_complete", procs_found=len(raw_procs))

    # =========================================================================
    # A2 — Table resolution (unchanged from v9)
    # =========================================================================

    def _a2_resolve_tables(
        self, all_tables, table_mapping, temp_table_pattern,
        input_catalog="", input_schemas=None,
    ) -> TableRegistry:
        """Resolve every table against Trino MCP."""
        import re
        registry = TableRegistry()
        temp_re = re.compile(temp_table_pattern, re.IGNORECASE) if temp_table_pattern else None
        schemas = input_schemas or []

        for source_name in all_tables:
            source_upper = source_name.strip().upper()
            trino_name = table_mapping.get(source_upper, table_mapping.get(
                source_name, source_name))

            if temp_re and temp_re.search(source_name):
                registry.entries[source_name] = TableRegistryEntry(
                    source_name=source_name, trino_fqn=trino_name,
                    status=TableStatus.TEMP_TABLE, is_temp=True,
                )
                continue

            try:
                parts = trino_name.split(".")
                if len(parts) >= 3:
                    fqn = trino_name
                    schema_to_query = parts[1]      # Just schema — MCP doesn't accept catalog.schema
                    table = parts[2]
                elif len(parts) == 2:
                    schema_to_query = parts[0]
                    table = parts[1]
                    fqn = f"{input_catalog}.{parts[0]}.{parts[1]}" if input_catalog else trino_name
                else:
                    table = parts[0]
                    fqn = trino_name
                    schema_to_query = None
                    if schemas and input_catalog:
                        for s in schemas:
                            candidate_fqn = f"{input_catalog}.{s}.{table}"
                            try:
                                available = self.mcp.list_tables(schema=s)
                                if table.lower() in [t.lower() for t in available]:
                                    fqn = candidate_fqn
                                    schema_to_query = s
                                    break
                            except Exception:
                                continue
                    if schema_to_query is None:
                        schema_to_query = "default"
                        if input_catalog:
                            fqn = f"{input_catalog}.default.{table}"

                available = self.mcp.list_tables(schema=schema_to_query)
                available_lower = [t.lower() for t in available]

                if table.lower() in available_lower:
                    cols_raw = self.mcp.desc_table(table=fqn)
                    columns = [
                        ColumnInfo(
                            name=c.get("name", c.get("Column", "")),
                            type=c.get("type", c.get("Type", "")),
                            nullable=str(c.get("nullable", c.get("Null", ""))),
                            comment=c.get("comment", c.get("Comment", "")),
                        )
                        for c in cols_raw
                    ]
                    registry.entries[source_name] = TableRegistryEntry(
                        source_name=source_name, trino_fqn=fqn,
                        status=TableStatus.EXISTS_IN_TRINO, columns=columns,
                    )
                else:
                    registry.entries[source_name] = TableRegistryEntry(
                        source_name=source_name, trino_fqn=fqn,
                        status=TableStatus.MISSING,
                    )
            except Exception as e:
                log.warning("table_resolution_error", table=source_name, error=str(e))
                registry.entries[source_name] = TableRegistryEntry(
                    source_name=source_name, trino_fqn=trino_name,
                    status=TableStatus.MISSING,
                )

        return registry

    # =========================================================================
    # A3 — Dependency graph (unchanged)
    # =========================================================================

    def _a3_build_graph(self, raw_procs):
        result = self.sandbox.run(
            script="extraction/build_graph.py",
            args={"mode": "dependency_graph", "procs": raw_procs},
        )
        result.raise_on_failure()
        data = result.stdout_json
        dep_graph = DependencyGraph(
            edges=[tuple(e) for e in data.get("edges", [])],
            cycles=data.get("cycles", []),
            external_deps=data.get("external_deps", {}),
        )
        conv_order = ConversionOrder(
            order=data.get("conversion_order", [p["name"] for p in raw_procs]),
            has_cycles=data.get("has_cycles", False),
            cycle_warning=data.get("cycle_warning", ""),
        )
        return dep_graph, conv_order

    # =========================================================================
    # A4 — Deep extraction (unchanged)
    # =========================================================================

    def _a4_deep_extract(self, raw_procs, adapter, sql_file_paths):
        procs = []
        for raw in raw_procs:
            entry = ProcEntry(**{k: v for k, v in raw.items() if k in ProcEntry.model_fields})
            if entry.needs_deep_extract():
                deep = self._run_deep_extract(entry, adapter)
                if deep:
                    entry.cursor_nesting_depth = deep.get("cursor_nesting_depth")
                    entry.dynamic_sql_patterns_found = deep.get("dynamic_sql_patterns_found", [])
                    entry.unsupported_occurrences = deep.get("unsupported_occurrences", {})
                    entry.dialect_functions_in_body = deep.get("dialect_functions_in_body", [])
            procs.append(entry)
        return procs

    def _run_deep_extract(self, entry, adapter):
        result = self.sandbox.run(
            script="extraction/build_graph.py",
            args={
                "mode": "deep_extract", "proc_name": entry.name,
                "source_file": entry.source_file, "start_line": entry.start_line,
                "end_line": entry.end_line, "adapter": adapter.model_dump(),
            },
        )
        if not result.success:
            log.warning("deep_extract_failed", proc=entry.name, stderr=result.stderr[:200])
            return None
        return result.stdout_json

    # =========================================================================
    # A5 — LLM complexity scoring (unchanged)
    # =========================================================================

    def _a5_score_complexity(self, procs, unsupported_constructs, circular_deps):
        _, messages = build_llm_complexity_input(
            procs=procs, unsupported_constructs=unsupported_constructs,
            circular_deps=circular_deps,
        )
        raw = self.llm.call(messages=messages, expect_json=True)
        return parse_llm_complexity_output(raw)


# ============================================================================
# Module-level helpers
# ============================================================================

def _build_construct_map(dialect_id: str, adapter: DialectAdapter) -> ConstructMap:
    """Build the flat lookup table from adapter mappings."""
    trino_map = {}
    pyspark_map = {}
    unmapped = []

    for fn in adapter.dialect_functions:
        fn_upper = fn.upper()
        t_eq = adapter.trino_mappings.get(fn) or adapter.trino_mappings.get(fn_upper)
        p_eq = adapter.pyspark_mappings.get(fn) or adapter.pyspark_mappings.get(fn_upper)
        if t_eq:
            trino_map[fn_upper] = t_eq
        else:
            unmapped.append(fn_upper)
        if p_eq:
            pyspark_map[fn_upper] = p_eq

    return ConstructMap(
        dialect_id=dialect_id,
        trino_map=trino_map,
        pyspark_map=pyspark_map,
        unmapped=unmapped,
        type_mappings=adapter.type_mappings or {},
    )


def _persist_adapter(adapter: DialectAdapter, adapters_dir: str) -> None:
    """Write adapter to adapters/{dialect_id}.json for future reuse."""
    path = Path(adapters_dir)
    path.mkdir(parents=True, exist_ok=True)
    filename = path / f"{adapter.dialect_id}.json"
    try:
        filename.write_text(
            json.dumps(adapter.model_dump(), indent=2, default=str),
            encoding="utf-8",
        )
        log.info("adapter_persisted", path=str(filename))
    except Exception as e:
        log.error("adapter_persist_failed", error=str(e))