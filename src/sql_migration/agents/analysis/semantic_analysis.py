"""
semantic_analysis.py (v9 — DISABLED in v10 Task 2)
=====================================================
Replaced by per-proc LLM analysis in agent.py A2 loop.
The old approach sent file paths to one giant LLM call with 80 tool calls.
The new approach sends each proc body to the LLM individually with rolling summary.

Kept commented for reference. To re-enable, uncomment and restore the
run_intelligent_analysis() call in agent.py run() method.
"""

# """
# semantic_analysis.py  (v9 — Intelligent Analysis Agent)
# =======================================================
# LLM-driven analysis with tool-calling. Replaces the entire A1-A5 chain
# with a single intelligent pass. The LLM uses sandbox parsers, reads code,
# queries Trino, discovers unknown UDFs, and builds semantic understanding.
# 
# Output artifacts (format-compatible with downstream agents):
#   manifest.json, semantic_map.json, table_registry.json,
#   dependency_graph.json, conversion_order.json, complexity_report.json
# """
# from __future__ import annotations
# import json
# from sql_migration.core.config_loader import get_config
# from sql_migration.core.llm_client import LLMClient
# from sql_migration.core.logger import get_logger
# from sql_migration.core.tool_executor import INTELLIGENT_ANALYSIS_TOOLS, ToolExecutor
# 
# log = get_logger("intelligent_analysis")
# 
# _SYSTEM_PROMPT = """\
# You are a senior data engineer performing a complete analysis of SQL source files
# before migrating them to PySpark / Trino SQL on a Starburst Trino lakehouse.
# 
# You have tools: parse_file, find_all_calls, search_code, read_lines, query_trino,
# build_dependency_graph, submit_analysis.
# 
# WORKFLOW:
# 1. PARSE: Call parse_file for each source file. Review parse_warnings. If proc
#    sizes look wrong or coverage is low, use read_lines to inspect and fix boundaries.
# 2. DISCOVER DEPS: Call find_all_calls for each file. Compare against known proc names.
#    Anything that's not a proc, SQL keyword, or dialect function is UNRESOLVED — flag it.
#    Use search_code to check if the function is defined elsewhere.
# 3. RESOLVE TABLES: For each table, use query_trino("DESCRIBE catalog.schema.table")
#    to verify existence and get columns. Try SHOW TABLES for ambiguous names.
# 4. DEPENDENCY GRAPH: Call build_dependency_graph with proc names and their calls.
# 5. SEMANTIC ANALYSIS: Read key procs (output writers first, trace backwards). Determine:
#    business purpose, pipeline stage, historical patterns, complexity (LOW/MEDIUM/HIGH/NEEDS_MANUAL).
#    Simple procs (<50 lines, straight SELECT/INSERT) score from parser output alone.
# 6. SUBMIT: Call submit_analysis with the complete structured output.
# 
# RULES:
# - If parse_file output looks wrong, FIX IT by reading the code.
# - Do NOT invent names — only report what you find.
# - Every unresolved dep goes in discovered_dependencies.
# - Complexity scores need evidence (line count, cursors, dynamic SQL, etc.)
# """
# 
# def _build_user_prompt(sql_file_paths, readme_signals, dialect_profile, adapter):
#     readme_ctx = ""
#     if readme_signals:
#         readme_ctx = (
#             f"\nREADME: engine={readme_signals.get('source_engine','?')}, "
#             f"dialect={readme_signals.get('declared_dialect','?')}, "
#             f"domain={readme_signals.get('business_domain','')}, "
#             f"packages={readme_signals.get('has_packages',False)}, "
#             f"skip={readme_signals.get('procs_to_skip',[])}, "
#             f"table_map={json.dumps(readme_signals.get('table_mapping',{}))}\n"
#         )
#     adapter_ctx = ""
#     if adapter:
#         adapter_ctx = (
#             f"\nADAPTER: functions={adapter.get('dialect_functions',[])[:20]}, "
#             f"unsupported={adapter.get('unsupported_constructs',[])}\n"
#         )
#     files = "\n".join(f"  {f}" for f in sql_file_paths)
#     return f"""Analyze these SQL source files completely.
# {readme_ctx}{adapter_ctx}
# SOURCE FILES:
# {files}
# 
# Call parse_file for each file, then follow the workflow. Call submit_analysis when done.
# 
# OUTPUT SCHEMA for submit_analysis({{analysis: ...}}):
# {{
#   "manifest": {{
#     "procs": [
#       {{"name":"str","source_file":"str","start_line":0,"end_line":100,"line_count":101,
#         "tables_read":["t1"],"tables_written":["t2"],"calls":["other_proc"],
#         "unresolved_deps":["unknown_udf"],"has_cursor":false,"has_dynamic_sql":false,
#         "has_exception_handler":false,"has_unsupported":[],"dialect_functions_in_body":["NVL"]}}
#     ],
#     "source_files": ["{sql_file_paths[0] if sql_file_paths else ''}"],
#     "dialect_id": "oracle"
#   }},
#   "table_registry": {{
#     "entries": {{
#       "table_name": {{
#         "source_name":"table_name","trino_fqn":"catalog.schema.table",
#         "status":"EXISTS_IN_TRINO|MISSING|TEMP_TABLE",
#         "columns":[{{"name":"col","type":"bigint","nullable":"YES"}}],
#         "is_temp":false
#       }}
#     }}
#   }},
#   "dependency_graph": {{
#     "edges":[["caller","callee"]],"cycles":[],"external_deps":{{"proc":["udf"]}}
#   }},
#   "conversion_order": {{
#     "order":["leaf","mid","top"],"has_cycles":false,"cycle_warning":""
#   }},
#   "complexity_report": {{
#     "scores": [
#       {{"name":"proc","score":"LOW|MEDIUM|HIGH|NEEDS_MANUAL",
#         "rationale":"evidence","priority_flag":false,"shared_utility_candidate":false}}
#     ]
#   }},
#   "semantic_map": {{
#     "pipeline_intent":"what this pipeline does",
#     "pipeline_complexity":"HIGH — reason",
#     "proc_summaries": [
#       {{"proc_name":"name","business_purpose":"what it does","stage":"extraction|transformation|loading|utility",
#         "inputs_description":"","outputs_description":"","key_business_rules":[],"historical_patterns":[],
#         "recommended_pyspark_approach":""}}
#     ],
#     "stages": [{{"stage_name":"name","description":"","procs":[],"inputs":[],"outputs":[]}}],
#     "data_flow": [{{"table_name":"TMP_X","produced_by":"proc_a","consumed_by":["proc_b"],
#       "nature":"intermediate","recommendation":"Convert to DataFrame variable"}}],
#     "target_architecture": {{
#       "summary":"","modules":[],"rationale":"","design_notes":[]
#     }},
#     "discovered_dependencies": [
#       {{"name":"unknown_udf","called_by":["proc1"],"call_count":5,
#         "sample_lines":[42],"resolution":"UNRESOLVED"}}
#     ]
#   }}
# }}"""
# 
# def run_intelligent_analysis(store, sandbox, mcp, sql_file_paths, run_id="",
#                               readme_signals=None, dialect_profile=None, adapter=None):
#     """
#     Run LLM-driven intelligent analysis. Returns the complete analysis dict.
#     On failure: returns partial result with error information.
#     """
#     llm = LLMClient(agent="analysis")
#     executor = ToolExecutor(
#         sandbox=sandbox, mcp=mcp, store=store, source_files=sql_file_paths,
#     )
#     messages = [
#         {"role": "system", "content": _SYSTEM_PROMPT},
#         {"role": "user", "content": _build_user_prompt(
#             sql_file_paths, readme_signals, dialect_profile, adapter,
#         )},
#     ]
#     log.info("intelligent_analysis_start", files=len(sql_file_paths), run_id=run_id)
# 
#     result = executor.run_agentic_loop(
#         llm_client=llm, messages=messages,
#         tools=INTELLIGENT_ANALYSIS_TOOLS,
#         max_tool_calls=80, max_turns=35,
#         call_id=f"analysis_{run_id}",
#     )
# 
#     if result.success and result.terminal_data:
#         log.info("intelligent_analysis_complete",
#                  tool_calls=result.tool_calls_made,
#                  tokens=result.total_input_tokens + result.total_output_tokens)
#         return result.terminal_data
#     else:
#         log.error("intelligent_analysis_failed",
#                   error=result.error[:300], tool_calls=result.tool_calls_made)
#         return result.terminal_data if isinstance(result.terminal_data, dict) else {
#             "error": result.error, "tool_calls_made": result.tool_calls_made,
#         }

