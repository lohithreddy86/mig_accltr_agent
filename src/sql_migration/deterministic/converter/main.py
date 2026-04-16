"""
Oracle PL/SQL → PySpark Conversion Pipeline

Main orchestrator that ties together:
1. PL/SQL Procedure Splitter
2. SQL Statement Extractor + Classifier
3. Router (complexity analysis → path selection)
4. SQLGlot Transpiler (Oracle → Spark SQL)
5. IR-based Code Generator (tree-walking)
6. Confidence Scorer

Usage:
    python -m converter.main script.txt --output output/
    python -m converter.main script.txt --output output/ --path regex
    python -m converter.main script.txt --output output/ --path auto
"""

import os
import sys
import json
import argparse
from datetime import datetime

from .plsql_splitter import split_package, extract_procedure_body
from .sql_extractor import extract_statements, get_statement_summary, StatementType, set_status_table_pattern
from .code_generator import generate_procedure, generate_orchestrator
from .ir_code_generator import generate_procedure_from_ir
from .regex_to_ir import statements_to_ir
from .router import (
    ConversionPath, analyze_complexity, select_path,
)
from .confidence_scorer import score_procedure, generate_summary_report, ConfidenceReport, rescore_with_llm_data
from .status_tracker import generate_status_tracker
from .ir_code_generator import set_llm_bridge


def _detect_package_name(source: str) -> str:
    """Detect the package name from source, or return a default."""
    import re
    match = re.search(
        r'(?:create\s+or\s+replace\s+)?Package\s+Body\s+(\w+)',
        source, re.IGNORECASE
    )
    if match:
        return match.group(1)
    return "converted_procedures"


def _detect_status_table(source: str) -> str | None:
    """Auto-detect the status tracking table from source.

    Scans for INSERT INTO ... (Name, ..., Start_Date) patterns that look
    like status tracking tables. Returns the table name or None.
    """
    import re
    # Look for tables that receive (Name/Job_Name, ..., Start_Date/Start_Time)
    # This is the generic status-tracking pattern seen in Oracle ETL packages.
    candidates = re.findall(
        r"Insert\s+Into\s+([\w\.]+)\s*\(\s*(?:Name|Job_Name)\s*,\s*\w+\s*,\s*(?:Start_Date|Start_Time)\s*\)",
        source, re.IGNORECASE
    )
    if candidates:
        # Return the most common candidate
        from collections import Counter
        return Counter(candidates).most_common(1)[0][0]
    return None


def _detect_orchestrator_proc(procedures, proc_statements) -> str | None:
    """Detect which procedure is an orchestrator (calls DBMS_SCHEDULER).

    Returns the name of the orchestrator procedure, or None.
    """
    for proc in procedures:
        data = proc_statements.get(proc.name, {})
        statements = data.get('statements', [])
        scheduler_count = sum(
            1 for s in statements
            if s.type == StatementType.DBMS_SCHEDULER
        )
        if scheduler_count >= 3:  # Multiple scheduler jobs = orchestrator
            return proc.name
    return None


def _to_module_name(name: str) -> str:
    """Convert a PascalCase or mixed-case name to a snake_case module name.

    Handles names already in snake_case (Pkg_Lac_Mis_Archive → pkg_lac_mis_archive)
    and PascalCase (PkgCustomer → pkg_customer). Preserves existing underscores.
    """
    # If the name already has underscores, just lowercase it
    if '_' in name:
        return name.lower()
    # PascalCase → snake_case
    import re
    s = re.sub(r'([A-Z])', lambda m: '_' + m.group(1).lower(), name)
    s = s.strip('_').lower()
    s = re.sub(r'_+', '_', s)
    return s


def run_pipeline(source_path: str, output_dir: str, verbose: bool = False,
                 path: str = "auto", use_llm: bool = False,
                 llm_provider: str = "deepseek", llm_dry_run: bool = False) -> dict:
    """Run the full conversion pipeline.

    Args:
        source_path: Path to PL/SQL source file
        output_dir: Directory for output files
        verbose: Print detailed progress
        path: Conversion path — "regex", "antlr", or "auto"

    Returns:
        Pipeline results including confidence reports
    """
    os.makedirs(output_dir, exist_ok=True)

    # Parse path override
    force_path = None
    if path == "regex":
        force_path = ConversionPath.REGEX
    elif path == "antlr":
        force_path = ConversionPath.ANTLR
    # "auto" leaves force_path as None → router decides

    # Initialize LLM bridge if requested
    llm_bridge = None
    if use_llm:
        from .llm_bridge import LLMBridge
        llm_bridge = LLMBridge(
            provider=llm_provider,
            cache_dir=os.path.join(output_dir, ".cache", "llm_conversions"),
            dry_run=llm_dry_run,
        )
        set_llm_bridge(llm_bridge)
        if verbose:
            mode = "dry-run" if llm_dry_run else "active"
            print(f"  LLM bridge initialized ({llm_provider}, {mode})")

    # Read source
    with open(source_path, 'r') as f:
        source = f.read()

    # Detect package name and status table from input
    package_name = _detect_package_name(source)
    module_name = _to_module_name(package_name)
    status_table = _detect_status_table(source)
    if status_table:
        set_status_table_pattern(status_table)
    if verbose:
        print(f"  Package: {package_name}, module: {module_name}")
        if status_table:
            print(f"  Status table detected: {status_table}")

    print(f"[1/7] Splitting PL/SQL package...")
    procedures = split_package(source)
    proc_count = sum(1 for p in procedures if p.unit_type == 'procedure')
    func_count = sum(1 for p in procedures if p.unit_type == 'function')
    print(f"  Found {proc_count} procedures, {func_count} functions")

    # Load supplementary source files from the same directory
    # These contain standalone procedure implementations that can replace
    # package body stubs which delegate via REMOTE_PROC_CALL
    source_dir = os.path.dirname(source_path)
    source_basename = os.path.basename(source_path)
    for supplement_file in sorted(os.listdir(source_dir)):
        if supplement_file == source_basename:
            continue
        if not supplement_file.endswith('.txt'):
            continue
        supplement_path = os.path.join(source_dir, supplement_file)
        try:
            with open(supplement_path, 'r') as f:
                supplement_source = f.read()
            supplement_procs = split_package(supplement_source)
            if supplement_procs:
                procedures.extend(supplement_procs)
                if verbose:
                    names = [p.name for p in supplement_procs]
                    print(f"  Loaded {len(supplement_procs)} supplementary procedure(s) from {supplement_file}: {', '.join(names)}")
        except Exception:
            pass  # Skip unreadable files

    # Resolve duplicate procedure names — prefer standalone implementations
    # over package body stubs that delegate via REMOTE_PROC_CALL
    seen_names = {}
    remove_indices = set()
    for i, proc in enumerate(procedures):
        if proc.name in seen_names:
            prev_i = seen_names[proc.name]
            prev_body = extract_procedure_body(procedures[prev_i])
            curr_body = extract_procedure_body(proc)
            prev_stmts = extract_statements(prev_body)
            curr_stmts = extract_statements(curr_body)
            prev_has_remote = any(s.type == StatementType.REMOTE_PROC_CALL for s in prev_stmts)
            curr_has_remote = any(s.type == StatementType.REMOTE_PROC_CALL for s in curr_stmts)
            if prev_has_remote and not curr_has_remote:
                remove_indices.add(prev_i)
                seen_names[proc.name] = i
                if verbose:
                    print(f"  Resolved duplicate: keeping standalone {proc.name} (replaces remote-call stub)")
            elif curr_has_remote and not prev_has_remote:
                remove_indices.add(i)
                if verbose:
                    print(f"  Resolved duplicate: keeping standalone {proc.name} (skipping remote-call stub)")
            # If both or neither have remote calls, keep the first (package body version)
            elif not prev_has_remote and not curr_has_remote:
                remove_indices.add(i)
        else:
            seen_names[proc.name] = i
    if remove_indices:
        procedures = [p for i, p in enumerate(procedures) if i not in remove_indices]

    # Analyze complexity and route each procedure
    print(f"[2/7] Analyzing complexity and routing...")
    proc_paths = {}
    for proc in procedures:
        sig = analyze_complexity(proc.raw_text)
        selected = select_path(sig, force_path)
        proc_paths[proc.name] = selected
        if verbose:
            print(f"  {proc.name}: {selected.value} "
                  f"(depth={sig.nested_begin_depth}, for={sig.has_for_loop}, "
                  f"goto={sig.has_goto}, exceptions={sig.exception_block_count})")

    regex_count = sum(1 for v in proc_paths.values() if v == ConversionPath.REGEX)
    antlr_count = sum(1 for v in proc_paths.values() if v == ConversionPath.ANTLR)
    print(f"  Routes: {regex_count} regex, {antlr_count} antlr")

    # Extract and classify statements for each procedure
    print(f"[3/7] Extracting and classifying SQL statements...")
    proc_statements = {}
    for proc in procedures:
        body = extract_procedure_body(proc)
        statements = extract_statements(body)
        proc_statements[proc.name] = {
            'procedure': proc,
            'statements': statements,
            'summary': get_statement_summary(statements),
            'path': proc_paths[proc.name],
        }
        if verbose:
            summary = proc_statements[proc.name]['summary']
            print(f"  {proc.name}: {summary['total_statements']} statements")

    # Score each procedure
    print(f"[4/7] Scoring procedure conversion confidence...")
    confidence_reports = []
    for name, data in proc_statements.items():
        report = score_procedure(name, data['statements'])
        confidence_reports.append(report)
        data['confidence'] = report

    # Generate confidence report
    report_text = generate_summary_report(confidence_reports)
    report_path = os.path.join(output_dir, 'confidence_report.txt')
    with open(report_path, 'w') as f:
        f.write(report_text)
    print(f"  Confidence report saved to {report_path}")

    # Detect orchestrator procedure (if any) — the one with multiple DBMS_SCHEDULER calls
    orchestrator_proc_name = _detect_orchestrator_proc(procedures, proc_statements)
    if orchestrator_proc_name and verbose:
        print(f"  Orchestrator detected: {orchestrator_proc_name}")

    # Generate converted PySpark code
    print(f"[5/7] Generating PySpark code...")
    all_procedure_code = []

    for proc in procedures:
        data = proc_statements[proc.name]
        statements = data['statements']
        selected_path = data['path']

        # Determine status name
        status_name = _extract_status_name(statements) or proc.name.upper()

        # Skip the detected orchestrator — generate it separately via call-graph inference
        if proc.name == orchestrator_proc_name:
            continue

        if selected_path == ConversionPath.ANTLR:
            try:
                from .antlr_bridge import parse_procedure_to_ir, ParseError
                proc_ir = parse_procedure_to_ir(proc, status_name)
                code = generate_procedure_from_ir(proc_ir)
                if verbose:
                    print(f"  Generated (IR, antlr): {proc.name}")
            except Exception as e:
                # Fallback to regex path if ANTLR fails
                proc_ir = statements_to_ir(proc, statements, status_name)
                proc_ir.conversion_path = "antlr-fallback"
                code = generate_procedure_from_ir(proc_ir)
                if verbose:
                    print(f"  Generated (IR, antlr-fallback): {proc.name} — {e}")
        else:
            # Regex path: use IR pipeline for consistency
            proc_ir = statements_to_ir(proc, statements, status_name)
            code = generate_procedure_from_ir(proc_ir)
            if verbose:
                print(f"  Generated (IR, regex): {proc.name}")

        all_procedure_code.append(code)

    # Point C: Post-generation LLM sweep for remaining TODOs
    # Build procedure name list matching all_procedure_code indices
    proc_names_for_code = [p.name for p in procedures if p.name != orchestrator_proc_name]
    llm_assist_counts = {}

    if use_llm and llm_bridge:
        todo_before = sum(code.count("# TODO") for code in all_procedure_code)
        if todo_before > 0 and verbose:
            print(f"  Post-gen sweep: {todo_before} TODOs remaining, attempting LLM fixes...")
        for i, code in enumerate(all_procedure_code):
            if "# TODO" in code:
                before_count = code.count("# TODO")
                all_procedure_code[i] = llm_bridge.fix_remaining_todos(code)
                after_count = all_procedure_code[i].count("# TODO")
                resolved = before_count - after_count
                if resolved > 0 and i < len(proc_names_for_code):
                    llm_assist_counts[proc_names_for_code[i]] = resolved
        todo_after = sum(code.count("# TODO") for code in all_procedure_code)
        if verbose and todo_before > 0:
            print(f"  Post-gen sweep resolved {todo_before - todo_after}/{todo_before} TODOs")
        if llm_dry_run and todo_before > 0:
            print(f"  Dry run: {todo_before} TODOs would be sent to LLM for resolution")

    # Rescore confidence reports with LLM assist data
    if use_llm and llm_bridge and llm_bridge.stats.get("todos_resolved", 0) > 0:
        for i, report in enumerate(confidence_reports):
            assist_count = llm_assist_counts.get(report.procedure_name, 0)
            if assist_count > 0:
                confidence_reports[i] = rescore_with_llm_data(report, assist_count)
                proc_statements[report.procedure_name]['confidence'] = confidence_reports[i]
        # Regenerate confidence report file
        report_text = generate_summary_report(confidence_reports)
        with open(report_path, 'w') as f:
            f.write(report_text)

    # Generate orchestrator from call graph (generic)
    orchestrator_code = generate_orchestrator(
        procedures, proc_statements, orchestrator_proc_name
    )

    # Assemble final output
    print(f"[6/7] Assembling output files...")

    # Write status tracker module (only if status tracking was detected)
    if status_table:
        tracker_path = os.path.join(output_dir, 'status_tracker.py')
        with open(tracker_path, 'w') as f:
            f.write(generate_status_tracker(status_table))

    # Write all procedures to a single module (name derived from input)
    procedures_filename = f'{module_name}.py'
    procedures_path = os.path.join(output_dir, procedures_filename)
    with open(procedures_path, 'w') as f:
        f.write(_generate_file_header(package_name, source_path))
        f.write('\n\n')
        if status_table:
            f.write('from status_tracker import track_status\n\n')
        for code in all_procedure_code:
            f.write(code)
            f.write('\n\n')

    # Write orchestrator
    orchestrator_path = os.path.join(output_dir, 'orchestrator.py')
    with open(orchestrator_path, 'w') as f:
        f.write(_generate_file_header(package_name, source_path))
        f.write('\n')
        f.write(f'from {module_name} import *\n\n')
        f.write(orchestrator_code)

    # Write analysis JSON (with path info)
    print(f"[7/7] Writing analysis metadata...")
    analysis = _generate_analysis(procedures, proc_statements, confidence_reports,
                                   package_name=package_name, source_path=source_path)

    # Add LLM stats if LLM was used
    if use_llm and llm_bridge:
        analysis["llm"] = llm_bridge.stats

    analysis_path = os.path.join(output_dir, 'analysis.json')
    with open(analysis_path, 'w') as f:
        json.dump(analysis, f, indent=2, default=str)

    # Clean up: reset LLM bridge and status table pattern
    set_llm_bridge(None)
    set_status_table_pattern(None)

    # Print summary
    _print_summary(confidence_reports)
    if use_llm and llm_bridge:
        stats = llm_bridge.stats
        print(f"LLM Stats: {stats['calls_made']} calls, {stats['cache_hits']} cache hits, "
              f"{stats['todos_resolved']} TODOs resolved, {stats['validation_failures']} validation failures")

    return {
        'procedures_count': len(procedures),
        'output_dir': output_dir,
        'confidence_reports': confidence_reports,
        'proc_paths': {k: v.value for k, v in proc_paths.items()},
        'files': [tracker_path, procedures_path, orchestrator_path, report_path, analysis_path],
        'llm_stats': llm_bridge.stats if (use_llm and llm_bridge) else None,
    }


def _extract_status_name(statements: list) -> str | None:
    """Extract the status tracking name from STATUS_INSERT statements."""
    for stmt in statements:
        if stmt.type == StatementType.STATUS_INSERT:
            import re
            match = re.search(r"'(\w+)'", stmt.sql)
            if match:
                return match.group(1)
    return None


def _generate_file_header(package_name: str = "Unknown", source_path: str = "") -> str:
    """Generate Python file header with imports and metadata (derived from input)."""
    source_display = os.path.basename(source_path) if source_path else "Unknown"
    return f'''"""
Auto-generated PySpark code converted from Oracle PL/SQL
Source: {package_name} ({source_display})
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

IMPORTANT: This code was auto-converted and requires:
1. Review of all TODO comments
2. Verification of database link table availability
3. Validation with test data before production use
4. Review of correlated UPDATE conversions (MERGE statements)
"""

import time
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)
'''


def _generate_analysis(procedures, proc_statements, confidence_reports,
                       package_name: str = "", source_path: str = "") -> dict:
    """Generate comprehensive analysis JSON."""
    analysis = {
        'generated_at': datetime.now().isoformat(),
        'source_file': os.path.basename(source_path) if source_path else 'unknown',
        'package_name': package_name,
        'total_procedures': len(procedures),
        'confidence_summary': {
            'average_score': sum(r.score for r in confidence_reports) / len(confidence_reports) if confidence_reports else 0,
            'auto_pass': sum(1 for r in confidence_reports if r.tier == 'auto_pass'),
            'review_recommended': sum(1 for r in confidence_reports if r.tier == 'review_recommended'),
            'human_required': sum(1 for r in confidence_reports if r.tier == 'human_required'),
            'blocked': sum(1 for r in confidence_reports if r.tier == 'blocked'),
        },
        'routing': {},
        'procedures': [],
        'all_tables_read': [],
        'all_tables_written': [],
        'db_link_dependencies': [],
    }

    all_tables_read = set()
    all_tables_written = set()

    for proc in procedures:
        data = proc_statements[proc.name]
        summary = data['summary']
        confidence = data['confidence']

        proc_info = {
            'name': proc.name,
            'lines': f"{proc.start_line}-{proc.end_line}",
            'parameters': proc.parameters,
            'conversion_path': data['path'].value,
            'statement_count': summary['total_statements'],
            'type_counts': summary['type_counts'],
            'construct_counts': summary['construct_counts'],
            'tables_written': summary['tables_written'],
            'tables_read': summary['tables_read'],
            'db_link_count': summary['db_link_statements'],
            'confidence_score': confidence.score,
            'confidence_tier': confidence.tier,
            'risk_factors': confidence.risk_factors,
        }
        analysis['procedures'].append(proc_info)
        analysis['routing'][proc.name] = data['path'].value

        all_tables_read.update(summary['tables_read'])
        all_tables_written.update(summary['tables_written'])

    analysis['all_tables_read'] = sorted(all_tables_read)
    analysis['all_tables_written'] = sorted(all_tables_written)

    return analysis


def _print_summary(reports: list[ConfidenceReport]):
    """Print a concise summary to stdout."""
    total = len(reports)
    avg = sum(r.score for r in reports) / total if total else 0
    auto_pass = sum(1 for r in reports if r.tier == 'auto_pass')
    review = sum(1 for r in reports if r.tier == 'review_recommended')
    human = sum(1 for r in reports if r.tier == 'human_required')
    blocked = sum(1 for r in reports if r.tier == 'blocked')

    print(f"\n{'='*60}")
    print(f"CONVERSION SUMMARY")
    print(f"{'='*60}")
    print(f"Procedures converted: {total}")
    print(f"Average confidence:   {avg:.1f}%")
    print(f"  Auto-pass:          {auto_pass}")
    print(f"  Review needed:      {review}")
    print(f"  Human required:     {human}")
    print(f"  Blocked:            {blocked}")
    print(f"{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(
        description='Convert Oracle PL/SQL to PySpark'
    )
    parser.add_argument('source', help='Path to PL/SQL source file')
    parser.add_argument('--output', '-o', default='output',
                        help='Output directory (default: output)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Verbose output')
    parser.add_argument('--path', '-p', choices=['regex', 'antlr', 'auto'],
                        default='auto',
                        help='Conversion path: regex (existing), antlr (new), auto (router decides)')
    parser.add_argument('--llm', action='store_true',
                        help='Enable LLM-assisted conversion for remaining TODOs')
    parser.add_argument('--llm-provider', choices=['deepseek', 'claude'],
                        default='deepseek',
                        help='LLM provider (default: deepseek)')
    parser.add_argument('--llm-dry-run', action='store_true',
                        help='Show what LLM would do without making API calls')

    args = parser.parse_args()

    if not os.path.exists(args.source):
        print(f"Error: Source file not found: {args.source}")
        sys.exit(1)

    results = run_pipeline(
        args.source, args.output, args.verbose, args.path,
        use_llm=args.llm, llm_provider=args.llm_provider,
        llm_dry_run=args.llm_dry_run,
    )

    print(f"Output files:")
    for f in results['files']:
        print(f"  {f}")

    # Print routing summary
    if results.get('proc_paths'):
        print(f"\nRouting:")
        for name, path_val in results['proc_paths'].items():
            print(f"  {name}: {path_val}")


if __name__ == '__main__':
    main()
