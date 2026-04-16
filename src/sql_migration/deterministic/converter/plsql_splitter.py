"""
PL/SQL Procedure Splitter

Splits a PL/SQL package body into individual procedures using regex-based
boundary detection. Designed for well-formatted Oracle PL/SQL packages.
"""

import re
from dataclasses import dataclass, field


@dataclass
class ProcedureInfo:
    name: str
    parameters: list[str]
    body: str
    start_line: int
    end_line: int
    raw_text: str
    unit_type: str = "procedure"  # "procedure" or "function"
    return_type: str = ""         # Non-empty for functions (e.g., "Number", "Varchar2")


def split_package(source: str) -> list[ProcedureInfo]:
    """Split a PL/SQL package body into individual procedures.

    Args:
        source: Full PL/SQL package body text

    Returns:
        List of ProcedureInfo objects, one per procedure
    """
    lines = source.split('\n')
    procedures = []

    # Pattern to match procedure AND function declarations
    # Handles: Procedure/Function name Is/As, with optional params and return type
    # Also handles: CREATE OR REPLACE Procedure/Function name Is/As (standalone units)
    proc_pattern = re.compile(
        r'^\s*(?:create\s+or\s+replace\s+)?(?:Procedure|Function)\s+(\w+)\s*(?:\((.*?)\))?\s*(?:Return\s+\w+(?:\([^)]*\))?\s+)?(?:Is|As)\s*$',
        re.IGNORECASE
    )

    # Also match with parameters on same line
    proc_pattern_with_params = re.compile(
        r'^\s*(?:create\s+or\s+replace\s+)?(?:Procedure|Function)\s+(\w+)\s*\(\s*(.*?)\s*\)\s*(?:Return\s+\w+(?:\([^)]*\))?\s+)?(?:Is|As)\s*$',
        re.IGNORECASE
    )

    # Detect whether a declaration line is a function (has RETURN clause)
    func_detect_pattern = re.compile(
        r'^\s*(?:create\s+or\s+replace\s+)?Function\b',
        re.IGNORECASE
    )
    return_type_pattern = re.compile(
        r'Return\s+(\w+(?:\([^)]*\))?)\s+(?:Is|As)',
        re.IGNORECASE
    )

    # Find all procedure/function boundaries
    proc_starts = []
    for i, line in enumerate(lines):
        match = proc_pattern.match(line) or proc_pattern_with_params.match(line)
        if match:
            name = match.group(1)
            params_str = match.group(2) or ''
            params = []
            if params_str:
                for p in params_str.split(','):
                    p = p.strip()
                    if p:
                        # Extract just the parameter name: "i_Yyyymm In Number" -> "i_Yyyymm"
                        param_name = re.split(r'\s+', p)[0]
                        params.append(param_name)
            # Detect whether this is a function or procedure
            is_function = bool(func_detect_pattern.match(line))
            unit_type = "function" if is_function else "procedure"
            return_type = ""
            if is_function:
                rt_match = return_type_pattern.search(line)
                if rt_match:
                    return_type = rt_match.group(1)
            proc_starts.append((i, name, params, unit_type, return_type))

    # Extract each procedure/function body (from start to End name;)
    for idx, (start_line, name, params, unit_type, return_type) in enumerate(proc_starts):
        # Find the End marker for this procedure
        end_pattern = re.compile(
            rf'^\s*End\s+{re.escape(name)}\s*;\s*$',
            re.IGNORECASE
        )

        end_line = None
        for i in range(start_line + 1, len(lines)):
            if end_pattern.match(lines[i]):
                end_line = i
                break

        if end_line is None:
            # Fallback: use the start of the next procedure or end of file
            if idx + 1 < len(proc_starts):
                end_line = proc_starts[idx + 1][0] - 1
            else:
                end_line = len(lines) - 1

        raw_text = '\n'.join(lines[start_line:end_line + 1])

        # Extract the body between Begin and Exception/End
        body_lines = lines[start_line:end_line + 1]
        body = '\n'.join(body_lines)

        procedures.append(ProcedureInfo(
            name=name,
            parameters=params,
            body=body,
            start_line=start_line + 1,  # 1-based
            end_line=end_line + 1,       # 1-based
            raw_text=raw_text,
            unit_type=unit_type,
            return_type=return_type,
        ))

    return procedures


def extract_procedure_body(proc: ProcedureInfo) -> str:
    """Extract just the executable body (between Begin and Exception/End).

    Strips variable declarations, Begin keyword, Exception block, and End.
    Returns only the DML/procedural statements.
    """
    lines = proc.body.split('\n')

    # Find first Begin
    begin_idx = None
    for i, line in enumerate(lines):
        if re.match(r'^\s*Begin\s*$', line, re.IGNORECASE):
            begin_idx = i
            break

    if begin_idx is None:
        return proc.body

    # Find Exception or End at the procedure level
    # Track nesting depth for inner BEGIN...END blocks
    exception_idx = None
    end_idx = len(lines) - 1
    depth = 0

    for i in range(begin_idx + 1, len(lines)):
        line_stripped = lines[i].strip().upper()

        # Track nested BEGIN blocks
        if re.match(r'^BEGIN\b', line_stripped):
            depth += 1
        elif re.match(r'^END\b', line_stripped) and depth > 0:
            depth -= 1
        elif depth == 0 and re.match(r'^EXCEPTION\b', line_stripped):
            exception_idx = i
            break
        elif depth == 0 and re.match(r'^END\s+\w+', line_stripped):
            end_idx = i
            break

    if exception_idx:
        body_text = '\n'.join(lines[begin_idx + 1:exception_idx])
    else:
        body_text = '\n'.join(lines[begin_idx + 1:end_idx])

    return body_text.strip()


def extract_variable_declarations(proc: ProcedureInfo) -> list[tuple[str, str]]:
    """Extract variable declarations from procedure header.

    Returns list of (variable_name, variable_type) tuples.
    """
    lines = proc.body.split('\n')
    declarations = []

    # Find declarations between procedure header and first Begin
    in_decl = False
    var_pattern = re.compile(
        r'^\s+(\w+)\s+([\w\(\),\s]+?)(?:\s*:=\s*(.+?))?\s*;\s*$',
        re.IGNORECASE
    )

    for i, line in enumerate(lines):
        if i == 0:  # Skip procedure declaration line
            in_decl = True
            continue
        if re.match(r'^\s*Begin\s*$', line, re.IGNORECASE):
            break
        if in_decl:
            match = var_pattern.match(line)
            if match:
                var_name = match.group(1)
                var_type = match.group(2).strip()
                declarations.append((var_name, var_type))

    return declarations


def extract_exception_block(proc: ProcedureInfo) -> str | None:
    """Extract the exception handling block from a procedure."""
    lines = proc.body.split('\n')

    exception_idx = None
    depth = 0

    # Find first Begin
    begin_found = False
    for i, line in enumerate(lines):
        line_stripped = line.strip().upper()
        if not begin_found:
            if re.match(r'^BEGIN\b', line_stripped):
                begin_found = True
            continue

        if re.match(r'^BEGIN\b', line_stripped):
            depth += 1
        elif re.match(r'^END\b', line_stripped) and depth > 0:
            depth -= 1
        elif depth == 0 and re.match(r'^EXCEPTION\b', line_stripped):
            exception_idx = i
            break

    if exception_idx is None:
        return None

    # Find the final End
    end_idx = len(lines) - 1
    return '\n'.join(lines[exception_idx:end_idx + 1])


if __name__ == '__main__':
    import sys

    if len(sys.argv) < 2:
        print("Usage: python plsql_splitter.py <script.txt>")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        source = f.read()

    procedures = split_package(source)

    print(f"Found {len(procedures)} procedures:\n")
    for proc in procedures:
        param_str = f"({', '.join(proc.parameters)})" if proc.parameters else ""
        body_lines = len(proc.body.split('\n'))
        print(f"  {proc.name}{param_str}")
        print(f"    Lines: {proc.start_line}-{proc.end_line} ({body_lines} lines)")
