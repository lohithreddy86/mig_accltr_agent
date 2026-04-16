"""
Prompt templates for LLM-assisted PL/SQL → PySpark conversion.

Isolated for easy tuning. Each template has a version string for cache invalidation.
"""

CORRELATED_UPDATE_TO_MERGE_VERSION = "v1"
CORRELATED_UPDATE_TO_MERGE = """You are converting Oracle PL/SQL to Spark SQL. Convert the following correlated UPDATE into a MERGE statement.

The SQL has already been partially transpiled from Oracle to Spark syntax (NVL→COALESCE, SYSDATE→CURRENT_TIMESTAMP, etc.), but the correlated UPDATE pattern could not be automatically converted.

**Input SQL (partially transpiled):**
```sql
{preprocessed_sql}
```

**Original Oracle SQL:**
```sql
{oracle_sql}
```

**Target table:** {target_table}

**Rules:**
1. Output ONLY a valid Spark SQL MERGE statement — no explanation, no markdown fences
2. The MERGE must have: USING (subquery or table) ON (join condition) WHEN MATCHED THEN UPDATE SET ...
3. The target table in the MERGE must be `{target_table}`
4. Preserve all column assignments from the original SET clause
5. Preserve the WHERE EXISTS / join condition from the original
6. Do NOT use Oracle-specific functions (NVL, SYSDATE, DECODE, (+)) — use Spark equivalents
7. Do NOT add columns or tables not present in the original
"""

UNKNOWN_STATEMENT_VERSION = "v1"
UNKNOWN_STATEMENT = """You are converting Oracle PL/SQL to PySpark. Convert the following PL/SQL statement to equivalent Python/PySpark code.

**PL/SQL statement:**
```plsql
{raw_plsql}
```

**Procedure context:** {procedure_context}

**Rules:**
1. Output ONLY valid Python code — no explanation, no markdown fences
2. Use `spark.sql(\"\"\"...\"\"\")` for any SQL operations
3. Use standard PySpark patterns (DataFrame API or spark.sql)
4. Do NOT use Oracle-specific functions — use Spark equivalents
5. Do NOT use eval(), exec(), os.system(), or subprocess
6. If the statement is a BEGIN...END block, convert the inner SQL statements
7. If the statement cannot be meaningfully converted, output a Python comment explaining why
"""

FOR_LOOP_FALLBACK_VERSION = "v1"
FOR_LOOP_FALLBACK = """You are converting an Oracle PL/SQL FOR cursor loop to PySpark. The automatic conversion failed — produce the PySpark equivalent.

**PL/SQL FOR loop:**
```plsql
{raw_plsql}
```

**Rules:**
1. Output ONLY valid Python code — no explanation, no markdown fences
2. Pattern: `_cursor_df = spark.sql(\"\"\"SELECT ...\"\"\")` then `for row in _cursor_df.collect():`
3. Access cursor fields via `row['field_name']`
4. Convert any Oracle SQL inside the loop body to Spark SQL
5. Do NOT use Oracle-specific functions
"""

TODO_FIX_VERSION = "v1"
TODO_FIX = """You are fixing a TODO comment in auto-generated PySpark code that was converted from Oracle PL/SQL.

**Generated code with TODO (lines around the TODO):**
```python
{surrounding_code}
```

**The specific TODO line:**
```
{todo_line}
```

**Rules:**
1. Output ONLY the replacement Python code for the TODO section — no explanation, no markdown fences
2. The replacement must be valid Python that integrates with the surrounding code
3. Use `spark.sql(\"\"\"...\"\"\")` for SQL operations
4. Maintain the same indentation level as the TODO line
5. Do NOT use Oracle-specific functions (NVL, SYSDATE, DECODE)
6. Do NOT use eval(), exec(), os.system(), or subprocess
7. If you cannot produce a valid conversion, output the original TODO comment unchanged
"""
