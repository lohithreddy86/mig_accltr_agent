"""
Deterministic conversion subsystem for Oracle PL/SQL → PySpark.

This package wraps the rule-based converter pipeline (APP/converter/) as a
deterministic preprocessing and first-pass conversion stage for the agentic
migration system.

SCOPE GUARD: This subsystem is activated ONLY for PySpark conversion strategies
(PYSPARK_DF, PYSPARK_PIPELINE). The Trino SQL conversion path (TRINO_SQL strategy,
query mode, TrinoSQLValidator) is NEVER touched by any code in this package.

Design:
  - interchange.py: Data contract between converter output and agent input
  - preprocessor.py: Deterministic Oracle SQL preprocessing (transpilation rules)
  - prompt_builder.py: Structured prompt construction from converter metadata
  - scoring.py: Unified confidence scoring (converter + agent credits)
"""
