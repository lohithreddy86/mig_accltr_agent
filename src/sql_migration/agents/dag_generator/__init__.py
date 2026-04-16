"""
sql_migration.agents.dag_generator
===================================
Stage 6 — Airflow DAG generator (placeholder).
Assembles converted PySpark files + scheduling graph into
an Airflow DAG and migration report.
"""
from sql_migration.agents.dag_generator.agent import DagGeneratorAgent, DagGeneratorOutput

__all__ = ["DagGeneratorAgent", "DagGeneratorOutput"]