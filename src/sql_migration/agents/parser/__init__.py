"""
sql_migration.agents.parser
============================
SSIS package parser: extracts SQL, scheduling graph, and metadata
from .dtsx / .ispac files for downstream pipeline processing.
"""
from sql_migration.agents.parser.agent import ParserAgent, ParserOutput

__all__ = ["ParserAgent", "ParserOutput"]