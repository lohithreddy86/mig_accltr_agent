"""
mcp_client.py
=============
Client for the Trino MCP server.
Uses fastmcp.Client for SSE transport — same library as the chatbot project
where MCP connectivity is proven to work.

Provides:
  - Read-only safety enforcement (blocks DML keywords)
  - Typed methods for each tool (list_schemas, list_tables, desc_table, run_query)
  - Retry with backoff
  - Fresh connection per call (avoids stale session issues)
  - Robust result parsing (handles CallToolResult, TextContent, raw dicts)

Usage:
    mcp = MCPClientSync()
    tables = mcp.list_tables(schema="core")
    columns = mcp.desc_table(table="lakehouse.core.account_master")
    rows = mcp.run_query("SELECT COUNT(*) FROM lakehouse.core.account_master")
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from typing import Any, Optional

from sql_migration.core.config_loader import get_config
from sql_migration.core.logger import get_logger

log = get_logger("mcp_client")

# ---------------------------------------------------------------------------
# Try to import fastmcp — graceful degradation for dry_run mode
# ---------------------------------------------------------------------------
try:
    from fastmcp import Client
    _MCP_AVAILABLE = True
except ImportError:
    _MCP_AVAILABLE = False
    log.warning("fastmcp_not_installed",
                msg="Install fastmcp: pip install fastmcp. Using mock client.")


# ---------------------------------------------------------------------------
# Safety: read-only enforcement
# ---------------------------------------------------------------------------

def _check_readonly(query: str) -> None:
    """Raise ValueError if query contains any DML keywords."""
    cfg = get_config()
    upper = query.upper()
    for keyword in cfg.mcp.blocked_keywords:
        if re.search(rf"\b{keyword}\b", upper):
            raise ValueError(
                f"Query blocked: contains DML keyword '{keyword}'. "
                f"The Trino MCP client is read-only.\nQuery: {query[:200]}"
            )


# ---------------------------------------------------------------------------
# Result extraction (from Bobby's working chatbot mcp_client.py)
# ---------------------------------------------------------------------------

def _extract_result(raw_result: Any) -> dict:
    """
    Extract a dict from the MCP tool response.

    fastmcp Client.call_tool() can return:
      - A CallToolResult object with .content (list) and .isError (bool)
      - A list of content objects (TextContent, etc.)
      - A dict
      - None
    """
    if raw_result is None:
        return {"error": "Empty response from MCP server"}

    is_error = getattr(raw_result, "isError", False) or getattr(raw_result, "is_error", False)
    content = getattr(raw_result, "content", None)

    if content is not None:
        items = content
    elif isinstance(raw_result, list):
        items = raw_result
    elif isinstance(raw_result, dict):
        return raw_result
    elif isinstance(raw_result, str):
        try:
            return json.loads(raw_result)
        except (json.JSONDecodeError, TypeError):
            return {"error": raw_result} if is_error else {"raw": raw_result}
    else:
        text = str(raw_result)
        log.warning("mcp_unexpected_result_type", type=type(raw_result).__name__,
                    preview=text[:200])
        try:
            return json.loads(text)
        except (json.JSONDecodeError, TypeError):
            return {"error": text} if is_error else {"raw": text}

    if not items:
        return {"error": "Empty content in MCP response"}

    for item in items:
        text = getattr(item, "text", None)
        if not text:
            continue
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                if is_error and "error" not in parsed:
                    parsed["error"] = text
                return parsed
            return {"data": parsed, "error": text} if is_error else {"data": parsed}
        except (json.JSONDecodeError, TypeError):
            pass
        if is_error:
            return {"error": text, "validation_error": True}
        return {"raw": text}

    return {"error": "No text content in MCP response"}


# ---------------------------------------------------------------------------
# Core MCP caller (fresh connection per call, like the working chatbot)
# ---------------------------------------------------------------------------

class MCPCaller:
    """
    Low-level MCP tool caller using fastmcp.Client.
    Opens a fresh SSE connection per call — avoids stale session issues.
    """

    def __init__(self) -> None:
        cfg = get_config()
        self.cfg = cfg.mcp
        base = self.cfg.server_url.rstrip("/")
        # Allow the configured URL to specify either the SSE legacy endpoint
        # (`/sse`) or the modern Streamable-HTTP endpoint (`/mcp`). Default to
        # `/sse` for backward compatibility when neither suffix is present.
        if base.endswith("/mcp") or base.endswith("/sse"):
            self.sse_url = base
        elif base:
            self.sse_url = f"{base}/sse"
        else:
            self.sse_url = base
        self._use_streamable_http = self.sse_url.endswith("/mcp")
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None or self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
        return self._loop

    def call_tool(self, tool_name: str, arguments: dict[str, Any]) -> dict:
        """Call an MCP tool synchronously. Returns parsed dict result."""
        if not _MCP_AVAILABLE:
            log.warning("mcp_mock_mode",
                        msg="fastmcp not installed. Returning mock data.")
            return _mock_result(tool_name, arguments)

        if not self.sse_url or self.sse_url == "/sse":
            log.error("mcp_server_url_empty",
                      msg="MCP server_url is empty. Set TRINO_MCP_SERVER_URL env var "
                          "or mcp.server_url in config.yaml.")
            return _mock_result(tool_name, arguments)

        # Pick transport based on URL suffix. fastmcp 3.x's auto-detection on
        # the bare URL is unreliable against mcp-trino's `/sse` endpoint
        # (silently hangs); explicitly select StreamableHttp for `/mcp`.
        if self._use_streamable_http:
            from fastmcp.client.transports import StreamableHttpTransport
            transport = StreamableHttpTransport(self.sse_url)
        else:
            transport = self.sse_url

        async def _run():
            async with Client(transport) as client:
                result = await client.call_tool(tool_name, arguments)
                return result

        loop = self._get_loop()
        t0 = time.monotonic()
        try:
            raw = loop.run_until_complete(
                asyncio.wait_for(_run(), timeout=self.cfg.query_timeout)
            )
            elapsed = (time.monotonic() - t0) * 1000
            parsed = _extract_result(raw)
            log.debug("mcp_tool_success", tool=tool_name, elapsed_ms=round(elapsed))
            return parsed
        except asyncio.TimeoutError:
            elapsed = (time.monotonic() - t0) * 1000
            log.warning("mcp_timeout", tool=tool_name, timeout=self.cfg.query_timeout,
                        elapsed_ms=round(elapsed))
            return {"error": f"MCP tool '{tool_name}' timed out after {self.cfg.query_timeout}s"}
        except Exception as e:
            elapsed = (time.monotonic() - t0) * 1000
            log.error("mcp_call_failed", tool=tool_name, error=str(e),
                      elapsed_ms=round(elapsed))
            raise ConnectionError(f"MCP call '{tool_name}' failed: {e}") from e

    def call_tool_with_retry(self, tool_name: str, arguments: dict[str, Any]) -> dict:
        """Call with retry on transient errors."""
        last_err: Exception | None = None
        for attempt in range(1, self.cfg.max_retries + 1):
            try:
                return self.call_tool(tool_name, arguments)
            except ConnectionError as e:
                last_err = e
                log.warning("mcp_retry", tool=tool_name, attempt=attempt,
                            max=self.cfg.max_retries, error=str(e)[:200])
                if attempt < self.cfg.max_retries:
                    time.sleep(2 ** attempt)

        log.error("mcp_all_retries_failed", tool=tool_name,
                  attempts=self.cfg.max_retries)
        return _mock_result(tool_name, arguments)

    def close(self):
        if self._loop and not self._loop.is_closed():
            self._loop.close()
            self._loop = None


def _mock_result(tool_name: str, arguments: dict) -> dict:
    """Return mock results when MCP is unavailable."""
    log.warning("mcp_mock_result", tool=tool_name, arguments=arguments,
                msg="Returning MOCK data — MCP not connected.")
    mock_results = {
        "list_schemas":  {"schemas": [], "count": 0},
        "list_tables":   {"tables": [], "count": 0},
        "desc_table":    {"columns": [], "column_count": 0},
        "describe_table": {"columns": [], "column_count": 0},
        "run_query":     {"columns": [], "rows": [], "row_count": 0},
    }
    return mock_results.get(tool_name, {"error": f"Mock: unknown tool {tool_name}"})


# ---------------------------------------------------------------------------
# High-level sync client (used by all agents)
# ---------------------------------------------------------------------------

class MCPClientSync:
    """
    Synchronous MCP client with typed methods for each Trino tool.
    This is the primary interface used by all agents.

    Caches list_tables and desc_table results for the lifetime of the
    instance (one pipeline run). Avoids redundant MCP calls when many
    procs/queries reference tables in the same schema.
    """

    def __init__(self) -> None:
        self._caller = MCPCaller()
        self._cfg = get_config().mcp

        # Per-run caches — key is the argument that uniquely identifies the call
        self._list_tables_cache: dict[str, list[str]] = {}   # schema → [table_names]
        self._desc_table_cache: dict[str, list[dict]] = {}   # fqn → [{name, type, ...}]

        log.info("mcp_client_init",
                 server=self._cfg.server_url,
                 sse_url=self._caller.sse_url)

    @property
    def _default_catalog(self) -> str:
        # Catalog used by list_schemas / list_tables / get_table_schema when
        # the caller didn't qualify the name. Pulled from the input_catalog
        # parsed out of the README (fallback: 'lakehouse').
        cat = getattr(self._cfg, "default_catalog", None)
        if cat:
            return cat
        try:
            from sql_migration.core.artifact_store import get_artifact_store
            store = get_artifact_store()
            prof = store.read("analysis", "dialect_profile.json") or {}
            readme = prof.get("data", prof)
            return readme.get("input_catalog") or "lakehouse"
        except Exception:
            return "lakehouse"

    def list_schemas(self, catalog: str | None = None) -> list[str]:
        catalog = catalog or self._default_catalog
        result = self._caller.call_tool_with_retry(
            self._cfg.tools.list_schemas, {"catalog": catalog})
        if "schemas" in result:
            return result["schemas"]
        if "data" in result and isinstance(result["data"], list):
            return result["data"]
        return []

    def list_tables(self, schema: str) -> list[str]:
        # Cache hit — return immediately
        cache_key = schema.lower()
        if cache_key in self._list_tables_cache:
            log.debug("mcp_cache_hit", tool="list_tables", schema=schema)
            return self._list_tables_cache[cache_key]

        # mcp-trino list_tables REQUIRES `catalog` + `schema`. If the caller
        # passed a dotted `catalog.schema`, split it; otherwise default.
        parts = schema.split(".")
        if len(parts) == 2:
            args = {"catalog": parts[0], "schema": parts[1]}
        else:
            args = {"catalog": self._default_catalog, "schema": schema}

        result = self._caller.call_tool_with_retry(
            self._cfg.tools.list_tables, args)
        if "tables" in result:
            tables = result["tables"]
        elif "data" in result and isinstance(result["data"], list):
            tables = result["data"]
        else:
            tables = []

        # Cache ALL results (incl. empty / errored). Otherwise A3 hammers
        # MCP with the same missing-schema lookup hundreds of times, each
        # paying the full timeout. Negative cache is acceptable for the
        # lifetime of one pipeline run.
        self._list_tables_cache[cache_key] = tables
        log.debug("mcp_cache_store", tool="list_tables", schema=schema,
                  count=len(tables))

        return tables

    def desc_table(self, table: str) -> list[dict]:
        """
        Describe a table's columns.
        Accepts: "schema.table" or "catalog.schema.table".
        Returns [{name, type, nullable, comment}].
        """
        # Cache hit
        cache_key = table.lower()
        if cache_key in self._desc_table_cache:
            log.debug("mcp_cache_hit", tool="desc_table", table=table)
            return self._desc_table_cache[cache_key]

        # mcp-trino get_table_schema REQUIRES catalog + schema + table.
        parts = table.split(".")
        if len(parts) >= 3:
            args = {"catalog": parts[0], "schema": parts[1], "table": parts[2]}
        elif len(parts) == 2:
            args = {"catalog": self._default_catalog, "schema": parts[0], "table": parts[1]}
        else:
            args = {"catalog": self._default_catalog, "schema": "default", "table": table}

        result = self._caller.call_tool_with_retry(
            self._cfg.tools.describe_table, args)

        # mcp-trino wraps results in {"data": [...]}. Fall back to "columns"
        # for other MCP servers that use the older shape.
        columns = result.get("columns")
        if columns is None and isinstance(result.get("data"), list):
            columns = result["data"]
        columns = columns or []
        normalized = []
        for col in columns:
            if isinstance(col, dict):
                normalized.append({
                    "name":     col.get("name", col.get("Column", col.get("column_name", ""))),
                    "type":     col.get("type", col.get("Type", col.get("data_type", ""))),
                    "nullable": col.get("nullable", col.get("Null", col.get("is_nullable", ""))),
                    "comment":  col.get("comment", col.get("Comment", "")),
                })

        # Cache non-empty results
        if normalized:
            self._desc_table_cache[cache_key] = normalized
            log.debug("mcp_cache_store", tool="desc_table", table=table,
                      columns=len(normalized))

        return normalized

    def desc_target_table(self, target_fqn: str) -> list[dict]:
        """Same as desc_table but semantically names the call as pointing at
        the write-side target catalog (e.g. `lz_lakehouse.lm_target_schema.X`).

        Kept as a separate method so call sites in A3/P3/C2 are unambiguous
        about whether they are probing the SOURCE catalog (reads) or the
        TARGET catalog (writes).
        """
        return self.desc_table(target_fqn)

    def run_query(self, query: str) -> list[dict]:
        """
        Execute a read-only SELECT query.
        Raises ValueError if the query contains DML keywords.
        Returns list of row dicts.
        """
        _check_readonly(query)
        result = self._caller.call_tool_with_retry(
            self._cfg.tools.run_query, {"query": query})

        if result.get("error"):
            log.warning("mcp_query_error", error=result["error"][:200])
            return []

        # mcp-trino returns {"data": [{col: val, ...}, ...]}. Older MCP
        # servers use {"rows": [...], "columns": [...]} — keep both.
        rows = result.get("rows")
        columns = result.get("columns", [])
        if rows is None and isinstance(result.get("data"), list):
            rows = result["data"]
        rows = rows or []

        if rows and columns and isinstance(rows[0], list):
            rows = [dict(zip(columns, row)) for row in rows]

        return rows

    def run_query_raw(self, query: str) -> dict:
        """
        Execute a read-only query and return the FULL result including errors.
        Used by the deterministic conversion pipeline so the LLM sees error messages
        and can self-correct (unlike run_query which swallows errors).

        Returns dict with keys: rows, columns, row_count, error (if any).
        """
        _check_readonly(query)
        result = self._caller.call_tool_with_retry(
            self._cfg.tools.run_query, {"query": query})
        return result

    def schema_probe(self, table: str) -> list[dict]:
        query = self._cfg.schema_probe_query.format(table=table)
        return self.run_query(query)

    def table_row_count(self, table: str) -> int:
        rows = self.run_query(f"SELECT COUNT(*) as cnt FROM {table}")
        if rows and isinstance(rows[0], dict):
            return int(rows[0].get("cnt", rows[0].get("count(1)",
                       rows[0].get("_col0", 0))))
        return 0

    def sample_rows(self, table: str, limit: int | None = None) -> list[dict]:
        n = limit or self._cfg.sample_row_limit
        return self.run_query(
            f"SELECT * FROM {table} ORDER BY RAND() LIMIT {n}"
        )