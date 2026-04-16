"""
logger.py
=========
Structured logging for the SQL Migration system.
Uses structlog for JSON/text output, per-agent log files, and optional
prompt/response logging for debugging.

Usage:
    from sql_migration.core.logger import get_logger
    log = get_logger("analysis")
    log.info("adapter_loaded", dialect="oracle", confidence=0.94)
    log.error("sandbox_timeout", proc="sp_calc_interest", elapsed=301)
"""

from __future__ import annotations

import logging
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any

import structlog
from structlog.types import EventDict, WrappedLogger

# ---------------------------------------------------------------------------
# Custom processors
# ---------------------------------------------------------------------------

def _add_agent_name(
    agent: str,
) -> structlog.types.Processor:
    """Factory: returns a processor that stamps every log entry with agent name.

    NOTE: For structlog-based loggers (the primary code path), this processor
    is superseded by get_logger()'s `base.bind(agent=agent)` which achieves
    the same effect. Per-agent file routing is handled by stdlib logger
    hierarchy (each agent gets its own logging.Logger via _get_file_handler).

    This factory is retained for use cases where raw stdlib log records need
    agent stamping — e.g., third-party libraries that emit logs without
    going through structlog, or custom handlers that need the agent field
    in the foreign_pre_chain.
    """
    def processor(
        logger: WrappedLogger,
        method: str,
        event_dict: EventDict,
    ) -> EventDict:
        event_dict.setdefault("agent", agent)
        return event_dict
    return processor


def _drop_color_message(
    logger: WrappedLogger,
    method: str,
    event_dict: EventDict,
) -> EventDict:
    """Remove uvicorn's 'color_message' key if present."""
    event_dict.pop("color_message", None)
    return event_dict


# ---------------------------------------------------------------------------
# Setup (called once at startup)
# ---------------------------------------------------------------------------

_configured = False


def configure_logging(
    level: str = "INFO",
    fmt: str = "json",
    logs_dir: str | None = None,
    per_agent_files: bool = True,
    include_prompts: bool = False,
    include_responses: bool = False,
    run_id: str = "default",
) -> None:
    """
    Configure structlog + stdlib logging.
    Call once from main entry point after config is loaded.
    """
    global _configured
    if _configured:
        return

    # Store flags for runtime use
    _flags["include_prompts"]   = include_prompts
    _flags["include_responses"] = include_responses
    _flags["logs_dir"]          = logs_dir
    _flags["per_agent_files"]   = per_agent_files
    _flags["run_id"]            = run_id

    log_level = getattr(logging, level.upper(), logging.INFO)

    # Shared processors (applied to every log call)
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        _drop_color_message,
        structlog.processors.StackInfoRenderer(),
    ]

    if fmt == "json":
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    structlog.configure(
        processors=shared_processors + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Root stdlib handler → stdout
    formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        foreign_pre_chain=shared_processors,
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)

    # Suppress noisy third-party loggers
    for noisy in ("httpx", "httpcore", "urllib3", "LiteLLM"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _configured = True


# Flags set during configure_logging
_flags: dict[str, Any] = {
    "include_prompts":   False,
    "include_responses": False,
    "logs_dir":          None,
    "per_agent_files":   True,
    "run_id":            "default",
}


# ---------------------------------------------------------------------------
# Per-agent file handler cache
# ---------------------------------------------------------------------------

_file_handlers: dict[str, logging.FileHandler] = {}


def _get_file_handler(agent: str) -> logging.FileHandler | None:
    """Create (once) and return a file handler for this agent's log file."""
    if not _flags["per_agent_files"] or not _flags["logs_dir"]:
        return None

    if agent in _file_handlers:
        return _file_handlers[agent]

    logs_dir = Path(_flags["logs_dir"])
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / f"{_flags['run_id']}_{agent}.log"

    fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    fh.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer(),
            foreign_pre_chain=[
                _add_agent_name(agent),  # Stamp agent on third-party records
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.processors.TimeStamper(fmt="iso"),
            ],
        )
    )
    _file_handlers[agent] = fh
    logging.getLogger(f"sql_migration.{agent}").addHandler(fh)
    return fh


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@lru_cache(maxsize=32)
def get_logger(agent: str) -> structlog.BoundLogger:
    """
    Return a bound structlog logger for the given agent.
    Every log call from this logger will include {"agent": agent}.

    Example:
        log = get_logger("conversion")
        log.info("chunk_converted", proc="sp_calc", chunk_id=2, lines=148)
    """
    _get_file_handler(agent)  # ensure file handler registered
    base = structlog.get_logger(f"sql_migration.{agent}")
    return base.bind(agent=agent)


def log_prompt(agent: str, prompt: str, call_id: str = "") -> None:
    """Log a full LLM prompt. Only emits if include_prompts=True in config."""
    if not _flags["include_prompts"]:
        return
    log = get_logger(agent)
    log.debug(
        "llm_prompt",
        call_id=call_id,
        prompt_length=len(prompt),
        prompt=prompt[:2000] + "..." if len(prompt) > 2000 else prompt,
    )


def log_response(agent: str, response: str, call_id: str = "") -> None:
    """Log a full LLM response. Only emits if include_responses=True in config."""
    if not _flags["include_responses"]:
        return
    log = get_logger(agent)
    log.debug(
        "llm_response",
        call_id=call_id,
        response_length=len(response),
        response=response[:2000] + "..." if len(response) > 2000 else response,
    )
