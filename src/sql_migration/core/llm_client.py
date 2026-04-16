"""
llm_client.py
=============
LiteLLM wrapper with:
  - Per-agent model routing
  - Automatic retry with exponential backoff
  - Tool-calling support for agentic loops
  - JSON mode enforcement
  - Enterprise proxy support (api_base, api_key, ssl_verify)
  - Prompt/response logging (opt-in via config)
  - Token usage tracking

Usage:
    from sql_migration.core.llm_client import LLMClient
    client = LLMClient(agent="analysis")

    # Single call (no history)
    response = client.call(messages=[...])

    # Tool-calling (for agentic loops)
    response = client.call_with_tools(messages=[...], tools=[...])
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

import litellm
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from sql_migration.core.config_loader import get_config
from sql_migration.core.error_handling import ErrorClass, ErrorClassifier
from sql_migration.core.logger import get_logger, log_prompt, log_response

# Silence LiteLLM's verbose output
litellm.suppress_debug_info = True

# Enterprise proxy SSL settings — must be set at module level.
# Passing ssl_verify as a kwarg to completion() has a known bug where it
# doesn't propagate to the underlying httpx client for some providers.
# Setting it here ensures it works for ALL providers.
_cfg_llm = get_config().llm
if not _cfg_llm.ssl_verify:
    litellm.ssl_verify = False

log = get_logger("llm_client")


# ---------------------------------------------------------------------------
# JSON extraction helper
# ---------------------------------------------------------------------------

def extract_json(text: str) -> Any:
    """
    Extract JSON from an LLM response.
    Handles: bare JSON, ```json ... ``` fences, JSON embedded in prose.
    Raises ValueError if no valid JSON found.
    """
    # Strip markdown fences
    stripped = text.strip()
    if stripped.startswith("```"):
        lines = stripped.split("\n")
        inner = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])
        stripped = inner.strip()

    # Try direct parse
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        pass

    # Try to find JSON object/array within prose
    import re
    for pattern in [r"\{[\s\S]*\}", r"\[[\s\S]*\]"]:
        match = re.search(pattern, stripped)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                continue

    raise ValueError(f"No valid JSON found in LLM response: {stripped[:200]}...")


# ---------------------------------------------------------------------------
# LLMClient
# ---------------------------------------------------------------------------

class LLMClient:
    """
    Per-agent LLM client with compaction, retry, and JSON extraction.
    Instantiate once per agent invocation (not per LLM call).
    """

    def __init__(self, agent: str) -> None:
        cfg = get_config()
        self.agent     = agent
        self.cfg_llm   = cfg.llm
        self.model     = cfg.llm.model_for_agent(agent)

        self.temperature    = cfg.llm.temperature
        self.max_tokens     = cfg.llm.max_tokens
        self.timeout        = cfg.llm.timeout_seconds
        self.retry_attempts = cfg.llm.retry_attempts
        self.backoff_base   = cfg.llm.retry_backoff_base

        # Enterprise proxy settings — passed to every litellm.completion() call.
        # When api_base is empty, litellm uses its default provider routing.
        self.api_base   = cfg.llm.api_base or None
        self.api_key    = cfg.llm.get_api_key() or None
        self.ssl_verify = cfg.llm.ssl_verify

        # When routing through an enterprise proxy (api_base is set), LiteLLM
        # must treat it as an OpenAI-compatible endpoint. Without a provider
        # prefix, LiteLLM infers the provider from the model name — e.g.
        # "gemini-2.5-flash" → Vertex AI → tries Google Cloud SDK → fails.
        # Prefixing with "openai/" forces the OpenAI-compatible route, which
        # just forwards the request to api_base with the model name as-is.
        if self.api_base and "/" not in self.model:
            self.model = f"openai/{self.model}"
            log.debug("proxy_model_prefix_applied",
                      original=cfg.llm.model_for_agent(agent),
                      prefixed=self.model,
                      api_base=self.api_base)

        self._total_input_tokens  = 0
        self._total_output_tokens = 0

    # ------------------------------------------------------------------
    # Enterprise proxy kwargs (injected into every litellm.completion call)
    # ------------------------------------------------------------------

    @property
    def _proxy_kwargs(self) -> dict[str, Any]:
        """Build kwargs for enterprise proxy routing.

        Returns a dict that is spread into every litellm.completion() call.
        When api_base is not configured, returns empty dict (litellm defaults).

        Handles:
          - api_base:    Enterprise proxy URL (e.g. https://genai-proxy.hdfc/v1)
          - api_key:     Resolved from the env var named in config.llm.api_key_env
          - ssl_verify:  False for enterprise proxies with internal/self-signed certs
        """
        kw: dict[str, Any] = {}
        if self.api_base:
            kw["api_base"] = self.api_base
        if self.api_key:
            kw["api_key"] = self.api_key
        if not self.ssl_verify:
            # LiteLLM passes this through to the underlying httpx client
            kw["ssl_verify"] = False
        return kw

    # ------------------------------------------------------------------
    # Tool-calling LLM call (for agentic loops)
    # ------------------------------------------------------------------

    def call_with_tools(
        self,
        messages: list[dict],
        tools: list[dict],
        call_id: str = "",
    ) -> Any:
        """
        Make a single LLM call with tool definitions.
        Returns the raw LiteLLM response object (not parsed).

        The caller (ToolExecutor.run_agentic_loop) handles:
          - Inspecting response.choices[0].message.tool_calls
          - Executing tools and appending results
          - Calling this method again with updated messages

        This method handles retry on transient errors, just like
        _call_with_retry, but returns the full response object
        (not just .content) so the caller can access tool_calls.
        """
        from collections import Counter
        from sql_migration.core.error_handling import ErrorClass, ErrorClassifier

        last_error: Exception | None = None
        error_sig_counts: Counter[str] = Counter()

        for attempt in range(1, self.retry_attempts + 1):
            try:
                kwargs: dict[str, Any] = {
                    "model":       self.model,
                    "messages":    messages,
                    "tools":       tools,
                    "temperature": self.temperature,
                    "max_tokens":  self.max_tokens,
                    "timeout":     self.timeout,
                    # LiteLLM's non-proxy provider paths sometimes drop
                    # `timeout`; `request_timeout` is the raw HTTP-layer knob
                    # honoured by every backend (including Anthropic + Gemini).
                    "request_timeout": self.timeout,
                    **self._proxy_kwargs,
                }

                result = litellm.completion(**kwargs)

                # Track token usage
                if hasattr(result, "usage") and result.usage:
                    self._total_input_tokens  += result.usage.prompt_tokens or 0
                    self._total_output_tokens += result.usage.completion_tokens or 0

                log.debug("llm_tool_call_success",
                          call_id=call_id,
                          attempt=attempt,
                          has_tool_calls=bool(
                              result.choices[0].message.tool_calls))
                return result

            except (litellm.BadRequestError, litellm.AuthenticationError) as e:
                log.error("llm_tool_call_unsolvable",
                          call_id=call_id, error=str(e))
                raise

            except Exception as e:
                error_msg = str(e)
                last_error = e
                error_class = ErrorClassifier.classify(error_msg)

                if error_class == ErrorClass.UNSOLVABLE:
                    raise

                wait = self.backoff_base ** attempt if error_class == ErrorClass.TRANSIENT else 1.0
                log.warning("llm_tool_call_retry",
                            call_id=call_id, attempt=attempt,
                            error_class=error_class.value,
                            error=error_msg[:200], wait=wait)
                if attempt < self.retry_attempts:
                    import time
                    time.sleep(wait)

        raise RuntimeError(
            f"LLM tool call failed after {self.retry_attempts} attempts. "
            f"Last error: {last_error}"
        )

    # ------------------------------------------------------------------
    # Core call (no history management)
    # ------------------------------------------------------------------

    def call(
        self,
        messages: list[dict],
        expect_json: bool = True,
        call_id: str | None = None,
    ) -> Any:
        """
        Make a single LLM call. Returns parsed JSON if expect_json=True,
        otherwise returns the raw string response.

        Retries on transient errors with exponential backoff.
        """
        call_id = call_id or uuid.uuid4().hex[:8]
        log_prompt(self.agent, _messages_to_str(messages), call_id)

        response_text = self._call_with_retry(messages, call_id)

        log_response(self.agent, response_text, call_id)

        if expect_json:
            try:
                return extract_json(response_text)
            except ValueError as e:
                log.error(
                    "json_extraction_failed",
                    call_id=call_id,
                    error=str(e),
                    response_snippet=response_text[:300],
                )
                raise

        return response_text

    # ------------------------------------------------------------------
    # Retry wrapper
    # ------------------------------------------------------------------

    def _call_with_retry(self, messages: list[dict], call_id: str) -> str:
        """
        Call LiteLLM with automatic retry on transient failures.

        Uses ErrorClassifier to distinguish:
          UNSOLVABLE  → raise immediately (auth, network, disk — no retry)
          TRANSIENT   → retry with exponential backoff (timeouts, 5xx)
          RETRYABLE   → retry with backoff (generic / unexpected)

        Error signatures are tracked so repeated identical root causes are
        logged with increasing severity even if the surface message differs
        slightly between attempts.
        """
        last_error: Exception | None = None
        from collections import Counter
        error_sig_counts: Counter[str] = Counter()

        for attempt in range(1, self.retry_attempts + 1):
            try:
                kwargs: dict[str, Any] = {
                    "model":       self.model,
                    "messages":    messages,
                    "temperature": self.temperature,
                    "max_tokens":  self.max_tokens,
                    "timeout":     self.timeout,
                    "request_timeout": self.timeout,
                    **self._proxy_kwargs,
                }

                # JSON mode: supported by Gemini and OpenAI-compat models
                if self.cfg_llm.json_mode and _supports_json_mode(self.model):
                    kwargs["response_format"] = {"type": "json_object"}

                result = litellm.completion(**kwargs)

                # Track token usage
                if hasattr(result, "usage") and result.usage:
                    self._total_input_tokens  += result.usage.prompt_tokens or 0
                    self._total_output_tokens += result.usage.completion_tokens or 0

                content = result.choices[0].message.content or ""
                log.debug(
                    "llm_call_success",
                    call_id=call_id,
                    attempt=attempt,
                    input_tokens=result.usage.prompt_tokens if result.usage else 0,
                    output_tokens=result.usage.completion_tokens if result.usage else 0,
                )
                return content

            except (litellm.BadRequestError, litellm.AuthenticationError) as e:
                # LiteLLM already classifies these; map to UNSOLVABLE and raise
                log.error("llm_unsolvable_error",
                          call_id=call_id, error=str(e), error_class="UNSOLVABLE")
                raise

            except Exception as e:
                error_msg = str(e)
                last_error = e

                # ── Classify via string patterns ──────────────────────────────
                error_class = ErrorClassifier.classify(error_msg)
                sig         = ErrorClassifier.get_signature(error_msg)
                error_sig_counts[sig] += 1
                occurrence  = error_sig_counts[sig]

                if error_class == ErrorClass.UNSOLVABLE:
                    log.error(
                        "llm_unsolvable_error",
                        call_id=call_id,
                        attempt=attempt,
                        error=error_msg,
                        error_class=error_class.value,
                        signature=sig,
                    )
                    # Re-raise immediately — retrying will never help
                    raise

                # ── Log with escalating severity based on occurrence ───────────
                # TRANSIENT (503, timeout): exponential backoff — infrastructure
                # needs time to recover. Base * 2^attempt = 2s, 4s, 8s.
                # RETRYABLE (generic): short fixed delay — a single retry often
                # succeeds. 1s delay regardless of attempt count.
                if error_class == ErrorClass.TRANSIENT:
                    wait = self.backoff_base ** attempt  # 2s, 4s, 8s
                else:
                    wait = 1.0  # RETRYABLE: short fixed delay

                log_fn = log.error if occurrence >= 3 else log.warning

                log_fn(
                    "llm_error_retry",
                    call_id=call_id,
                    attempt=attempt,
                    max_attempts=self.retry_attempts,
                    error_class=error_class.value,
                    occurrence=occurrence,
                    signature=sig[:80],
                    wait_seconds=wait,
                    error=error_msg[:200],
                )

                if attempt < self.retry_attempts:
                    time.sleep(wait)

        raise RuntimeError(
            f"LLM call failed after {self.retry_attempts} attempts. "
            f"Last error: {last_error}"
        )

    # ------------------------------------------------------------------
    # Usage stats
    # ------------------------------------------------------------------

    @property
    def token_usage(self) -> dict[str, int]:
        return {
            "input_tokens":  self._total_input_tokens,
            "output_tokens": self._total_output_tokens,
            "total_tokens":  self._total_input_tokens + self._total_output_tokens,
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _supports_json_mode(model: str) -> bool:
    """Check if the model supports response_format=json_object.

    Matches against model name fragments so both "gemini/gemini-2.5-pro"
    and "openai/gemini-2.5-pro" (enterprise proxy) are recognized.
    """
    _JSON_MODE_FRAGMENTS = {
        "gpt-4", "gpt-3.5-turbo", "gpt-4o",
        "gemini-2.5-pro", "gemini-2.5-flash",
        "gemini-2.0-flash",
        "gemini-1.5-pro", "gemini-1.5-flash",
    }
    model_lower = model.lower()
    return any(frag in model_lower for frag in _JSON_MODE_FRAGMENTS)


def _messages_to_str(messages: list[dict]) -> str:
    """Flatten messages to string for logging."""
    parts = []
    for m in messages:
        role    = m.get("role", "?")
        content = m.get("content", "")
        if isinstance(content, list):
            content = " ".join(
                p.get("text", "") if isinstance(p, dict) else str(p)
                for p in content
            )
        parts.append(f"[{role.upper()}] {content[:500]}")
    return "\n".join(parts)