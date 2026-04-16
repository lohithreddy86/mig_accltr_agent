"""
Content-addressed LLM response cache.

Key: SHA-256(category + normalized_sql)
Storage: JSON files in .cache/llm_conversions/{category}.json
In-memory LRU for hot lookups during single run.
No TTL — PL/SQL→Spark mappings don't expire; invalidated by prompt version change.
"""

import os
import re
import json
import hashlib
from functools import lru_cache


class LLMCache:
    """Persistent + in-memory cache for LLM conversion results."""

    def __init__(self, cache_dir: str = ".cache/llm_conversions"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self._memory: dict[str, dict] = {}  # category -> {hash: entry}
        self._stats = {"hits": 0, "misses": 0}
        self._load_all()

    def _load_all(self):
        """Load all category files from disk into memory."""
        if not os.path.isdir(self.cache_dir):
            return
        for fname in os.listdir(self.cache_dir):
            if fname.endswith(".json"):
                category = fname[:-5]
                path = os.path.join(self.cache_dir, fname)
                try:
                    with open(path, "r") as f:
                        self._memory[category] = json.load(f)
                except (json.JSONDecodeError, IOError):
                    self._memory[category] = {}

    def get(self, category: str, sql: str, prompt_version: str) -> str | None:
        """Look up a cached result.

        Returns None on miss or if prompt version has changed.
        """
        key = self._make_key(category, sql)
        cat_cache = self._memory.get(category, {})
        entry = cat_cache.get(key)

        if entry and entry.get("prompt_version") == prompt_version:
            self._stats["hits"] += 1
            return entry["result"]

        self._stats["misses"] += 1
        return None

    def put(self, category: str, sql: str, prompt_version: str, result: str):
        """Store a validated result."""
        key = self._make_key(category, sql)
        if category not in self._memory:
            self._memory[category] = {}

        self._memory[category][key] = {
            "prompt_version": prompt_version,
            "result": result,
            "normalized_sql": self._normalize(sql)[:200],  # For debugging
        }
        self._flush(category)

    def _make_key(self, category: str, sql: str) -> str:
        normalized = self._normalize(sql)
        raw = f"{category}:{normalized}"
        return hashlib.sha256(raw.encode()).hexdigest()

    @staticmethod
    def _normalize(sql: str) -> str:
        """Normalize SQL for cache key: lowercase keywords, strip whitespace,
        replace string literals with placeholders."""
        s = sql.strip()
        # Replace string literals with placeholder
        s = re.sub(r"'[^']*'", "'?'", s)
        # Replace numeric literals with placeholder
        s = re.sub(r"\b\d+\b", "?", s)
        # Collapse whitespace
        s = re.sub(r"\s+", " ", s)
        return s.lower()

    def _flush(self, category: str):
        """Write a category's cache to disk."""
        path = os.path.join(self.cache_dir, f"{category}.json")
        try:
            with open(path, "w") as f:
                json.dump(self._memory.get(category, {}), f, indent=2)
        except IOError:
            pass  # Best effort

    @property
    def stats(self) -> dict:
        return dict(self._stats)
