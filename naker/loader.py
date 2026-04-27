# ============================================================
# NAKER SENTINEL — Loader Module
# Path: naker/loader.py
# Configuration management, caching & prompt loading
# ============================================================

import json
import hashlib
import logging
import os
import platform
import psutil
import time
from pathlib import Path
from typing import Any, Optional

import yaml

logger = logging.getLogger("naker.loader")


# ============================================================
# NakerConfig — Dataclass-style config defaults
# ============================================================
class NakerConfig:
    """Default configuration presets for different system profiles."""

    PROFILES = {
        "low_spec": {
            "scraper": {
                "max_concurrent_requests": 2,
                "request_timeout": 45,
                "retry_attempts": 2,
                "rate_limit_delay": 3.0,
                "max_articles_per_source": 20,
                "max_total_articles": 200,
            },
            "interrogation": {
                "batch_size": 2,
                "timeout": 180,
                "max_tokens": 1024,
            },
            "data_management": {
                "checkpoint_interval": 5,
            },
        },
        "high_performance": {
            "scraper": {
                "max_concurrent_requests": 10,
                "request_timeout": 20,
                "retry_attempts": 3,
                "rate_limit_delay": 0.5,
                "max_articles_per_source": 100,
                "max_total_articles": 1000,
            },
            "interrogation": {
                "batch_size": 10,
                "timeout": 60,
                "max_tokens": 4096,
            },
            "data_management": {
                "checkpoint_interval": 25,
            },
        },
    }

    @classmethod
    def get_profile(cls, name: str) -> dict:
        return cls.PROFILES.get(name, {})


# ============================================================
# FileCache — lightweight file-based cache
# ============================================================
class FileCache:
    """Simple file-based cache with expiry."""

    def __init__(self, cache_dir: str = ".naker_cache", default_ttl: int = 3600):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.default_ttl = default_ttl
        self._hits = 0
        self._misses = 0

    def _key_path(self, key: str) -> Path:
        hashed = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"{hashed}.json"

    def get(self, key: str) -> Optional[Any]:
        path = self._key_path(key)
        if not path.exists():
            self._misses += 1
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if time.time() > data.get("expires", 0):
                path.unlink(missing_ok=True)
                self._misses += 1
                return None
            self._hits += 1
            return data.get("value")
        except (json.JSONDecodeError, OSError):
            self._misses += 1
            return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        path = self._key_path(key)
        ttl = ttl or self.default_ttl
        data = {"value": value, "expires": time.time() + ttl, "key": key}
        try:
            path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        except OSError as e:
            logger.warning(f"Cache write failed for {key}: {e}")

    def clear(self):
        for f in self.cache_dir.glob("*.json"):
            f.unlink(missing_ok=True)
        logger.info("Cache cleared")

    @property
    def stats(self) -> dict:
        total = self._hits + self._misses
        return {
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": f"{self._hits / total * 100:.1f}%" if total > 0 else "N/A",
            "cached_files": len(list(self.cache_dir.glob("*.json"))),
        }


# ============================================================
# PromptLoader — loads and validates prompt templates
# ============================================================
class PromptLoader:
    """Loads prompt templates from files."""

    def __init__(self, prompt_dir: str = "naker/prompts"):
        self.prompt_dir = Path(prompt_dir)

    def load(self, filename: str) -> Optional[str]:
        path = self.prompt_dir / filename
        if not path.exists():
            logger.warning(f"Prompt file not found: {path}")
            return None
        try:
            text = path.read_text(encoding="utf-8").strip()
            if not text:
                logger.warning(f"Prompt file is empty: {path}")
                return None
            logger.info(f"Loaded prompt: {path} ({len(text)} chars)")
            return text
        except Exception as e:
            logger.error(f"Error loading prompt {path}: {e}")
            return None

    def list_prompts(self) -> list[str]:
        if not self.prompt_dir.exists():
            return []
        return [f.name for f in self.prompt_dir.glob("*.txt")]


# ============================================================
# System Profile Detection
# ============================================================
def detect_system_profile() -> str:
    """Auto-detect system resources to pick config preset."""
    try:
        ram_gb = psutil.virtual_memory().total / (1024 ** 3)
        cpu_count = os.cpu_count() or 2
        if ram_gb < 6 or cpu_count <= 2:
            logger.info(f"Detected low-spec system: {ram_gb:.1f}GB RAM, {cpu_count} CPUs")
            return "low_spec"
        else:
            logger.info(f"Detected capable system: {ram_gb:.1f}GB RAM, {cpu_count} CPUs")
            return "high_performance"
    except Exception:
        logger.warning("Could not detect system profile, defaulting to low_spec")
        return "low_spec"


# ============================================================
# ConfigLoader — main configuration loader
# ============================================================
class ConfigLoader:
    """Loads configuration from YAML, applies system profile overrides."""

    def __init__(self, config_path: Optional[str] = None, profile: Optional[str] = None):
        self.config_path = Path(config_path) if config_path else None
        self.profile = profile or detect_system_profile()
        self.config = {}
        self._load()

    def _load(self):
        # Load from YAML if provided
        if self.config_path and self.config_path.exists():
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    self.config = yaml.safe_load(f) or {}
                logger.info(f"Loaded config from {self.config_path}")
            except Exception as e:
                logger.error(f"Failed to load config: {e}")
                self.config = {}
        else:
            if self.config_path:
                logger.warning(f"Config file not found: {self.config_path}")
            self.config = {}

        profile_overrides = NakerConfig.get_profile(self.profile)
        if profile_overrides:
            self._deep_merge(self.config, profile_overrides)
            logger.info(f"Applied '{self.profile}' profile overrides")

    @staticmethod
    def _deep_merge(base: dict, override: dict):
        """Recursively merge override into base."""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                ConfigLoader._deep_merge(base[key], value)
            else:
                base[key] = value

    def get(self, section: str, default: Any = None) -> Any:
        return self.config.get(section, default)

    def __getitem__(self, key: str) -> Any:
        return self.config[key]

    def __contains__(self, key: str) -> bool:
        return key in self.config

    @property
    def all(self) -> dict:
        return self.config