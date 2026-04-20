# ============================================================
# NAKER SENTINEL — Package Init
# Path: naker/__init__.py
# ============================================================

"""
Naker Sentinel — BPS Employment News Scraper & Auditor
Modular pipeline for collecting, analyzing, and scoring
employment-related news for Bandung and West Java region.

Modules:
    - loader      : Configuration management & caching
    - scraper     : Async web scraping with Playwright
    - parser      : HTML parsing & content extraction
    - scorer      : Relevance scoring & pre-flight checks
    - interrogator: AI-powered analysis via Ollama/Llama
    - manager     : Data persistence, checkpoints, merging
    - sentinel    : Main orchestrator (full pipeline)

Usage:
    # CLI
    python -m naker.sentinel --config naker/config/config.yaml

    # Programmatic
    from naker.sentinel import NakerSentinel
    import asyncio

    sentinel = NakerSentinel("naker/config/config.yaml")
    result = asyncio.run(sentinel.run())
"""

__version__ = "66.0.0"
__author__ = "BPS Naker Team"
from naker.loader import ConfigLoader
from naker.scraper import NewsScraper
from naker.parser import ArticleParser
from naker.scorer import RelevanceScorer
from naker.interrogator import AIInterrogator
from naker.manager import DataManager
__all__ = [
    "ConfigLoader", "NewsScraper", "ArticleParser", "RelevanceScorer",
    "AIInterrogator", "DataManager", "NakerSentinel",
]