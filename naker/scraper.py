# ============================================================
# NAKER SENTINEL — Scraper Module
# Path: naker/scraper.py
# Async web scraper for Bandung employment news
# ============================================================

import asyncio
import hashlib
import logging
import random
import time
from typing import Optional
from urllib.parse import quote_plus, urljoin

import aiohttp
from bs4 import BeautifulSoup

logger = logging.getLogger("naker.scraper")


# ============================================================
# Source definitions — updated 2025
# ============================================================
DEFAULT_SOURCES = [
    {
        "name": "bandung.go.id",
        "base_url": "https://bandung.go.id",
        "search_path": "/?s={query}",
        "category": "government",
        "credibility": "high",
        "selectors": {
            "article_links": "h2.entry-title a, h3.entry-title a, .post-title a",
            "title": "h1.entry-title, h1.post-title",
            "content": ".entry-content, .post-content, article",
        },
    },
    {
        "name": "tempo.co",
        "base_url": "https://www.tempo.co",
        "search_path": "/search?q={query}",
        "category": "national_media",
        "credibility": "high",
        "selectors": {
            "article_links": ".card-box a, .title a, h2 a",
            "title": "h1",
            "content": ".detail-in, .content-detail, article",
        },
    },
    {
        "name": "tirto.id",
        "base_url": "https://tirto.id",
        "search_path": "/search?q={query}",
        "category": "national_media",
        "credibility": "high",
        "selectors": {
            "article_links": ".news-list-item a, h2 a, .title a",
            "title": "h1",
            "content": ".content-text, .content-text-editor, article",
        },
    },
    {
        "name": "narasi.tv",
        "base_url": "https://narasi.tv",
        "search_path": "/search?q={query}",
        "category": "national_media",
        "credibility": "high",
        "selectors": {
            "article_links": ".article-card a, h3 a, .title a",
            "title": "h1",
            "content": ".article-content, .content, article",
        },
    },
    {
        "name": "ayobandung.com",
        "base_url": "https://www.ayobandung.com",
        "search_path": "/search?q={query}",
        "category": "local_media",
        "credibility": "high",
        "selectors": {
            "article_links": ".post-title a, h2 a, .article-title a",
            "title": "h1.title, h1",
            "content": ".article-content, .post-content, article",
        },
    },
    {
        "name": "pikiran-rakyat.com",
        "base_url": "https://www.pikiran-rakyat.com",
        "search_path": "/search?q={query}",
        "category": "local_media",
        "credibility": "high",
        "selectors": {
            "article_links": ".article__title a, h2 a, .title a",
            "title": "h1.read__title, h1",
            "content": ".read__content, .article__content, article",
        },
    },
    {
        "name": "bandung.kompas.com",
        "base_url": "https://bandung.kompas.com",
        "search_path": "/search/?q={query}",
        "category": "national_media",
        "credibility": "high",
        "selectors": {
            "article_links": ".article__title a, h2 a, .gsc-thumbnail-inside a",
            "title": "h1.read__title, h1",
            "content": ".read__content, .clearfix, article",
        },
    },
    {
        "name": "disdagin.bandung.go.id",
        "base_url": "https://disdagin.bandung.go.id",
        "search_path": "/?s={query}",
        "category": "government",
        "credibility": "high",
        "selectors": {
            "article_links": "h2.entry-title a, .post-title a",
            "title": "h1.entry-title, h1",
            "content": ".entry-content, .post-content, article",
        },
    },
    {
        "name": "radarbandung.id",
        "base_url": "https://www.radarbandung.id",
        "search_path": "/search?q={query}",
        "category": "local_media",
        "credibility": "medium",
        "selectors": {
            "article_links": ".post-title a, h2 a, .title a",
            "title": "h1",
            "content": ".post-content, .entry-content, article",
        },
    },
    {
        "name": "kumparan.com",
        "base_url": "https://kumparan.com",
        "search_path": "/search?q={query}",
        "category": "national_media",
        "credibility": "medium",
        "selectors": {
            "article_links": "a[data-qa-id='news-item'], h2 a, .title a",
            "title": "h1",
            "content": ".content, .article-content, article",
        },
    },
    {
        "name": "cnnindonesia.com",
        "base_url": "https://www.cnnindonesia.com",
        "search_path": "/search/?query={query}",
        "category": "national_media",
        "credibility": "high",
        "selectors": {
            "article_links": ".media__title a, h2 a, .title a",
            "title": "h1",
            "content": ".detail-text, .content, article",
        },
    },
    {
        "name": "rri.co.id",
        "base_url": "https://www.rri.co.id",
        "search_path": "/search?q={query}",
        "category": "national_media",
        "credibility": "high",
        "selectors": {
            "article_links": ".post-title a, h2 a, .title a",
            "title": "h1",
            "content": ".post-content, .article-content, article",
        },
    },
    {
        "name": "infobandungkota.com",
        "base_url": "https://infobandungkota.com",
        "search_path": "/?s={query}",
        "category": "local_media",
        "credibility": "medium",
        "selectors": {
            "article_links": "h2.entry-title a, .post-title a",
            "title": "h1.entry-title, h1",
            "content": ".entry-content, .post-content, article",
        },
    },
    {
        "name": "prfmnews.id",
        "base_url": "https://prfmnews.id",
        "search_path": "/search?q={query}",
        "category": "local_media",
        "credibility": "medium",
        "selectors": {
            "article_links": ".article__title a, h2 a, .title a",
            "title": "h1",
            "content": ".read__content, .article-content, article",
        },
    },
    {
        "name": "kilasbandungnews.com",
        "base_url": "https://kilasbandungnews.com",
        "search_path": "/?s={query}",
        "category": "local_media",
        "credibility": "medium",
        "selectors": {
            "article_links": "h2.entry-title a, .post-title a",
            "title": "h1.entry-title, h1",
            "content": ".entry-content, .post-content, article",
        },
    },
    {
        "name": "bandungbergerak.id",
        "base_url": "https://bandungbergerak.id",
        "search_path": "/?s={query}",
        "category": "local_media",
        "credibility": "medium",
        "selectors": {
            "article_links": "h2.entry-title a, .post-title a",
            "title": "h1.entry-title, h1",
            "content": ".entry-content, .post-content, article",
        },
    },
    {
        "name": "koranmandala.com",
        "base_url": "https://koranmandala.com",
        "search_path": "/?s={query}",
        "category": "local_media",
        "credibility": "medium",
        "selectors": {
            "article_links": "h2.entry-title a, .post-title a",
            "title": "h1.entry-title, h1",
            "content": ".entry-content, .post-content, article",
        },
    },
    {
        "name": "jabarekspres.com",
        "base_url": "https://jabarekspres.com",
        "search_path": "/search?q={query}",
        "category": "regional_media",
        "credibility": "medium",
        "selectors": {
            "article_links": ".post-title a, h2 a, .title a",
            "title": "h1",
            "content": ".post-content, .entry-content, article",
        },
    },
    {
        "name": "jabarprov.go.id",
        "base_url": "https://jabarprov.go.id",
        "search_path": "/?s={query}",
        "category": "government",
        "credibility": "high",
        "selectors": {
            "article_links": "h2.entry-title a, .post-title a, .title a",
            "title": "h1",
            "content": ".entry-content, .post-content, article",
        },
    },
    {
        "name": "jabar.tribunnews.com",
        "base_url": "https://jabar.tribunnews.com",
        "search_path": "/search?q={query}",
        "category": "national_media",
        "credibility": "medium",
        "selectors": {
            "article_links": ".f20 a, h3 a, .title a",
            "title": "h1.f50, h1",
            "content": ".side-article, .txt-article, article",
        },
    },
]


class NewsScraper:
    """Asynchronous news scraper with rate limiting and caching."""

    def __init__(self, config: dict, cache=None):
        scr_cfg = config.get("scraper", {})
        self.max_concurrent = scr_cfg.get("max_concurrent_requests", 5)
        self.timeout = scr_cfg.get("request_timeout", 30)
        self.retry_attempts = scr_cfg.get("retry_attempts", 3)
        self.retry_delay = scr_cfg.get("retry_delay", 2)
        self.rate_limit_delay = scr_cfg.get("rate_limit_delay", 1.5)
        self.max_per_source = scr_cfg.get("max_articles_per_source", 50)
        self.max_total = scr_cfg.get("max_total_articles", 500)
        self.user_agents = scr_cfg.get("user_agents", [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ])
        self.search_queries = scr_cfg.get("search_queries", [
            "ketenagakerjaan Bandung",
            "lowongan kerja Bandung",
            "PHK Bandung",
            "pengangguran Bandung",
        ])

        # Use config sources or defaults
        cfg_sources = scr_cfg.get("sources", [])
        if cfg_sources:
            self.sources = self._build_sources_from_config(cfg_sources)
        else:
            self.sources = DEFAULT_SOURCES

        self.cache = cache
        self._session = None
        self._semaphore = None
        self._seen_urls = set()
        self._stats = {
            "total_fetched": 0,
            "total_errors": 0,
            "total_cached": 0,
            "sources_scraped": 0,
            "total_time": 0.0,
        }

    @staticmethod
    def _build_sources_from_config(cfg_sources: list) -> list:
        """Convert config.yaml source entries to internal format."""
        built = []
        for s in cfg_sources:
            built.append({
                "name": s.get("name", "unknown"),
                "base_url": s.get("url", ""),
                "search_path": s.get("search_path", "/?s={query}"),
                "category": s.get("category", "unknown"),
                "credibility": s.get("credibility", "medium"),
                "selectors": {
                    "article_links": "h2 a, h3 a, .title a, .post-title a",
                    "title": "h1",
                    "content": ".entry-content, .post-content, .content, article",
                },
            })
        return built

    def _random_ua(self) -> str:
        return random.choice(self.user_agents)

    def _url_hash(self, url: str) -> str:
        return hashlib.md5(url.encode()).hexdigest()

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout),
                headers={"User-Agent": self._random_ua()},
            )
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.max_concurrent)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _fetch_url(self, url: str) -> Optional[str]:
        """Fetch a single URL with retries and caching."""
        # Check cache first
        if self.cache:
            cached = self.cache.get(url)
            if cached:
                self._stats["total_cached"] += 1
                return cached

        async with self._semaphore:
            for attempt in range(1, self.retry_attempts + 1):
                try:
                    headers = {"User-Agent": self._random_ua()}
                    async with self._session.get(url, headers=headers) as resp:
                        if resp.status == 200:
                            html = await resp.text()
                            self._stats["total_fetched"] += 1
                            if self.cache:
                                self.cache.set(url, html, ttl=3600)
                            return html
                        elif resp.status == 429:
                            wait = self.rate_limit_delay * (2 ** attempt)
                            logger.warning(f"Rate limited on {url}, waiting {wait:.1f}s")
                            await asyncio.sleep(wait)
                        else:
                            logger.warning(f"HTTP {resp.status} for {url} (attempt {attempt})")
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout fetching {url} (attempt {attempt})")
                except Exception as e:
                    logger.warning(f"Error fetching {url} (attempt {attempt}): {e}")

                if attempt < self.retry_attempts:
                    await asyncio.sleep(self.retry_delay * attempt)

        self._stats["total_errors"] += 1
        return None

    def _extract_article_links(self, html: str, source: dict) -> list[str]:
        """Extract article links from search results page."""
        soup = BeautifulSoup(html, "html.parser")
        selectors = source.get("selectors", {}).get("article_links", "h2 a, h3 a")
        links = []
        seen = set()

        for selector in selectors.split(","):
            selector = selector.strip()
            for tag in soup.select(selector):
                href = tag.get("href", "")
                if not href or href.startswith("#") or href.startswith("javascript:"):
                    continue
                # Make absolute URL
                if href.startswith("/"):
                    href = urljoin(source["base_url"], href)
                elif not href.startswith("http"):
                    href = urljoin(source["base_url"], href)
                # Deduplicate
                if href not in seen and href not in self._seen_urls:
                    seen.add(href)
                    links.append(href)

        return links[:self.max_per_source]

    async def _scrape_source(self, source: dict) -> list[dict]:
        """Scrape a single source for all search queries."""
        articles = []
        source_name = source["name"]
        logger.info(f"Scraping source: {source_name}")

        for query in self.search_queries:
            search_path = source["search_path"].format(query=quote_plus(query))
            search_url = source["base_url"] + search_path

            html = await self._fetch_url(search_url)
            if not html:
                continue

            links = self._extract_article_links(html, source)
            logger.info(f"  [{source_name}] query='{query}' → {len(links)} links")

            for link in links:
                if link in self._seen_urls:
                    continue
                self._seen_urls.add(link)

                articles.append({
                    "url": link,
                    "source": source_name,
                    "source_category": source.get("category", "unknown"),
                    "source_credibility": source.get("credibility", "medium"),
                    "search_query": query,
                })

                if len(articles) >= self.max_per_source:
                    break

            # Rate limiting between queries
            await asyncio.sleep(self.rate_limit_delay)

            if len(articles) >= self.max_per_source:
                break

        self._stats["sources_scraped"] += 1
        logger.info(f"  [{source_name}] total: {len(articles)} article URLs")
        return articles

    async def scrape_all(self) -> list[dict]:
        """Scrape all configured sources."""
        await self._ensure_session()
        start = time.monotonic()

        all_articles = []
        for source in self.sources:
            if len(all_articles) >= self.max_total:
                logger.info(f"Reached max total articles ({self.max_total}), stopping")
                break
            batch = await self._scrape_source(source)
            all_articles.extend(batch)

        elapsed = time.monotonic() - start
        self._stats["total_time"] = round(elapsed, 2)

        logger.info(
            f"Scraping complete: {len(all_articles)} articles from "
            f"{self._stats['sources_scraped']} sources in {elapsed:.1f}s"
        )
        return all_articles[:self.max_total]

    async def fetch_article_content(self, article: dict) -> dict:
        """Fetch the full HTML of an article page."""
        await self._ensure_session()
        html = await self._fetch_url(article["url"])
        if html:
            article["raw_html"] = html
        else:
            article["raw_html"] = None
            article["fetch_error"] = True
        return article

    async def fetch_all_content(self, articles: list[dict]) -> list[dict]:
        """Fetch content for all articles with concurrency control."""
        await self._ensure_session()
        tasks = [self.fetch_article_content(a) for a in articles]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        processed = []
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                logger.error(f"Fetch error for {articles[i].get('url')}: {r}")
                articles[i]["raw_html"] = None
                articles[i]["fetch_error"] = True
                processed.append(articles[i])
            else:
                processed.append(r)
        return processed

    @property
    def stats(self) -> dict:
        return {**self._stats, "seen_urls": len(self._seen_urls)}