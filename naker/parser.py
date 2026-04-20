# ============================================================
# NAKER SENTINEL — Parser Module
# Path: naker/parser.py
# HTML parsing & content extraction dari artikel berita
# ============================================================

import re
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse

logger = logging.getLogger("naker.parser")


@dataclass
class ParsedArticle:
    """Struktur data hasil parsing artikel."""
    url: str = ""
    source: str = ""
    title: str = ""
    content: str = ""
    author: str = ""
    published_date: str = ""
    tags: List[str] = field(default_factory=list)
    images: List[str] = field(default_factory=list)
    summary: str = ""
    word_count: int = 0
    parse_success: bool = False
    parse_error: str = ""
    raw_html_length: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "url": self.url,
            "source": self.source,
            "title": self.title,
            "content": self.content,
            "author": self.author,
            "published_date": self.published_date,
            "tags": self.tags,
            "images": self.images,
            "summary": self.summary,
            "word_count": self.word_count,
            "parse_success": self.parse_success,
            "parse_error": self.parse_error,
            "metadata": self.metadata,
        }


# ============================================================
# Source-specific selectors
# ============================================================
SOURCE_SELECTORS = {
    "pikiran-rakyat.com": {
        "title": ["h1.read__title", "h1.title", "h1"],
        "content": [".read__content", ".article__content", "article .content"],
        "author": [".read__credit__item a", ".author__name", "[rel='author']"],
        "date": [".read__time", "time", ".date"],
        "tags": [".read__tags a", ".article__tags a", ".tag a"],
    },
    "bandung.kompas.com": {
        "title": ["h1.read__title", "h1"],
        "content": [".read__content", ".clearfix"],
        "author": [".read__credit__item a", ".credit-title-name a"],
        "date": [".read__time", "time"],
        "tags": [".tag__article__item a", ".tag a"],
    },
    "ayobandung.com": {
        "title": ["h1.title", "h1.post-title", "h1"],
        "content": [".article-content", ".post-content", ".content-detail"],
        "author": [".author-name", ".credit a", "[rel='author']"],
        "date": [".post-date", "time", ".date"],
        "tags": [".post-tag a", ".tag a"],
    },
    "tempo.co": {
        "title": ["h1.title", "h1"],
        "content": [".detail-in", ".content-detail", "article .text"],
        "author": [".author a", ".credit a"],
        "date": [".date", "time"],
        "tags": [".tag-article a", ".keyword a"],
    },
    "tirto.id": {
        "title": ["h1.content-title", "h1"],
        "content": [".content-text-editor", ".content-text"],
        "author": [".reporter a", ".author a"],
        "date": [".date", "time"],
        "tags": [".tag-list a", ".tags a"],
    },
    "cnnindonesia.com": {
        "title": ["h1.title", "h1"],
        "content": [".detail-text", ".content"],
        "author": [".author a", ".credit a"],
        "date": [".date", "time"],
        "tags": [".tags a", ".keyword a"],
    },
    "jabar.tribunnews.com": {
        "title": ["h1.f50", "h1"],
        "content": [".side-article", ".txt-article"],
        "author": [".credit a", ".author a"],
        "date": ["time", ".date"],
        "tags": [".tag a"],
    },
    "kumparan.com": {
        "title": ["h1", "[data-qa-id='title']"],
        "content": [".content", ".article-content"],
        "author": [".author-name", ".credit a"],
        "date": ["time", ".date"],
        "tags": [".tag a"],
    },
    "narasi.tv": {
        "title": ["h1", ".article-title"],
        "content": [".article-content", ".content"],
        "author": [".author a", ".writer a"],
        "date": ["time", ".date"],
        "tags": [".tag a"],
    },
    "rri.co.id": {
        "title": ["h1", ".post-title"],
        "content": [".post-content", ".article-content"],
        "author": [".author a", ".reporter a"],
        "date": ["time", ".date"],
        "tags": [".tag a"],
    },
    "radarbandung.id": {
        "title": ["h1", ".post-title"],
        "content": [".post-content", ".entry-content"],
        "author": [".author a"],
        "date": ["time", ".date"],
        "tags": [".tag a"],
    },
    "prfmnews.id": {
        "title": ["h1", ".article__title"],
        "content": [".read__content", ".article-content"],
        "author": [".credit a", ".author a"],
        "date": ["time", ".date"],
        "tags": [".tag a"],
    },
    "jabarekspres.com": {
        "title": ["h1", ".post-title"],
        "content": [".post-content", ".entry-content"],
        "author": [".author a"],
        "date": ["time", ".date"],
        "tags": [".tag a"],
    },
}

# Fallback / generic selectors
FALLBACK_SELECTORS = {
    "title": [
        "h1.entry-title", "h1.post-title", "h1.title", "h1",
        "meta[property='og:title']",
    ],
    "content": [
        ".entry-content", ".post-content", ".article-content",
        ".content", "article", ".text", "main",
    ],
    "author": [
        "[rel='author']", ".author a", ".writer", ".credit a",
        "meta[name='author']",
    ],
    "date": [
        "time[datetime]", "time", ".date", ".published",
        "meta[property='article:published_time']",
    ],
    "tags": [
        ".tag a", ".tags a", ".keyword a", ".post-tag a",
    ],
}


# ============================================================
# Date parsing helpers — BUG FIX: multiple format support
# ============================================================
DATE_FORMATS = [
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y",
    "%d %B %Y %H:%M",
    "%d %B %Y",
    "%d %b %Y %H:%M",
    "%d %b %Y",
    "%B %d, %Y",
    "%b %d, %Y",
]

# Indonesian month names for pre-processing
INDO_MONTHS = {
    "januari": "January", "februari": "February", "maret": "March",
    "april": "April", "mei": "May", "juni": "June",
    "juli": "July", "agustus": "August", "september": "September",
    "oktober": "October", "november": "November", "desember": "December",
}


def parse_date_safe(date_str: str) -> str:
    """
    Parse date string with multiple format support.
    BUG FIX: handles Indonesian month names + timezone offsets.
    Returns ISO format string or original string if unparseable.
    """
    if not date_str or not date_str.strip():
        return ""

    cleaned = date_str.strip()

    # Replace Indonesian month names
    lower = cleaned.lower()
    for indo, eng in INDO_MONTHS.items():
        if indo in lower:
            cleaned = re.sub(indo, eng, cleaned, flags=re.IGNORECASE)
            break

    # Remove common Indonesian prefixes
    cleaned = re.sub(r"^(Senin|Selasa|Rabu|Kamis|Jumat|Sabtu|Minggu),?\s*",
                     "", cleaned, flags=re.IGNORECASE)

    # Remove "WIB", "WITA", "WIT" timezone markers
    cleaned = re.sub(r"\s*(WIB|WITA|WIT)\s*$", "", cleaned, flags=re.IGNORECASE)

    # Try each format
    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(cleaned.strip(), fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

    # Try extracting from ISO 8601 with regex
    iso_match = re.search(r"(\d{4}-\d{2}-\d{2})", cleaned)
    if iso_match:
        return iso_match.group(1)

    logger.debug(f"Could not parse date: '{date_str}'")
    return date_str.strip()


# ============================================================
# Text cleaning helpers
# ============================================================
def clean_text(text: str) -> str:
    """Clean extracted text content."""
    if not text:
        return ""
    # Remove excessive whitespace
    text = re.sub(r"\s+", " ", text)
    # Remove common noise patterns
    noise_patterns = [
        r"Baca juga:.*?(?=\.|$)",
        r"BACA JUGA:.*?(?=\.|$)",
        r"Baca Juga:.*?(?=\.|$)",
        r"Lihat juga:.*?(?=\.|$)",
        r"Simak juga:.*?(?=\.|$)",
        r"ADVERTISEMENT",
        r"SCROLL TO CONTINUE.*",
        r"Halaman \d+ dari \d+",
        r"Halaman Selanjutnya.*",
        r"\[Gambas:.*?\]",
        r"Google News",
        r"Ikuti kami di.*",
        r"Follow @\w+",
        r"Editor:.*?(?=\n|$)",
        r"Pewarta:.*?(?=\n|$)",
        r"Sumber:.*?(?=\n|$)",
    ]
    for pattern in noise_patterns:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)
    # Final cleanup
    text = re.sub(r"\s+", " ", text).strip()
    return text


def generate_summary(content: str, max_length: int = 200) -> str:
    """
    Auto-generate summary from content.
    BUG FIX: safe against empty/short content and index errors.
    """
    if not content or len(content.strip()) < 10:
        return ""

    clean = content.strip()

    # Try to get first 2 sentences
    sentences = re.split(r"(?<=[.!?])\s+", clean)
    sentences = [s.strip() for s in sentences if len(s.strip()) > 15]

    if not sentences:
        # Fallback: just truncate
        return clean[:max_length].rsplit(" ", 1)[0] + "..." if len(clean) > max_length else clean

    summary = sentences[0]
    if len(sentences) > 1 and len(summary) + len(sentences[1]) + 2 <= max_length:
        summary = summary + " " + sentences[1]

    if len(summary) > max_length:
        summary = summary[:max_length].rsplit(" ", 1)[0] + "..."

    return summary


# ============================================================
# Main Parser Class
# ============================================================
class ArticleParser:
    """Parses raw HTML into structured article data."""

    def __init__(self, config: dict = None):
        cfg = (config or {}).get("parser", {})
        self.min_word_count = cfg.get("min_word_count", 50)
        self.max_word_count = cfg.get("max_word_count", 50000)
        self.summary_length = cfg.get("summary_length", 200)
        self.extract_metadata = cfg.get("extract_metadata", True)
        self._stats = {
            "total_parsed": 0,
            "successful": 0,
            "failed": 0,
            "low_quality": 0,
        }

    def _get_selectors(self, source: str) -> dict:
        """Get source-specific selectors or fallback."""
        # Match source name to selector keys
        for key in SOURCE_SELECTORS:
            if key in source.lower():
                return SOURCE_SELECTORS[key]
        return FALLBACK_SELECTORS

    def _extract_with_selectors(self, soup, selectors: list, attr: str = None) -> str:
        """Try multiple CSS selectors, return first match."""
        for selector in selectors:
            try:
                # Handle meta tags
                if selector.startswith("meta["):
                    tag = soup.select_one(selector)
                    if tag:
                        return (tag.get("content", "") or "").strip()
                else:
                    tag = soup.select_one(selector)
                    if tag:
                        if attr:
                            return (tag.get(attr, "") or "").strip()
                        return tag.get_text(strip=True)
            except Exception:
                continue
        return ""

    def _extract_content(self, soup, selectors: list) -> str:
        """Extract main content, joining all paragraphs."""
        for selector in selectors:
            try:
                container = soup.select_one(selector)
                if container:
                    # Remove unwanted elements
                    for tag in container.select(
                        "script, style, nav, footer, aside, .ads, "
                        ".advertisement, .social-share, .related-article, "
                        ".sidebar, .comment, .breadcrumb, noscript, iframe"
                    ):
                        tag.decompose()

                    paragraphs = container.find_all("p")
                    if paragraphs:
                        text = " ".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
                    else:
                        text = container.get_text(separator=" ", strip=True)

                    text = clean_text(text)
                    if len(text.split()) >= self.min_word_count // 2:
                        return text
            except Exception:
                continue
        return ""

    def _extract_tags(self, soup, selectors: list) -> List[str]:
        """Extract article tags/keywords."""
        tags = []
        for selector in selectors:
            try:
                elements = soup.select(selector)
                for el in elements:
                    tag_text = el.get_text(strip=True)
                    if tag_text and len(tag_text) < 50 and tag_text not in tags:
                        tags.append(tag_text)
                if tags:
                    return tags[:15]
            except Exception:
                continue
        return tags

    def _extract_images(self, soup, base_url: str = "") -> List[str]:
        """Extract article images."""
        images = []
        seen = set()
        for img in soup.select("article img, .content img, .entry-content img, .post-content img"):
            src = img.get("src") or img.get("data-src") or ""
            if src and src not in seen and not src.startswith("data:"):
                seen.add(src)
                images.append(src)
                if len(images) >= 10:
                    break
        return images

    def _extract_date(self, soup, selectors: list) -> str:
        """Extract and parse publication date."""
        # Try datetime attribute first
        for selector in selectors:
            try:
                tag = soup.select_one(selector)
                if tag:
                    dt_attr = tag.get("datetime", "")
                    if dt_attr:
                        return parse_date_safe(dt_attr)
                    text = tag.get_text(strip=True)
                    if text:
                        return parse_date_safe(text)
            except Exception:
                continue

        # Try meta tags
        for meta_prop in ["article:published_time", "datePublished", "pubdate"]:
            tag = soup.select_one(f"meta[property='{meta_prop}']") or \
                  soup.select_one(f"meta[name='{meta_prop}']")
            if tag:
                content = tag.get("content", "")
                if content:
                    return parse_date_safe(content)

        return ""

    def parse(self, article: dict) -> ParsedArticle:
        """
        Parse a single article from raw HTML.
        BUG FIX: graceful handling of BeautifulSoup import and parse errors.
        """
        self._stats["total_parsed"] += 1
        result = ParsedArticle(
            url=article.get("url", ""),
            source=article.get("source", ""),
        )

        raw_html = article.get("raw_html")
        if not raw_html:
            result.parse_error = "No raw HTML content"
            self._stats["failed"] += 1
            return result

        result.raw_html_length = len(raw_html)

        try:
            from bs4 import BeautifulSoup
        except ImportError:
            result.parse_error = "BeautifulSoup not installed"
            self._stats["failed"] += 1
            logger.error("BeautifulSoup not installed: pip install beautifulsoup4")
            return result

        try:
            soup = BeautifulSoup(raw_html, "html.parser")
        except Exception as e:
            result.parse_error = f"HTML parse error: {e}"
            self._stats["failed"] += 1
            logger.error(f"Failed to parse HTML for {result.url}: {e}")
            return result

        try:
            selectors = self._get_selectors(result.source)

            # Extract title
            result.title = self._extract_with_selectors(
                soup, selectors.get("title", FALLBACK_SELECTORS["title"])
            )
            if not result.title:
                # Try og:title as last resort
                og = soup.select_one("meta[property='og:title']")
                if og:
                    result.title = (og.get("content", "") or "").strip()

            # Extract content
            result.content = self._extract_content(
                soup, selectors.get("content", FALLBACK_SELECTORS["content"])
            )

            # Extract author
            result.author = self._extract_with_selectors(
                soup, selectors.get("author", FALLBACK_SELECTORS["author"])
            )

            # Extract date
            result.published_date = self._extract_date(
                soup, selectors.get("date", FALLBACK_SELECTORS["date"])
            )

            # Extract tags
            result.tags = self._extract_tags(
                soup, selectors.get("tags", FALLBACK_SELECTORS["tags"])
            )

            # Extract images
            result.images = self._extract_images(soup, article.get("url", ""))

            # Word count
            result.word_count = len(result.content.split()) if result.content else 0

            # Auto-generate summary
            result.summary = generate_summary(result.content, self.summary_length)

            # Extract metadata from meta tags
            if self.extract_metadata:
                result.metadata = self._extract_meta(soup)

            # Quality check
            if result.content and result.word_count >= self.min_word_count:
                result.parse_success = True
                self._stats["successful"] += 1
            elif result.content:
                result.parse_success = True
                result.parse_error = f"Low word count: {result.word_count}"
                self._stats["successful"] += 1
                self._stats["low_quality"] += 1
            else:
                result.parse_error = "No content extracted"
                self._stats["failed"] += 1

        except Exception as e:
            result.parse_error = f"Extraction error: {e}"
            self._stats["failed"] += 1
            logger.error(f"Error extracting content from {result.url}: {e}")

        return result

    def _extract_meta(self, soup) -> dict:
        """Extract useful metadata from meta tags."""
        meta = {}
        meta_mappings = {
            "og:description": "description",
            "og:image": "image",
            "og:site_name": "site_name",
            "og:type": "type",
            "keywords": "keywords",
            "news_keywords": "news_keywords",
        }
        for prop, key in meta_mappings.items():
            tag = soup.select_one(f"meta[property='{prop}']") or \
                  soup.select_one(f"meta[name='{prop}']")
            if tag:
                content = (tag.get("content", "") or "").strip()
                if content:
                    meta[key] = content
        return meta

    def parse_batch(self, articles: list) -> list:
        """Parse multiple articles."""
        results = []
        for article in articles:
            parsed = self.parse(article)
            results.append(parsed)
        return results

    @property
    def stats(self) -> dict:
        total = self._stats["total_parsed"]
        return {
            **self._stats,
            "success_rate": (
                f"{self._stats['successful']}/{total}"
                if total > 0 else "0/0"
            ),
        }
    
    def extract_article_content(html: str, url: str, selectors: dict = None) -> dict:
        """
        Bridge function to maintain compatibility with NakerSentinel.
        Wraps ArticleParser class logic into a simple functional call.
        """
        from urllib.parse import urlparse
        
        parser = ArticleParser()
        # Menyiapkan payload sesuai format yang diharapkan oleh kelas ArticleParser
        article_payload = {
            "raw_html": html,
            "url": url,
            "source": urlparse(url).netloc,
            "selectors": selectors
        }
        
        parsed_obj = parser.parse(article_payload)
        # Mengembalikan hasil dalam bentuk dictionary murni
        return parsed_obj.to_dict()