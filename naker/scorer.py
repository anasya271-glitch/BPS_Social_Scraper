# ============================================================
# NAKER SENTINEL — Scorer Module
# Path: naker/scorer.py
# Relevance scoring & pre-flight article checks
# ============================================================

import re
import math
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any

logger = logging.getLogger("naker.scorer")


# ============================================================
# Keyword sets
# ============================================================
PRIMARY_KEYWORDS = [
    "ketenagakerjaan", "tenaga kerja", "pengangguran", "lowongan kerja",
    "PHK", "pemutusan hubungan kerja", "upah minimum", "UMK", "UMP", "UMR",
    "buruh", "serikat pekerja", "BPJS Ketenagakerjaan", "Disnaker",
    "Dinas Tenaga Kerja", "TPK", "Tingkat Pengangguran Terbuka", "TPAK",
    "padat karya", "outsourcing", "kontrak kerja", "magang", "pelatihan kerja",
    "angkatan kerja", "informal", "BLK", "Balai Latihan Kerja",
]

SECONDARY_KEYWORDS = [
    "UMKM", "investasi", "industri", "ekonomi", "inflasi",
    "manufaktur", "perdagangan", "pariwisata", "digitalisasi",
    "automasi", "gig economy", "BPS", "Badan Pusat Statistik",
    "sertifikasi", "pelatihan", "produktivitas",
]

GEOGRAPHIC_PRIMARY = [
    "Bandung", "Kota Bandung",
]

GEOGRAPHIC_SECONDARY = [
    "Jawa Barat", "Jabar", "Metropolitan Bandung", "Bandung Raya",
]

NEGATIVE_PATTERNS = [
    r"gosip|selebriti|artis",
    r"sepak\s*bola|liga\s*bola|pertandingan",
    r"resep\s*masakan",
    r"zodiak|ramalan|horoscope",
    r"drama\s*korea|drakor|K-pop",
]

# [BUG FIX] Pre-compile negative patterns with safe error handling
_COMPILED_NEGATIVES: List[re.Pattern] = []
for _pat in NEGATIVE_PATTERNS:
    try:
        _COMPILED_NEGATIVES.append(re.compile(_pat, re.IGNORECASE))
    except re.error as _e:
        logger.warning(f"Invalid negative regex pattern '{_pat}': {_e}")

EXCLUSION_KEYWORDS = [
    "horoscope", "zodiak", "resep masakan", "liga sepakbola",
    "gosip artis", "drakor", "K-pop",
]

# Source credibility tiers
# [BUG FIX] Added missing "low" tier that was referenced
# in _score_credibility() but never defined — caused KeyError
SOURCE_CREDIBILITY = {
    "high": [
        "bandung.go.id", "tempo.co", "tirto.id", "narasi.tv",
        "ayobandung.com", "pikiran-rakyat.com", "bandung.kompas.com",
        "disdagin.bandung.go.id", "cnnindonesia.com", "rri.co.id",
        "jabarprov.go.id", "bps.go.id", "kemnaker.go.id",
        "kompas.com", "detik.com",
    ],
    "medium": [
        "radarbandung.id", "kumparan.com", "infobandungkota.com",
        "prfmnews.id", "kilasbandungnews.com", "bandungbergerak.id",
        "koranmandala.com", "jabarekspres.com", "jabar.tribunnews.com",
        "liputan6.com", "merdeka.com", "sindonews.com",
    ],
    "low": [
        "blogspot.com", "wordpress.com", "medium.com",
        "facebook.com", "twitter.com", "instagram.com",
    ],
}


# ============================================================
# Indonesian month name mapping for date parsing
# ============================================================
_INDONESIAN_MONTHS = {
    "januari": "January", "februari": "February", "maret": "March",
    "april": "April", "mei": "May", "juni": "June",
    "juli": "July", "agustus": "August", "september": "September",
    "oktober": "October", "november": "November", "desember": "December",
}


# ============================================================
# Data Model
# ============================================================
@dataclass
class ScoredArticle:
    """Article with relevance scores attached."""
    url: str = ""
    title: str = ""
    source: str = ""
    published_date: str = ""
    content: str = ""
    word_count: int = 0

    # Scores (0.0 – 1.0)
    keyword_score: float = 0.0
    geographic_score: float = 0.0
    recency_score: float = 0.0
    quality_score: float = 0.0
    credibility_score: float = 0.0
    negative_penalty: float = 0.0

    # Composite
    total_score: float = 0.0
    relevance_label: str = "low"  # low / medium / high

    # Detail
    matched_keywords: List[str] = field(default_factory=list)
    matched_geo: List[str] = field(default_factory=list)
    penalties: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "url": self.url,
            "title": self.title,
            "source": self.source,
            "published_date": self.published_date,
            "word_count": self.word_count,
            "keyword_score": round(self.keyword_score, 4),
            "geographic_score": round(self.geographic_score, 4),
            "recency_score": round(self.recency_score, 4),
            "quality_score": round(self.quality_score, 4),
            "credibility_score": round(self.credibility_score, 4),
            "negative_penalty": round(self.negative_penalty, 4),
            "total_score": round(self.total_score, 4),
            "relevance_label": self.relevance_label,
            "matched_keywords": self.matched_keywords,
            "matched_geo": self.matched_geo,
            "penalties": self.penalties,
        }


# ============================================================
# Date parsing helper
# [BUG FIX] Robust multi-format + Indonesian month names
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
    "%d %b %Y",
    "%B %d, %Y",
    "%d-%m-%Y",
]


def parse_date_robust(date_str: str) -> Optional[datetime]:
    """
    Parse date from various formats.

    [BUG FIX] Changes from original:
    - Handles None/empty input safely
    - Strips Indonesian timezone markers (WIB/WITA/WIT)
    - Normalizes Indonesian month names to English
    - Tries more format variants
    - Falls back to ISO date extraction via regex
    """
    if not date_str or not isinstance(date_str, str):
        return None

    cleaned = date_str.strip()

    # Remove Indonesian timezone markers
    cleaned = re.sub(r"\s*(WIB|WITA|WIT)\s*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.strip()

    # [BUG FIX] Normalize Indonesian month names to English
    cleaned_lower = cleaned.lower()
    for indo_month, eng_month in _INDONESIAN_MONTHS.items():
        if indo_month in cleaned_lower:
            # Case-insensitive replace while preserving surrounding text
            cleaned = re.sub(
                re.escape(indo_month), eng_month, cleaned, flags=re.IGNORECASE
            )
            break

    # Collapse excess whitespace
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    # Try each known format
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue

    # Fallback: extract just the ISO date portion
    iso_match = re.search(r"(\d{4}-\d{2}-\d{2})", cleaned)
    if iso_match:
        try:
            return datetime.strptime(iso_match.group(1), "%Y-%m-%d")
        except ValueError:
            pass

    # [BUG FIX] Try dateutil as last resort if available
    try:
        from dateutil import parser as dateutil_parser
        return dateutil_parser.parse(cleaned, fuzzy=True)
    except Exception:
        pass

    logger.debug(f"Could not parse date: '{date_str}'")
    return None


# ============================================================
# Main Scorer Class
# ============================================================
class RelevanceScorer:
    """Scores articles for relevance to Bandung employment topics."""

    def __init__(self, config: dict = None):
        cfg = (config or {}).get("scoring", {})
        self.weights = cfg.get("weights", {
            "keyword_relevance": 0.35,
            "source_credibility": 0.15,
            "geographic_relevance": 0.20,
            "recency": 0.15,
            "content_quality": 0.15,
        })
        thresholds = cfg.get("thresholds", {})
        self.min_score = thresholds.get("minimum_score", 0.3)
        self.high_threshold = thresholds.get("high_relevance", 0.7)
        self.auto_accept = thresholds.get("auto_accept", 0.85)

        penalties = cfg.get("penalties", {})
        self.penalty_duplicate = penalties.get("duplicate_content", -0.5)
        self.penalty_low_wc = penalties.get("low_word_count", -0.2)
        self.penalty_no_date = penalties.get("no_date", -0.1)
        self.penalty_clickbait = penalties.get("clickbait", -0.3)

        self._stats = {
            "total_scored": 0,
            "high_relevance": 0,
            "medium_relevance": 0,
            "low_relevance": 0,
            "excluded": 0,
        }

    # --------------------------------------------------------
    # Sub-scorers
    # --------------------------------------------------------
    def _score_keywords(self, text: str) -> Tuple[float, List[str]]:
        """Score based on keyword matches in text."""
        if not text:
            return 0.0, []

        text_lower = text.lower()
        matched = []

        primary_hits = 0
        for kw in PRIMARY_KEYWORDS:
            if kw.lower() in text_lower:
                primary_hits += 1
                matched.append(kw)

        secondary_hits = 0
        for kw in SECONDARY_KEYWORDS:
            if kw.lower() in text_lower:
                secondary_hits += 1
                matched.append(kw)

        # [BUG FIX] Guard against division-by-zero
        max_primary = max(len(PRIMARY_KEYWORDS), 1)
        max_secondary = max(len(SECONDARY_KEYWORDS), 1)

        primary_ratio = min(primary_hits / max_primary, 1.0)
        secondary_ratio = min(secondary_hits / max_secondary, 1.0)

        score = (primary_ratio * 0.75) + (secondary_ratio * 0.25)

        # Boost: title contains primary keyword
        return min(score, 1.0), matched

    def _score_geographic(self, text: str) -> Tuple[float, List[str]]:
        """Score based on geographic keyword matches."""
        if not text:
            return 0.0, []

        text_lower = text.lower()
        matched = []
        score = 0.0

        for geo in GEOGRAPHIC_PRIMARY:
            if geo.lower() in text_lower:
                score += 0.7
                matched.append(geo)

        for geo in GEOGRAPHIC_SECONDARY:
            if geo.lower() in text_lower:
                score += 0.3
                matched.append(geo)

        return min(score, 1.0), matched

    def _score_recency(self, date_str: str) -> float:
        """Score based on article freshness."""
        parsed = parse_date_robust(date_str)
        if parsed is None:
            return 0.3  # Unknown date gets mediocre score

        now = datetime.now()
        # Make both offset-naive for comparison
        if parsed.tzinfo is not None:
            parsed = parsed.replace(tzinfo=None)

        age_days = (now - parsed).days

        if age_days < 0:
            # Future date — probably parse error; treat as recent
            return 0.8
        if age_days <= 1:
            return 1.0
        elif age_days <= 3:
            return 0.9
        elif age_days <= 7:
            return 0.8
        elif age_days <= 14:
            return 0.6
        elif age_days <= 30:
            return 0.4
        elif age_days <= 90:
            return 0.2
        else:
            return 0.1

    def _score_quality(self, article: dict) -> float:
        """Score based on content quality indicators."""
        content = article.get("content") or article.get("title") or ""
        word_count = article.get("word_count", 0)

        # [BUG FIX] Auto-compute word_count if missing/zero
        if word_count <= 0 and content:
            word_count = len(content.split())

        score = 0.5  # baseline

        # Word count factor
        if word_count >= 500:
            score += 0.3
        elif word_count >= 200:
            score += 0.15
        elif word_count < 100:
            score -= 0.2

        # Has a meaningful title
        title = article.get("title") or ""
        if len(title) > 20:
            score += 0.1

        # Content not suspiciously short
        if content and len(content) > 300:
            score += 0.1

        return max(min(score, 1.0), 0.0)

    def _score_credibility(self, source: str) -> float:
        """Score based on source domain credibility tier."""
        if not source:
            return 0.3

        source_lower = source.lower()

        for domain in SOURCE_CREDIBILITY.get("high", []):
            if domain in source_lower:
                return 1.0

        for domain in SOURCE_CREDIBILITY.get("medium", []):
            if domain in source_lower:
                return 0.6

        # [BUG FIX] Original code referenced SOURCE_CREDIBILITY["low"]
        # which did not exist, causing KeyError. Now uses .get() with fallback.
        for domain in SOURCE_CREDIBILITY.get("low", []):
            if domain in source_lower:
                return 0.2

        return 0.3  # unknown source

    def _detect_negative(self, text: str) -> Tuple[float, List[str]]:
        """Detect negative/irrelevant patterns."""
        if not text:
            return 0.0, []

        penalties = []
        penalty = 0.0
        text_lower = text.lower()

        # Check compiled negative regexes
        for pat in _COMPILED_NEGATIVES:
            try:
                if pat.search(text_lower):
                    penalties.append(f"neg_pattern:{pat.pattern}")
                    penalty += 0.15
            except Exception as e:
                logger.debug(f"Regex search error: {e}")
                continue

        # Check exclusion keywords
        for kw in EXCLUSION_KEYWORDS:
            if kw.lower() in text_lower:
                penalties.append(f"exclusion:{kw}")
                penalty += 0.1

        return min(penalty, 1.0), penalties

    # --------------------------------------------------------
    # Main scoring pipeline
    # --------------------------------------------------------
    def score(self, article: dict) -> ScoredArticle:
        """Score a single article and return a ScoredArticle."""
        sa = ScoredArticle(
            url=article.get("url", ""),
            title=article.get("title", ""),
            source=article.get("source", ""),
            published_date=article.get("published_date", ""),
            content=article.get("content", ""),
            word_count=article.get("word_count", 0),
        )

        # [BUG FIX] Auto-compute word_count from content if missing
        if sa.word_count <= 0 and sa.content:
            sa.word_count = len(sa.content.split())

        combined_text = f"{sa.title} {sa.content}".strip()

        # Sub-scores
        sa.keyword_score, sa.matched_keywords = self._score_keywords(combined_text)
        sa.geographic_score, sa.matched_geo = self._score_geographic(combined_text)
        sa.recency_score = self._score_recency(sa.published_date)
        sa.quality_score = self._score_quality(article)
        sa.credibility_score = self._score_credibility(sa.source)
        sa.negative_penalty, sa.penalties = self._detect_negative(combined_text)

        # Weighted composite
        w = self.weights
        raw = (
            sa.keyword_score * w.get("keyword_relevance", 0.35)
            + sa.credibility_score * w.get("source_credibility", 0.15)
            + sa.geographic_score * w.get("geographic_relevance", 0.20)
            + sa.recency_score * w.get("recency", 0.15)
            + sa.quality_score * w.get("content_quality", 0.15)
        )

        # Apply negative penalty
        sa.total_score = max(raw - sa.negative_penalty, 0.0)

        # Apply additional penalties
        if sa.word_count < 50:
            sa.total_score = max(sa.total_score + self.penalty_low_wc, 0.0)
            sa.penalties.append("low_word_count")

        if not sa.published_date:
            sa.total_score = max(sa.total_score + self.penalty_no_date, 0.0)
            sa.penalties.append("no_date")

        # Classify
        if sa.total_score >= self.high_threshold:
            sa.relevance_label = "high"
            self._stats["high_relevance"] += 1
        elif sa.total_score >= self.min_score:
            sa.relevance_label = "medium"
            self._stats["medium_relevance"] += 1
        else:
            sa.relevance_label = "low"
            self._stats["low_relevance"] += 1

        self._stats["total_scored"] += 1
        return sa

    def score_batch(self, articles: List[dict]) -> List[ScoredArticle]:
        """Score a batch of articles."""
        results = []
        for art in articles:
            try:
                scored = self.score(art)
                results.append(scored)
            except Exception as e:
                logger.error(f"Scoring failed for '{art.get('url', '?')}': {e}")
                continue
        return results

    def filter_relevant(
        self, articles: List[dict], min_score: float = None
    ) -> List[dict]:
        """Score and filter, returning only relevant articles as dicts."""
        threshold = min_score if min_score is not None else self.min_score
        scored = self.score_batch(articles)
        return [
            sa.to_dict()
            for sa in sorted(scored, key=lambda s: s.total_score, reverse=True)
            if sa.total_score >= threshold
        ]

    def quick_check(self, title: str, text: str, source: str = "",
                    date_str: str = "") -> dict:
        """Quick-check a single article from raw fields."""
        article = {
            "url": "",
            "title": title or "",
            "content": text or "",
            "source": source or "",
            "published_date": date_str or "",
            "word_count": len((text or "").split()),
        }
        scored = self.score(article)
        return scored.to_dict()

    @property
    def stats(self) -> dict:
        return {**self._stats}