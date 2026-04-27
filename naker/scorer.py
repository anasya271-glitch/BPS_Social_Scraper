# ============================================================
# NAKER SENTINEL — Scorer Module
# Path: naker/scorer.py
# Skoring relevansi & pengecekan artikel secara pre-flight sebelum masuk ke tahap audit AI. 
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
    r"\bketenaga[\s\-]?kerjaan", r"\btenaga[\s\-]?kerja", r"\bpengangguran", r"\blowongan[\s\-]?kerja",
    r"\bPHK", r"\bpemutusan[\s\-]?hubungan[\s\-]?kerja", r"\bupah[\s\-]?minimum", r"\bUMK", r"\bUMP", r"\bUMR",
    r"\bburuh", r"\bserikat[\s\-]?pekerja", r"\bBPJS[\s\-]?Ketenagakerjaan", r"\bDisnaker",
    r"\bDinas[\s\-]?Tenaga[\s\-]?Kerja", r"\bTPK", r"\bTingkat[\s\-]?Pengangguran[\s\-]?Terbuka", r"\bTPAK",
    r"\bpadat[\s\-]?karya", r"\bout[\s\-]?sourcing", r"\bkontrak[\s\-]?kerja", r"\bmagang", r"\bpelatihan[\s\-]?kerja",
    r"\bangkatan[\s\-]?kerja", r"\binformal", r"\bBLK", r"\bBalai[\s\-]?Latihan[\s\-]?Kerja",
]

SECONDARY_KEYWORDS = [
    r"\bUMKM", r"\binvestasi", r"\bindustri", r"\bekonomi", r"\binflasi",
    r"\bmanufaktur", r"\bperdagangan", r"\bpariwisata", r"\bdigitalisasi",
    r"\bautomasi", r"\bgig[\s\-]?economy", r"\bBPS", r"\bBadan[\s\-]?Pusat[\s\-]?Statistik",
    r"\bsertifikasi", r"\bpelatihan", r"\bproduktivitas",
]

GEOGRAPHIC_PRIMARY = [
    r"\bBandung\b", r"\bKota[\s\-]?Bandung\b"
]

GEOGRAPHIC_SECONDARY = [
    r"\bJawa[\s\-]?Barat\b", r"\bJabar\b", r"\bMetropolitan[\s\-]?Bandung\b", r"\bBandung[\s\-]?Raya\b",
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

# ============================================================
# SCORING CONFIGURATION
# ============================================================
SCORING_MATRIX = {
    'explicit_kota_bandung': 40,      
    'kecamatan_mention': 30,          
    'bandung_generic': 20,            
    'bandung_with_context': 35,       
    'naker_explicit_3plus': 40,    
    'naker_explicit_2': 30,          
    'naker_explicit_1': 20,     
    'naker_euphemism': 35,           
    'blacklist_strong': -60,       
    'blacklist_weak': -30,            
    'noise_detected': -25,           
    'dual_mention_unclear': -15,      
    'domain_credibility': 10,         
    'title_match': 10,                
}

GEOGRAPHY = {
    "STRICT_ANCHORS": [
        r"\bkota[\s\-]?bandung",
        r"\bpemkot[\s\-]?bandung",
        r"\bwali[\s\-]?kota[\s\-]?bandung",
        r"\bbandung[\s\-]?kota",
        r"\bdprd[\s\-]?kota[\s\-]?bandung",
        r"\bdisnaker[\s\-]?kota[\s\-]?bandung",
        r"\bdisnaker[\s\-]?bandung",
        r"\bpemkot[\s\-]?bdg",
        r"\bkotamadya[\s\-]?bandung",
    ],
    "DISTRICTS": [
        r"\bandir",
        r"\bastana[\s\-]?anyar",
        r"\bantapani",
        r"\barcamanik",
        r"\bbabakan[\s\-]?ciparay",
        r"\bbandung[\s\-]?kidul",
        r"\bbandung[\s\-]?kulon",
        r"\bbandung[\s\-]?wetan",
        r"\bbatununggal",
        r"\bbojongloa[\s\-]?kaler",
        r"\bbojongloa[\s\-]?kidul",
        r"\bbuah[\s\-]?batu",
        r"\bcibeunying",
        r"\bcibiru",
        r"\bcicendo",
        r"\bcidadap",
        r"\bcinambo",
        r"\bcoblong",
        r"\bgede[\s\-]?bage",
        r"\bkiara[\s\-]?condong",
        r"\blengkong",
        r"\bmandalajati",
        r"\bpanyileukan",
        r"\branca[\s\-]?sari",
        r"\bregol",
        r"\bsukajadi",
        r"\bsukasari",
        r"\bsumur[\s\-]?bandung",
        r"\bujung[\s\-]?berung",
    ],
    "LANDMARKS": [
        r"\bjl\.?\s?asia\s?afrika", r"\bjl\.?\s?dago", r"\bjl\.?\s?braga",
        r"\balun[\s\-]?alun", r"\bgedung\s?sate", r"\btrans\s?studio", r"\bgasibu",
    ],
    "BLACKLIST": [
        r"\bkabupaten[\s\-]?bandung", r"\bkab\.?\s?bandung", r"\bsoreang",
        r"\bkbb", r"\bbandung[\s\-]?barat", r"\blembang", r"\bcimahi",
    ],
}

NAKER_POSITIF = [r"\bbuka lowongan", r"\blowongan[\s\-]?kerja", r"\blowongan[\s\-]?pabrik", r"\brekrutmen", r"\brekrutmen[\s\-]?massal", r"\brekrutmen[\s\-]?terbuka", r"\bpenerimaan[\s\-]?karyawan", r"\bpenerimaan[\s\-]?pegawai", r"\bpenerimaan[\s\-]?tenaga[\s\-]?kerja", r"\bterima[\s\-]?karyawan", r"\bjob[\s\-]?fair", r"\bbursa[\s\-]?kerja", r"\bbursa[\s\-]?kerja khusus", r"\bbkk", r"\bexpo[\s\-]?kerja", r"\bpameran[\s\-]?kerja", r"\bhiring", r"\bwe are hiring", r"\bdibutuhkan segera", r"\bbutuh tenaga", r"\bmembuka posisi", r"\bkarir terbuka", r"\bcareer opportunity", r"\bpadat karya", r"\bserap tenaga kerja", r"\bpenyerapan tenaga kerja", r"\bmembuka lapangan kerja", r"\btambah karyawan", r"\bpenambahan pekerja", r"\bpenambahan sdm", r"\bpenambahan tenaga", r"\bpabrik baru", r"\bbuka pabrik", r"\bpabrik ekspansi", r"\bpeningkatan produksi", r"\bpeningkatan kapasitas", r"\bekspansi bisnis", r"\bproyek infrastruktur", r"\binvestasi baru", r"\binvestasi pabrik", r"\bpelatihan kerja", r"\bmagang", r"\bpraktek kerja", r"\bapprentice", r"\bvokasi"]
NAKER_NEGATIF = [r"\bphk", r"\bpemutusan hubungan kerja", r"\bdirumahkan", r"\bpemecatan", r"\bdipecat", r"\brasionalisasi karyawan", r"\brasionalisasi tenaga kerja", r"\brasionalisasi\s+sdm", r"\bright[\s\-]?sizing", r"\bdown[\s\-]?sizing", r"\brestrukturisasi organisasi", r"\brefisiensi\s?sdm", r"\befisiensi\s?sumber\s?daya\s?manusia", r"\bpengurangan tenaga kerja", r"\bpengurangan karyawan", r"\bpenyesuaian struktur", r"\boptimalisasi tenaga kerja", r"\bpemangkasan\s+pegawai", r"\bpemangkasan\s+karyawan", r"\breduksi\s+tenaga\s+kerja", r"\breduksi\s+sdm", r"\btidak diperpanjang kontrak", r"\bkontrak tidak dilanjutkan", r"\bkontrak habis", r"\bkontrak berakhir", r"\bberhenti beroperasi", r"\bpenutupan operasional", r"\bpenghentian produksi", r"\bpenghentian operasi", r"\bsuspend\s?operasi", r"\btutup sementara", r"\bpabrik tutup", r"\btutup pabrik", r"\bgulung tikar", r"\bbangkrut", r"\bpailit", r"\blikuidasi", r"\bpenutupan perusahaan", r"\bpenutupan pabrik", r"\bkurangi karyawan", r"\bkurangi tenaga", r"\bpemotongan\s?pegawai", r"\bpengurangan\s?shift", r"\bpemotongan\s?jam\s?kerja", r"\bpengurangan\s?jam\s?kerja", r"\bgagal panen", r"\bpuso", r"\bomzet turun drastis", r"\bsepi pembeli", r"\bsepi order", r"\bkehilangan kontrak", r"\brelokasi pabrik", r"\bpindah pabrik"]
NAKER_ISU = [r"\bupah minimum", r"\bump", r"\bumk", r"\bumr", r"\bumkm", r"\bgaji", r"\bpeningkatan upah", r"\bkenaikan upah", r"\bupah buruh", r"\bupah pekerja", r"\btunjangan", r"\bthr", r"\btunjangan hari raya", r"\bbpjs ketenagakerjaan", r"\bjamsostek", r"\bpesangon", r"\buang pesangon", r"\bbonus karyawan", r"\bdemo buruh", r"\bunjuk rasa buruh", r"\bpemogokan", r"\baksi mogok", r"\bmogok kerja", r"\bserikat pekerja", r"\bserikat buruh", r"\bout[\s\-]?sourcing", r"\bpekerja kontrak", r"\bpkwt", r"\bpkwtt", r"\bkontrak kerja", r"\bperjanjian kerja", r"\bhak pekerja", r"\bkesejahteraan buruh", r"\bcuti bersama", r"\blembur", r"\bk3", r"\bkeselamatan kerja"]
NOISE_WORDS = [r"\bpersib", r"\bliga", r"\bpilkada", r"\bcpns", r"\bsyarat pendaftaran", r"\bgempa", r"\bkecelakaan", r"\bpembunuhan", r"\bnarkoba", r"\bbencana", r"\bpilkada", r"\bkampanye", r"\bcapres", r"\bcawalkot", r"\bpartai", r"\bkpu", r"\bbawaslu", r"\bkampus", r"\buniversitas", r"\binstitut", r"\bpoliteknik", r"\bmahasiswa", r"\bdosen", r"\brektor", r"\bwisuda", r"\bsnbt", r"\bppdb", r"\bseminar", r"\bwebinar", r"\basn", r"\bcpns", r"\bpppk", r"\bmutasi jabatan", r"\brotasi jabatan", r"\bkinerja asn", r"\bsyarat pendaftaran", r"\bcara melamar", r"\blink pendaftaran", r"\bkirim lamaran"]
TITLE_BLACKLIST = [r"\bvisi", r"\bmisi", r"\btupoksi", r"\bkontak", r"\bjadwal", r"\bindeks", r"\bpegawai", r"\bsejarah", r"\bgallery", r"\bprofil", r"\bbab i", r"\bpower[\s\-]?point", r"\bopen[\s\-]?data", r"\bjurnal", r"\bpengumuman", r"\blayanan", r"\bberanda", r"\bhome", r"\bindex"]
DOCUMENT_EXTENSIONS = [".pdf", ".doc", ".docx", ".xls", ".xlsx"]

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

    keyword_score: float = 0.0
    geographic_score: float = 0.0
    recency_score: float = 0.0
    quality_score: float = 0.0
    credibility_score: float = 0.0
    negative_penalty: float = 0.0

    total_score: float = 0.0
    relevance_label: str = "low"  # low / medium / high

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

    cleaned = re.sub(r"\s*(WIB|WITA|WIT)\s*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.strip()

    cleaned_lower = cleaned.lower()
    for indo_month, eng_month in _INDONESIAN_MONTHS.items():
        if indo_month in cleaned_lower:
            # Case-insensitive replace while preserving surrounding text
            cleaned = re.sub(
                re.escape(indo_month), eng_month, cleaned, flags=re.IGNORECASE
            )
            break

    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue

    iso_match = re.search(r"(\d{4}-\d{2}-\d{2})", cleaned)
    if iso_match:
        try:
            return datetime.strptime(iso_match.group(1), "%Y-%m-%d")
        except ValueError:
            pass

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
        
        # Inisialisasi V66 Smart Patterns (Precision Senses)
        self.v66_patterns = self._compile_v66_patterns()

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

        max_primary = max(len(PRIMARY_KEYWORDS), 1)
        max_secondary = max(len(SECONDARY_KEYWORDS), 1)

        primary_ratio = min(primary_hits / max_primary, 1.0)
        secondary_ratio = min(secondary_hits / max_secondary, 1.0)

        score = (primary_ratio * 0.75) + (secondary_ratio * 0.25)

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
            return 0.3

        now = datetime.now()
        if parsed.tzinfo is not None:
            parsed = parsed.replace(tzinfo=None)

        age_days = (now - parsed).days

        if age_days < 0:
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

        if word_count <= 0 and content:
            word_count = len(content.split())

        score = 0.5

        if word_count >= 500:
            score += 0.3
        elif word_count >= 200:
            score += 0.15
        elif word_count < 100:
            score -= 0.2

        title = article.get("title") or ""
        if len(title) > 20:
            score += 0.1

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

        for pat in _COMPILED_NEGATIVES:
            try:
                if pat.search(text_lower):
                    penalties.append(f"neg_pattern:{pat.pattern}")
                    penalty += 0.15
            except Exception as e:
                logger.debug(f"Regex search error: {e}")
                continue

        for kw in NOISE_WORDS:
            if kw.lower() in text_lower:
                penalties.append(f"noise:{kw}")
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

        if sa.word_count <= 0 and sa.content:
            sa.word_count = len(sa.content.split())

        combined_text = f"{sa.title} {sa.content}".strip()

        sa.keyword_score, sa.matched_keywords = self._score_keywords(combined_text)
        sa.geographic_score, sa.matched_geo = self._score_geographic(combined_text)
        sa.recency_score = self._score_recency(sa.published_date)
        sa.quality_score = self._score_quality(article)
        sa.credibility_score = self._score_credibility(sa.source)
        sa.negative_penalty, sa.penalties = self._detect_negative(combined_text)

        w = self.weights
        raw = (
            sa.keyword_score * w.get("keyword_relevance", 0.35)
            + sa.credibility_score * w.get("source_credibility", 0.15)
            + sa.geographic_score * w.get("geographic_relevance", 0.20)
            + sa.recency_score * w.get("recency", 0.15)
            + sa.quality_score * w.get("content_quality", 0.15)
        )

        sa.total_score = max(raw - sa.negative_penalty, 0.0)

        if sa.word_count < 50:
            sa.total_score = max(sa.total_score + self.penalty_low_wc, 0.0)
            sa.penalties.append("low_word_count")

        if not sa.published_date:
            sa.total_score = max(sa.total_score + self.penalty_no_date, 0.0)
            sa.penalties.append("no_date")

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

    def _compile_v66_patterns(self) -> Dict[str, List[re.Pattern]]:
        """Pre-compile patterns dari V66 untuk performa maksimal."""
        return {
            'strict_anchors': [re.compile(p, re.I) for p in GEOGRAPHY['STRICT_ANCHORS']],
            'districts': [re.compile(p, re.I) for p in GEOGRAPHY['DISTRICTS']],
            'landmarks': [re.compile(p, re.I) for p in GEOGRAPHY['LANDMARKS']],
            'blacklist': [re.compile(p, re.I) for p in GEOGRAPHY['BLACKLIST']],
            'naker_positif': [re.compile(p, re.I) for p in NAKER_POSITIF],
            'naker_negatif': [re.compile(p, re.I) for p in NAKER_NEGATIF],
            'naker_isu': [re.compile(p, re.I) for p in NAKER_ISU],
            'noise': [re.compile(p, re.I) for p in NOISE_WORDS],
        }

    def calculate_v66_score(self, title: str, url: str, text: str = "") -> Tuple[int, Dict[str, str]]:
        """Sistem scoring 0-100 murni dari V66 Precision Sentinel."""
        score = 0
        breakdown = {}
        combined = f"{title} {url} {text}".lower()

        if any(p.search(combined) for p in self.v66_patterns['strict_anchors']):
            score += SCORING_MATRIX['explicit_kota_bandung']
            breakdown['geo'] = "Explicit Kota Bandung (+40)"
        elif any(p.search(combined) for p in self.v66_patterns['districts']):
            score += SCORING_MATRIX['kecamatan_mention']
            breakdown['geo'] = "Kecamatan Bandung (+30)"
        elif any(p.search(combined) for p in self.v66_patterns['landmarks']):
            score += SCORING_MATRIX['bandung_with_context']
            breakdown['geo'] = "Bandung + Landmark (+35)"
        elif "bandung" in combined:
            score += SCORING_MATRIX['bandung_generic']
            breakdown['geo'] = "Generic Bandung (+20)"

        naker_hits = (
            sum(1 for p in self.v66_patterns['naker_positif'] if p.search(combined)) +
            sum(1 for p in self.v66_patterns['naker_negatif'] if p.search(combined)) +
            sum(1 for p in self.v66_patterns['naker_isu'] if p.search(combined))
        )
        if naker_hits >= 3:
            score += SCORING_MATRIX['naker_explicit_3plus']
            breakdown['naker'] = f"Naker Strong (+40)"
        elif naker_hits >= 1:
            score += SCORING_MATRIX['naker_explicit_1']
            breakdown['naker'] = "Naker Weak (+20)"

        if any(p.search(combined) for p in self.v66_patterns['blacklist']):
            score += SCORING_MATRIX['blacklist_strong']
            breakdown['penalty'] = "Out of Jurisdiction (-60)"
            
        return max(0, min(100, score)), breakdown

    def is_rejected_preflight(self, title: str, url: str) -> Tuple[bool, str]:
        """Audit cepat sebelum mengunduh seluruh isi artikel (BPS V66 Standard)."""
        title_lower = str(title or "").lower()
        url_lower = str(url or "").lower()
        
        if any(url_lower.endswith(ext) for ext in DOCUMENT_EXTENSIONS):
            return True, "Ekstensi Dokumen Non-Naratif"
        
        expanded_blacklist = TITLE_BLACKLIST + [
            r"\bkereta", r"\btiket", r"\bkursi", r"\bkriminal", r"\bkecelakaan", 
            r"\bpembunuhan", r"\bnapi", r"\bpencabulan", r"\bbansos"
        ]
        if any(b in title_lower for b in expanded_blacklist):
            return True, "Halaman Statis/Irrelevant"
        
        quick_score = 0
        combined = f"{title_lower} {url_lower}"
        
        def check_smart_keywords(kws):
            for kw in kws:
                if not isinstance(kw, str): continue
                smart_kw = kw.replace(" ", r"[\s\-]?")
                try:
                    if re.search(smart_kw, combined):
                        return True
                except re.error as e:
                    pass
            return False
            
        expanded_primary = PRIMARY_KEYWORDS + [
            r"\bloker", r"\bbursa kerja", r"\brekrutmen", r"\bjob[\s\-]?fair", r"\blowongan", r"\bphk", 
            r"\bpemutusan hubungan kerja", r"\bdirumahkan", r"\bpengangguran",
            r"\bgaji", r"\bupah", r"\bhonorer", r"\bpegawai", r"\bdikontrak", r"\bumk", r"\bumr",
            r"\bserikat pekerja", r"\bbpjs ketenagakerjaan", r"\bdisnaker", r"\btka", r"\bpkwt",
            r"\bmagang"
        ]
        
        if check_smart_keywords(expanded_primary):
            quick_score += 20
            
        if check_smart_keywords(SECONDARY_KEYWORDS):
            quick_score += 10
            
        if quick_score < 10:
            return True, f"Relevance Score Terlalu Rendah ({quick_score}/100)"
            
        return False, ""

    @property
    def stats(self) -> dict:
        return {**self._stats}