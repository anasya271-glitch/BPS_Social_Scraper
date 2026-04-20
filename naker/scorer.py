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

# ============================================================
# V66 SMART SCORING CONFIGURATION (BPS-NAKER V66)
# ============================================================
SCORING_MATRIX = {
    'explicit_kota_bandung': 40,      # "Kota Bandung", "Pemkot Bandung"
    'kecamatan_mention': 30,          # Cicendo, Coblong, etc
    'bandung_generic': 20,            # Just "Bandung"
    'bandung_with_context': 35,       # "Bandung" + street/landmark
    'naker_explicit_3plus': 40,       # >=3 keywords
    'naker_explicit_2': 30,           # 2 keywords
    'naker_explicit_1': 20,           # 1 keyword
    'naker_euphemism': 35,            # Indirect terms
    'blacklist_strong': -60,          # "Kabupaten Bandung" + blacklist
    'blacklist_weak': -30,            # Province mention
    'noise_detected': -25,            # Spam/academic
    'dual_mention_unclear': -15,      # Kota+Kab both mentioned ambiguously
    'domain_credibility': 10,         # Kompas, Tempo, govt
    'title_match': 10,                # Title has geo+naker
}

GEOGRAPHY = {
    "STRICT_ANCHORS": [
        r"\bkota[\s\-]?bandung\b",
        r"\bpemkot[\s\-]?bandung\b",
        r"\bwali[\s\-]?kota[\s\-]?bandung\b",
        r"\bbandung[\s\-]?kota\b",
        r"\bdprd[\s\-]?kota[\s\-]?bandung\b",
        r"\bdisnaker[\s\-]?kota[\s\-]?bandung\b",
        r"\bdisnaker[\s\-]?bandung\b",
        r"\bpemkot[\s\-]?bdg\b",
        r"\bkotamadya[\s\-]?bandung\b",
    ],
    "DISTRICTS": [
        r"\bandir\b",
        r"\bastana[\s\-]?anyar\b",
        r"\bantapani\b",
        r"\barcamanik\b",
        r"\bbabakan[\s\-]?ciparay\b",
        r"\bbandung[\s\-]?kidul\b",
        r"\bbandung[\s\-]?kulon\b",
        r"\bbandung[\s\-]?wetan\b",
        r"\bbatununggal\b",
        r"\bbojongloa[\s\-]?kaler\b",
        r"\bbojongloa[\s\-]?kidul\b",
        r"\bbuah[\s\-]?batu\b",
        r"\bcibeunying\b",
        r"\bcibiru\b",
        r"\bcicendo\b",
        r"\bcidadap\b",
        r"\bcinambo\b",
        r"\bcoblong\b",
        r"\bgedebage\b",
        r"\bkiara[\s\-]?condong\b",
        r"\blengkong\b",
        r"\bmandalajati\b",
        r"\bpanyileukan\b",
        r"\brancasari\b",
        r"\bregol\b",
        r"\bsukajadi\b",
        r"\bsukasari\b",
        r"\bsumur[\s\-]?bandung\b",
        r"\bujung[\s\-]?berung\b",
    ],
    "LANDMARKS": [
        r"\bjl\.?\s?asia\s?afrika\b", r"\bjl\.?\s?dago\b", r"\bjl\.?\s?braga\b",
        r"\balun[\s\-]?alun\b", r"\bgedung\s?sate\b", r"\btrans\s?studio\b", r"\bgasibu\b",
    ],
    "BLACKLIST": [
        r"\bkabupaten[\s\-]?bandung\b", r"\bkab\.?\s?bandung\b", r"\bsoreang\b",
        r"\bkbb\b", r"\bbandung[\s\-]?barat\b", r"\blembang\b", r"\bcimahi\b",
    ],
}

NAKER_POSITIF = [r"\bbuka lowongan\b", r"\blowongan kerja\b", r"\blowongan pabrik\b", r"\brekrutmen\b", r"\brekrutmen massal\b", r"\brekrutmen terbuka\b", r"\bpenerimaan karyawan\b", r"\bpenerimaan pegawai\b", r"\bpenerimaan tenaga kerja\b", r"\bterima karyawan\b", r"\bjob fair\b", r"\bbursa kerja\b", r"\bbursa kerja khusus\b", r"\bbkk\b", r"\bexpo kerja\b", r"\bpameran kerja\b", r"\bhiring\b", r"\bwe are hiring\b", r"\bdibutuhkan segera\b", r"\bbutuh tenaga\b", r"\bmembuka posisi\b", r"\bkarir terbuka\b", r"\bcareer opportunity\b", r"\bpadat karya\b", r"\bserap tenaga kerja\b", r"\bpenyerapan tenaga kerja\b", r"\bmembuka lapangan kerja\b", r"\btambah karyawan\b", r"\bpenambahan pekerja\b", r"\bpenambahan sdm\b", r"\bpenambahan tenaga\b", r"\bpabrik baru\b", r"\bbuka pabrik\b", r"\bpabrik ekspansi\b", r"\bpeningkatan produksi\b", r"\bpeningkatan kapasitas\b", r"\bekspansi bisnis\b", r"\bproyek infrastruktur\b", r"\binvestasi baru\b", r"\binvestasi pabrik\b", r"\bpelatihan kerja\b", r"\bmagang\b", r"\bpraktek kerja\b", r"\bapprentice\b", r"\bvokasi\b"]
NAKER_NEGATIF = [r"\bphk\b", r"\bpemutusan hubungan kerja\b", r"\bdirumahkan\b", r"\bpemecatan\b", r"\bdipecat\b", r"\brasionalisasi karyawan\b", r"\brasionalisasi tenaga kerja\b", r"\brasionalisasi\s+sdm\b", r"\bright[\s\-]?sizing\b", r"\bdown[\s\-]?sizing\b", r"\brestrukturisasi organisasi\b", r"\brefisiensi\s?sdm\b", r"\befisiensi\s?sumber\s?daya\s?manusia\b", r"\bpengurangan tenaga kerja\b", r"\bpengurangan karyawan\b", r"\bpenyesuaian struktur\b", r"\boptimalisasi tenaga kerja\b", r"\bpemangkasan\s+pegawai\b", r"\bpemangkasan\s+karyawan\b", r"\breduksi\s+tenaga\s+kerja\b", r"\breduksi\s+sdm\b", r"\btidak diperpanjang kontrak\b", r"\bkontrak tidak dilanjutkan\b", r"\bkontrak habis\b", r"\bkontrak berakhir\b", r"\bberhenti beroperasi\b", r"\bpenutupan operasional\b", r"\bpenghentian produksi\b", r"\bpenghentian operasi\b", r"\bsuspend\s?operasi\b", r"\btutup sementara\b", r"\bpabrik tutup\b", r"\btutup pabrik\b", r"\bgulung tikar\b", r"\bbangkrut\b", r"\bpailit\b", r"\blikuidasi\b", r"\bpenutupan perusahaan\b", r"\bpenutupan pabrik\b", r"\bkurangi karyawan\b", r"\bkurangi tenaga\b", r"\bpemotongan\s?pegawai\b", r"\bpengurangan\s?shift\b", r"\bpemotongan\s?jam\s?kerja\b", r"\bpengurangan\s?jam\s?kerja\b", r"\bgagal panen\b", r"\bpuso\b", r"\bomzet turun drastis\b", r"\bsepi pembeli\b", r"\bsepi order\b", r"\bkehilangan kontrak\b", r"\brelokasi pabrik\b", r"\bpindah pabrik\b"]
NAKER_ISU = [r"\bupah minimum\b", r"\bump\b", r"\bumk\b", r"\bumr\b", r"\bumkm\b", r"\bgaji\b", r"\bpeningkatan upah\b", r"\bkenaikan upah\b", r"\bupah buruh\b", r"\bupah pekerja\b", r"\btunjangan\b", r"\bthr\b", r"\btunjangan hari raya\b", r"\bbpjs ketenagakerjaan\b", r"\bjamsostek\b", r"\bpesangon\b", r"\buang pesangon\b", r"\bbonus karyawan\b", r"\bdemo buruh\b", r"\bunjuk rasa buruh\b", r"\bpemogokan\b", r"\baksi mogok\b", r"\bmogok kerja\b", r"\bserikat pekerja\b", r"\bserikat buruh\b", r"\boutsourcing\b", r"\bpekerja kontrak\b", r"\bpkwt\b", r"\bpkwtt\b", r"\bkontrak kerja\b", r"\bperjanjian kerja\b", r"\bhak pekerja\b", r"\bkesejahteraan buruh\b", r"\bcuti bersama\b", r"\blembur\b", r"\bk3\b", r"\bkeselamatan kerja\b"]
NOISE_WORDS = [r"\bpersib\b", r"\bliga\b", r"\bpilkada\b", r"\bcpns\b", r"\bsyarat pendaftaran\b", r"\bgempa\b", r"\bkecelakaan\b", r"\bpembunuhan\b", r"\bnarkoba\b", r"\bbencana\b", r"\bpilkada\b", r"\bkampanye\b", r"\bcapres\b", r"\bcawalkot\b", r"\bpartai\b", r"\bkpu\b", r"\bbawaslu\b", r"\bkampus\b", r"\buniversitas\b", r"\binstitut\b", r"\bpoliteknik\b", r"\bmahasiswa\b", r"\bdosen\b", r"\brektor\b", r"\bwisuda\b", r"\bsnbt\b", r"\bppdb\b", r"\bseminar\b", r"\bwebinar\b", r"\basn\b", r"\bcpns\b", r"\bpppk\b", r"\bmutasi jabatan\b", r"\brotasi jabatan\b", r"\bkinerja asn\b", r"\bsyarat pendaftaran\b", r"\bcara melamar\b", r"\blink pendaftaran\b", r"\bkirim lamaran\b"]
TITLE_BLACKLIST = ["visi", "misi", "tupoksi", "kontak", "jadwal", "indeks", "pegawai", "sejarah", "gallery", "profil", "bab i", "powerpoint", "open data", "jurnal", "pengumuman", "layanan", "beranda", "home", "index"]
DOCUMENT_EXTENSIONS = [".pdf", ".doc", ".docx", ".xls", ".xlsx"]

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

        # Check noise keywords
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

    # ============================================================
    # V66 SMART LOGIC — Pindahan dari src/naker_scraper.py
    # ============================================================
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

        # GEOGRAPHIC SCORING (Max 40)
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

        # NAKER SCORING (Max 40)
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

        # PENALTIES
        if any(p.search(combined) for p in self.v66_patterns['blacklist']):
            score += SCORING_MATRIX['blacklist_strong']
            breakdown['penalty'] = "Out of Jurisdiction (-60)"
            
        return max(0, min(100, score)), breakdown

    def is_rejected_preflight(self, title: str, url: str) -> Tuple[bool, str]:
        """Audit cepat sebelum mengunduh seluruh isi artikel (BPS V66 Standard)."""
        title_lower = title.lower()
        url_lower = url.lower()
        
        # 1. Cek Ekstensi Dokumen
        if any(url_lower.endswith(ext) for ext in DOCUMENT_EXTENSIONS):
            return True, "Ekstensi Dokumen Non-Naratif"
        
        # 2. Cek Title Blacklist
        if any(b in title_lower for b in TITLE_BLACKLIST):
            return True, "Halaman Statis/Administratif"
        
        # 3. Quick Score Check (Hanya Judul & URL)
        score, _ = self.calculate_v66_score(title, url)
        if score < 30:
            return True, f"Relevance Score Terlalu Rendah ({score}/100)"
        
        return False, f"Pass Pre-flight (Score: {score}/100)"

    @property
    def stats(self) -> dict:
        return {**self._stats}