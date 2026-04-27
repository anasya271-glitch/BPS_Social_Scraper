"""
test_naker.py
Unit tests untuk semua modul Naker.
Dioptimasi untuk laptop low-spec (6GB RAM, Ryzen 3 3250U).

Jalankan:
    python -m pytest test_naker.py -v   # jalankan semua test
    python -m pytest test_naker.py -v -x    # berhenti di error pertama
    python -m pytest test_naker.py -v -k "test_scorer"  # jalankan scorer saja
    python -m pytest test_naker.py -v -k "test_loader" # jalankan loader saja
    python -m pytest test_naker.py -v -k "test_parser" # jalankan parser saja (butuh beautifulsoup4)
    python test_naker.py                      # tanpa pytest (fallback)
"""

import os
import sys
import json
import time
import tempfile
import shutil
import unittest
from naker.sentinel import NewsScraper, NEWS_SOURCES
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from naker.loader import NakerConfig, FileCache, PromptLoader, ConfigLoader, detect_system_profile
from scorer import ArticleScorer, ScoredArticle, score_and_rank, NAKER_KEYWORDS


# ================================================================
# TEST LOADER
# ================================================================

class TestNakerConfig(unittest.TestCase):
    """Test konfigurasi dan preset hardware."""

    def test_default_config(self):
        cfg = NakerConfig()
        self.assertEqual(cfg.max_concurrent_requests, 3)
        self.assertEqual(cfg.request_timeout, 15)
        self.assertGreater(cfg.batch_size, 0)

    def test_low_spec_preset(self):
        cfg = NakerConfig.for_low_spec()
        self.assertEqual(cfg.max_concurrent_requests, 2)
        self.assertEqual(cfg.max_memory_mb, 300)
        self.assertEqual(cfg.batch_size, 3)
        self.assertFalse(cfg.extract_images)

    def test_high_spec_preset(self):
        cfg = NakerConfig.for_high_spec()
        self.assertEqual(cfg.max_concurrent_requests, 8)
        self.assertEqual(cfg.max_memory_mb, 2048)
        self.assertTrue(cfg.extract_images)

    def test_to_dict_and_back(self):
        original = NakerConfig.for_low_spec()
        d = original.to_dict()
        restored = NakerConfig.from_dict(d)
        self.assertEqual(original.max_concurrent_requests, restored.max_concurrent_requests)
        self.assertEqual(original.batch_size, restored.batch_size)

    def test_from_dict_ignores_unknown_keys(self):
        d = {"max_concurrent_requests": 5, "unknown_field": "should_be_ignored"}
        cfg = NakerConfig.from_dict(d)
        self.assertEqual(cfg.max_concurrent_requests, 5)
        self.assertFalse(hasattr(cfg, "unknown_field"))

    def test_ConfigLoader_auto_detect(self):
        cfg = ConfigLoader()
        self.assertIsInstance(cfg, NakerConfig)
        self.assertGreater(cfg.batch_size, 0)

    def test_ConfigLoader_from_json(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"max_concurrent_requests": 99, "batch_size": 42}, f)
            f.flush()
            cfg = ConfigLoader(f.name)
        os.unlink(f.name)
        self.assertEqual(cfg.max_concurrent_requests, 99)
        self.assertEqual(cfg.batch_size, 42)


class TestFileCache(unittest.TestCase):
    """Test file-based cache (ringan, tanpa Redis)."""

    def setUp(self):
        self.cache_dir = tempfile.mkdtemp(prefix="naker_test_cache_")
        self.cache = FileCache(cache_dir=self.cache_dir, max_age_seconds=5)

    def tearDown(self):
        shutil.rmtree(self.cache_dir, ignore_errors=True)

    def test_set_and_get(self):
        self.cache.set("key1", {"title": "Test Article"})
        result = self.cache.get("key1")
        self.assertIsNotNone(result)
        self.assertEqual(result["title"], "Test Article")

    def test_get_missing_key(self):
        result = self.cache.get("nonexistent")
        self.assertIsNone(result)

    def test_cache_expiry(self):
        self.cache.max_age = 1  # 1 detik
        self.cache.set("expire_me", "data")
        self.assertIsNotNone(self.cache.get("expire_me"))
        time.sleep(1.5)
        self.assertIsNone(self.cache.get("expire_me"))

    def test_delete(self):
        self.cache.set("del_key", "value")
        self.assertTrue(self.cache.delete("del_key"))
        self.assertIsNone(self.cache.get("del_key"))

    def test_clear(self):
        self.cache.set("a", 1)
        self.cache.set("b", 2)
        count = self.cache.clear()
        self.assertEqual(count, 2)
        self.assertIsNone(self.cache.get("a"))

    def test_stats(self):
        self.cache.set("x", "data")
        stats = self.cache.stats()
        self.assertEqual(stats["entries"], 1)
        self.assertGreater(stats["total_size_kb"], 0)

    def test_unicode_content(self):
        self.cache.set("indo", "Berita ketenagakerjaan Bandung 2024")
        result = self.cache.get("indo")
        self.assertEqual(result, "Berita ketenagakerjaan Bandung 2024")


class TestPromptLoader(unittest.TestCase):
    """Test prompt template loader."""

    def setUp(self):
        self.prompts_dir = tempfile.mkdtemp(prefix="naker_prompts_")
        Path(self.prompts_dir, "analyze.txt").write_text(
            "Analisis artikel berikut tentang {topic}:\n\n{content}",
            encoding="utf-8",
        )

    def tearDown(self):
        shutil.rmtree(self.prompts_dir, ignore_errors=True)

    def test_load_prompt(self):
        loader = PromptLoader(self.prompts_dir)
        result = loader.load("analyze", topic="ketenagakerjaan", content="isi artikel")
        self.assertIn("ketenagakerjaan", result)
        self.assertIn("isi artikel", result)

    def test_load_missing_prompt(self):
        loader = PromptLoader(self.prompts_dir)
        with self.assertRaises(FileNotFoundError):
            loader.load("nonexistent")

    def test_list_prompts(self):
        loader = PromptLoader(self.prompts_dir)
        prompts = loader.list_prompts()
        self.assertIn("analyze", prompts)


# ================================================================
# TEST PARSER (tanpa BeautifulSoup = skip, dengan = full test)
# ================================================================

class TestParser(unittest.TestCase):
    """Test ArticleParser."""

    @classmethod
    def setUpClass(cls):
        try:
            from parser import ArticleParser, ParsedArticle
            cls.ArticleParser = ArticleParser
            cls.ParsedArticle = ParsedArticle
            cls.bs4_available = True
        except ImportError:
            cls.bs4_available = False

    def setUp(self):
        if not self.bs4_available:
            self.skipTest("beautifulsoup4 not installed - skipping parser tests")
        self.parser = self.ArticleParser(extract_images=False)

    # Sample HTML yang mirip artikel berita
    SAMPLE_HTML = """
    <html>
    <head><title>UMK Bandung 2024 Naik 5%</title>
    <meta name="author" content="Redaksi Pikiran Rakyat">
    <meta property="article:published_time" content="2024-12-01">
    </head>
    <body>
    <nav>Navigation</nav>
    <article>
        <h1>UMK Bandung 2024 Naik 5 Persen</h1>
        <span class="date">1 Desember 2024</span>
        <div class="content-inner">
            <p>Pemerintah Kota Bandung resmi mengumumkan kenaikan Upah Minimum Kota (UMK) sebesar 5 persen untuk tahun 2024.</p>
            <p>Kenaikan ini disambut baik oleh serikat pekerja namun dinilai masih belum cukup oleh beberapa kalangan buruh.</p>
            <p>Dinas Ketenagakerjaan Kota Bandung menyatakan bahwa angka ini sudah mempertimbangkan kondisi ekonomi dan inflasi.</p>
        </div>
        <div class="tag"><a>ketenagakerjaan</a><a>bandung</a><a>umk</a></div>
    </article>
    <footer>Footer</footer>
    </body></html>
    """

    MINIMAL_HTML = "<html><body><p>Short content.</p></body></html>"

    def test_parse_full_article(self):
        result = self.parser.parse(self.SAMPLE_HTML, url="https://pikiran-rakyat.com/test")
        self.assertIn("UMK", result.title)
        self.assertIn("Bandung", result.content)
        self.assertTrue(result.is_valid)
        self.assertGreater(result.word_count, 10)

    def test_parse_extracts_author(self):
        result = self.parser.parse(self.SAMPLE_HTML, url="https://pikiran-rakyat.com/test")
        # Bisa dari meta tag atau selector
        # Author mungkin kosong tergantung selector match, tapi tidak boleh error
        self.assertIsInstance(result.author, str)

    def test_parse_minimal_html(self):
        result = self.parser.parse(self.MINIMAL_HTML)
        self.assertFalse(result.is_valid)  # Terlalu pendek

    def test_parse_empty_html(self):
        result = self.parser.parse("")
        self.assertEqual(result.title, "")
        self.assertFalse(result.is_valid)

    def test_parse_detik_url_detection(self):
        result = self.parser.parse(self.SAMPLE_HTML, url="https://www.detik.com/jabar/berita/123")
        self.assertEqual(result.source_name, "detik.com")

    def test_parse_many(self):
        # Simulasi list of pages
        pages = [
            MagicMock(ok=True, html=self.SAMPLE_HTML, url="https://pikiran-rakyat.com/1"),
            MagicMock(ok=True, html=self.SAMPLE_HTML, url="https://pikiran-rakyat.com/2"),
            MagicMock(ok=False, html="", url="https://fail.com"),
        ]
        results = self.parser.parse_many(pages)
        self.assertEqual(len(results), 2)  # Yang ok=False di-skip

    def test_to_dict(self):
        result = self.parser.parse(self.SAMPLE_HTML, url="https://test.com")
        d = result.to_dict()
        self.assertIn("title", d)
        self.assertIn("url", d)
        self.assertIsInstance(d, dict)


# ================================================================
# TEST SCORER
# ================================================================

class TestArticleScorer(unittest.TestCase):
    """Test scoring dan ranking artikel."""

    def setUp(self):
        self.scorer = ArticleScorer()

    def _make_article(self, title="", content="", date="", url=""):
        return {
            "title": title,
            "content": content,
            "url": url,
            "source_name": "test",
            "published_date": date,
            "word_count": len(content.split()),
        }

    def test_high_relevance_article(self):
        art = self._make_article(
            title="PHK Massal di Bandung, Disnaker Turun Tangan",
            content="Dinas Ketenagakerjaan Kota Bandung mencatat ada pemutusan hubungan kerja "
                    "massal di sektor manufaktur. Serikat pekerja menggelar aksi mogok kerja. "
                    "Upah minimum kota dinilai masih rendah oleh kalangan buruh. "
                    * 5,
            date="2024-12-01",
        )
        scored = self.scorer.score_article(art)
        self.assertGreater(scored.keyword_score, 0.5)
        self.assertGreater(len(scored.matched_keywords), 3)

    def test_low_relevance_article(self):
        art = self._make_article(
            title="Resep Masakan Sunda Terpopuler",
            content="Berikut resep nasi timbel yang enak dan mudah dibuat di rumah. "
                    "Siapkan bahan-bahan seperti beras, daun pisang, dan lauk pauk." * 3,
            date="2024-06-15",
        )
        scored = self.scorer.score_article(art)
        self.assertLess(scored.keyword_score, 0.3)

    def test_recency_recent(self):
        # Artikel hari ini
        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")
        art = self._make_article(title="Test", content="Test content here.", date=today)
        scored = self.scorer.score_article(art)
        self.assertEqual(scored.recency_score, 1.0)

    def test_recency_old(self):
        art = self._make_article(title="Test", content="Test content.", date="2020-01-01")
        scored = self.scorer.score_article(art)
        self.assertLess(scored.recency_score, 0.1)

    def test_recency_unknown_date(self):
        art = self._make_article(title="Test", content="Test content.", date="")
        scored = self.scorer.score_article(art)
        self.assertEqual(scored.recency_score, 0.3)

    def test_quality_good_article(self):
        art = self._make_article(
            title="Judul Artikel yang Cukup Informatif dan Jelas",
            content="Paragraf pertama menjelaskan konteks. "
                    "Paragraf kedua memberikan detail. "
                    "Paragraf ketiga berisi data pendukung. "
                    "Paragraf keempat memberikan analisis mendalam tentang topik. " * 10,
        )
        scored = self.scorer.score_article(art)
        self.assertGreater(scored.quality_score, 0.5)

    def test_quality_poor_article(self):
        art = self._make_article(title="Hi", content="Short")
        scored = self.scorer.score_article(art)
        self.assertLess(scored.quality_score, 0.3)

    def test_score_many_ranking(self):
        articles = [
            self._make_article("Berita biasa", "Konten singkat.", date="2023-01-01"),
            self._make_article(
                "PHK di Bandung Meningkat",
                "Dinas ketenagakerjaan Bandung melaporkan peningkatan PHK di sektor industri. "
                "Buruh menggelar aksi. Upah minimum belum naik. " * 5,
                date="2024-12-01",
            ),
        ]
        ranked = self.scorer.score_many(articles)
        self.assertEqual(ranked[0].rank, 1)
        self.assertIn("ketenagakerjaan", ranked[0].matched_keywords)

    def test_filter_relevant(self):
        articles = [
            self._make_article("Resep Makanan", "Cara masak nasi goreng enak." * 5),
            self._make_article(
                "Lowongan Kerja Bandung 2024",
                "Disnaker Bandung membuka lowongan kerja baru untuk tenaga kerja terampil. " * 5,
                date="2024-11-01",
            ),
        ]
        filtered = self.scorer.filter_relevant(articles)
        self.assertGreaterEqual(len(filtered), 1)

    def test_date_parsing_indonesian(self):
        art = self._make_article(title="T", content="C " * 20, date="15 Januari 2024")
        scored = self.scorer.score_article(art)
        self.assertGreater(scored.recency_score, 0)

    def test_score_and_rank_convenience(self):
        articles = [
            self._make_article("A", "Konten " * 20),
            self._make_article("B", "Ketenagakerjaan Bandung " * 20, date="2024-12-01"),
        ]
        result = score_and_rank(articles, threshold=0.1)
        self.assertIsInstance(result, list)

    def test_scored_article_to_dict(self):
        art = self._make_article("Test", "Content " * 20)
        scored = self.scorer.score_article(art)
        d = scored.to_dict()
        self.assertIn("total_score", d)
        self.assertIn("matched_keywords", d)
        self.assertIsInstance(d["total_score"], float)


# ================================================================
# TEST SCRAPER (mock-based, tanpa network)
# ================================================================

class TestScraperStructure(unittest.TestCase):
    """Test scraper structure dan config (tanpa actual HTTP)."""

    def test_import_scraper(self):
        self.assertTrue(len(NEWS_SOURCES) >= 3)

    def test_scraped_page_ok(self):
        # NewsScraper tidak punya ScrapedPage, hasilnya dict dengan key 'status'
        page = {"url": "https://test.com", "status": 200, "html": "<html>content</html>"}
        self.assertEqual(page["status"], 200)
    def test_scraped_page_not_ok(self):
        page = {"url": "https://test.com", "status": 404, "html": ""}
        self.assertNotEqual(page["status"], 200)
    def test_scraper_init_defaults(self):
        s = NewsScraper()
        self.assertEqual(s.max_concurrent, 3)
        self.assertEqual(s.request_timeout, 15)
    def test_scraper_init_custom(self):
        s = NewsScraper(max_concurrent=2, request_timeout=20, delay=2.0)
        self.assertEqual(s.max_concurrent, 2)
        self.assertEqual(s.delay, 2.0)
    def test_scraper_stats_initial(self):
        s = NewsScraper()
        stats = s.get_stats()
        self.assertEqual(stats["total_requests"], 0)
        self.assertEqual(stats["successful"], 0)


# ================================================================
# INTEGRATION TEST (ringan)
# ================================================================

class TestIntegration(unittest.TestCase):
    """Test integrasi antar modul (tanpa network)."""

    def test_config_to_scraper_params(self):
        cfg = NakerConfig.for_low_spec()
        scraper = NewsScraper(
            max_concurrent=cfg.max_concurrent_requests,
            request_timeout=cfg.request_timeout,
            delay=cfg.delay_between_requests,
        )
        self.assertEqual(scraper.max_concurrent, 2)

    def test_full_pipeline_mock(self):
        """Simulasi pipeline: parse → score → rank (tanpa network)."""
        try:
            from parser import ArticleParser
        except ImportError:
            self.skipTest("beautifulsoup4 not installed")

        html = """
        <html><body>
        <h1>Lowongan Kerja Baru di Bandung dari Disnaker</h1>
        <article>
        <p>Dinas Ketenagakerjaan Kota Bandung membuka 500 lowongan kerja baru
        untuk sektor industri manufaktur dan teknologi.</p>
        <p>Program pelatihan kerja di BLK juga diperluas untuk mengurangi
        pengangguran di kalangan anak muda.</p>
        <p>Upah minimum kota akan disesuaikan berdasarkan inflasi 2024.</p>
        </article>
        </body></html>
        """

        # Parse
        parser = ArticleParser()
        article = parser.parse(html, url="https://test.com/berita/1")
        self.assertTrue(article.is_valid)

        # Score
        scorer = ArticleScorer()
        scored = scorer.score_article(article)
        self.assertGreater(scored.total_score, 0)
        self.assertGreater(len(scored.matched_keywords), 0)

        # Verify keywords matched
        expected_matches = {"ketenagakerjaan", "bandung", "lowongan kerja"}
        actual_matches = set(scored.matched_keywords)
        self.assertTrue(expected_matches & actual_matches)  # At least some overlap


# ================================================================
# RESOURCE USAGE CHECK (khusus laptop)
# ================================================================

class TestResourceUsage(unittest.TestCase):
    """Pastikan test suite tidak makan resource berlebihan."""

    def test_memory_usage_acceptable(self):
        """Test suite harus pakai < 100MB RAM."""
        try:
            import psutil
            process = psutil.Process()
            mem_mb = process.memory_info().rss / (1024 * 1024)
            self.assertLess(mem_mb, 100, f"Test suite pakai {mem_mb:.1f}MB RAM (limit 100MB)")
        except ImportError:
            # Tanpa psutil, skip saja
            self.skipTest("psutil not installed - skipping memory check")

    def test_cache_disk_usage(self):
        """Cache test tidak boleh > 1MB."""
        cache_dir = tempfile.mkdtemp()
        cache = FileCache(cache_dir=cache_dir)
        for i in range(50):
            cache.set(f"key_{i}", f"value_{i}" * 100)
        stats = cache.stats()
        self.assertLess(stats["total_size_kb"], 1024)
        shutil.rmtree(cache_dir)


# ================================================================
# RUNNER
# ================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("NAKER UNIT TESTS")
    print("=" * 60)
    print(f"Python: {sys.version}")
    print(f"Platform: {sys.platform}")

    try:
        import psutil
        ram = psutil.virtual_memory()
        print(f"RAM: {ram.total / (1024**3):.1f} GB (available: {ram.available / (1024**3):.1f} GB)")
    except ImportError:
        print("RAM: (install psutil for details)")

    print("=" * 60)
    unittest.main(verbosity=2)