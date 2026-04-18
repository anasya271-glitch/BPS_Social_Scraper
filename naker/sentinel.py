"""
sentinel.py - Orchestration pipeline for NAKER SENTINEL.
Class-based design with staged pipeline: scrape → parse → score → filter → interrogate → save.
"""

import asyncio
import argparse
import logging
import yaml
import sys
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from pathlib import Path
from collections import Counter

from bandung_scraper import BandungScraper, SEARCH_QUERIES
from parser import extract_article_content, parse_date_robust
from scorer import RelevanceScorer
from manager import ArticleManager

logger = logging.getLogger("sentinel")

DEFAULT_CONFIG_PATH = "config.yaml"


class NakerSentinel:
    """
    Main orchestrator for the NAKER SENTINEL pipeline.
    Stages: scrape → parse → score → filter → interrogate → save → summary.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize all pipeline components from config.

        Args:
            config: Full configuration dict (typically loaded from YAML).
        """
        self.config = config
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None

        # Setup logging first
        self._setup_logging()

        # Initialize components
        self.scraper = BandungScraper(config=config.get("scraper", {}))
        self.scorer = RelevanceScorer(config.get("scorer", {}))
        self.manager = ArticleManager(config.get("manager", {}))

        # Pipeline state
        self.raw_articles: List[Dict[str, Any]] = []
        self.parsed_articles: List[Dict[str, Any]] = []
        self.scored_articles: List[Dict[str, Any]] = []
        self.filtered_articles: List[Dict[str, Any]] = []
        self.interrogated_articles: List[Dict[str, Any]] = []

        # Stats tracking
        self.stats: Dict[str, Any] = {
            "stage_scrape": {},
            "stage_parse": {},
            "stage_score": {},
            "stage_filter": {},
            "stage_interrogate": {},
            "stage_save": {},
        }

        logger.info("NakerSentinel initialized.")

    def _setup_logging(self):
        """Configure logging based on config settings."""
        log_cfg = self.config.get("logging", {})
        level_str = log_cfg.get("level", "INFO").upper()
        level = getattr(logging, level_str, logging.INFO)
        log_format = log_cfg.get("format", "%(asctime)s [%(levelname)s] %(name)s: %(message)s")

        # Root logger
        root = logging.getLogger()
        root.setLevel(level)

        # Console handler
        if not root.handlers:
            ch = logging.StreamHandler(sys.stdout)
            ch.setLevel(level)
            ch.setFormatter(logging.Formatter(log_format))
            root.addHandler(ch)

        # File handler (optional)
        log_file = log_cfg.get("file")
        if log_file:
            try:
                fh = logging.FileHandler(log_file, encoding="utf-8")
                fh.setLevel(level)
                fh.setFormatter(logging.Formatter(log_format))
                root.addHandler(fh)
                logger.debug(f"Logging to file: {log_file}")
            except OSError as e:
                logger.warning(f"Could not open log file '{log_file}': {e}")

    async def stage_scrape(self) -> List[Dict[str, Any]]:
        """Stage 1: Discover and fetch articles from all sources."""
        logger.info("=" * 50)
        logger.info("STAGE 1: SCRAPE — Discovering and fetching articles")
        logger.info("=" * 50)

        t0 = datetime.now(timezone.utc)

        # Discover article URLs
        discovered = await self.scraper.discover_articles()
        logger.info(f"Discovered {len(discovered)} unique article URLs.")

        if not discovered:
            logger.warning("No articles discovered. Pipeline may produce empty results.")
            self.stats["stage_scrape"] = {"discovered": 0, "fetched": 0, "success": 0, "duration_s": 0}
            return []

        # Fetch full HTML for each article
        fetched = await self.scraper.fetch_all_articles(discovered)
        successful = [a for a in fetched if a.get("fetch_success")]

        duration = (datetime.now(timezone.utc) - t0).total_seconds()
        self.stats["stage_scrape"] = {
            "discovered": len(discovered),
            "fetched": len(fetched),
            "success": len(successful),
            "failed": len(fetched) - len(successful),
            "duration_s": round(duration, 2),
        }

        logger.info(f"Fetched {len(successful)}/{len(fetched)} articles in {duration:.1f}s.")
        self.raw_articles = successful
        return successful

    def stage_parse(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Stage 2: Parse HTML content into structured article data."""
        logger.info("=" * 50)
        logger.info("STAGE 2: PARSE — Extracting content from HTML")
        logger.info("=" * 50)

        t0 = datetime.now(timezone.utc)
        parsed = []
        success_count = 0
        error_count = 0

        for art in articles:
            try:
                result = extract_article_content(
                    html=art.get("html", ""),
                    url=art.get("url", ""),
                    selectors=art.get("selectors"),
                )
                art.update(result)
                if result.get("success"):
                    success_count += 1
                    parsed.append(art)
                else:
                    logger.debug(f"Parse incomplete for {art.get('url', '?')}: missing title or body")
            except Exception as e:
                error_count += 1
                logger.error(f"Parse error for {art.get('url', '?')}: {e}")

        duration = (datetime.now(timezone.utc) - t0).total_seconds()
        self.stats["stage_parse"] = {
            "input": len(articles),
            "success": success_count,
            "errors": error_count,
            "duration_s": round(duration, 2),
        }

        logger.info(f"Parsed {success_count}/{len(articles)} articles ({error_count} errors) in {duration:.1f}s.")
        self.parsed_articles = parsed
        return parsed

    def stage_score(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Stage 3: Score articles for relevance."""
        logger.info("=" * 50)
        logger.info("STAGE 3: SCORE — Computing relevance scores")
        logger.info("=" * 50)

        t0 = datetime.now(timezone.utc)
        scored = self.scorer.score_batch(articles)

        avg_score = sum(a.get("relevance_score", 0) for a in scored) / max(len(scored), 1)
        above_threshold = sum(1 for a in scored if a.get("is_relevant"))

        duration = (datetime.now(timezone.utc) - t0).total_seconds()
        self.stats["stage_score"] = {
            "input": len(articles),
            "scored": len(scored),
            "avg_score": round(avg_score, 4),
            "above_threshold": above_threshold,
            "duration_s": round(duration, 2),
        }

        logger.info(f"Scored {len(scored)} articles (avg={avg_score:.3f}, relevant={above_threshold}) in {duration:.1f}s.")
        self.scored_articles = scored
        return scored

    def stage_filter(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Stage 4: Filter articles by relevance threshold, age, and body length."""
        logger.info("=" * 50)
        logger.info("STAGE 4: FILTER — Applying quality and relevance filters")
        logger.info("=" * 50)

        t0 = datetime.now(timezone.utc)
        filter_cfg = self.config.get("filter", {})
        threshold = filter_cfg.get("relevance_threshold", 0.4)
        max_age_days = filter_cfg.get("max_age_days", 30)
        min_body_len = filter_cfg.get("min_body_length", 100)

        now = datetime.now(timezone.utc)
        filtered = []
        reasons_dropped: Dict[str, int] = Counter()

        for art in articles:
            # Check relevance score
            if art.get("relevance_score", 0) < threshold:
                reasons_dropped["below_threshold"] += 1
                continue

            # Check body length
            body = art.get("body", "")
            if len(body) < min_body_len:
                reasons_dropped["body_too_short"] += 1
                continue

            # Check article age
            date_parsed = art.get("date_parsed")
            if date_parsed:
                try:
                    age_days = (now - date_parsed).total_seconds() / 86400.0
                    if age_days > max_age_days:
                        reasons_dropped["too_old"] += 1
                        continue
                except TypeError:
                    pass  # Can't compute age — keep the article

            filtered.append(art)

        duration = (datetime.now(timezone.utc) - t0).total_seconds()
        self.stats["stage_filter"] = {
            "input": len(articles),
            "output": len(filtered),
            "dropped": len(articles) - len(filtered),
            "drop_reasons": dict(reasons_dropped),
            "duration_s": round(duration, 2),
        }

        logger.info(f"Filtered {len(filtered)}/{len(articles)} articles. Dropped: {dict(reasons_dropped)}")
        self.filtered_articles = filtered
        return filtered

    def stage_interrogate(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Stage 5: Deep analysis — tag topics, classify urgency, extract key entities."""
        logger.info("=" * 50)
        logger.info("STAGE 5: INTERROGATE — Deep tagging and classification")
        logger.info("=" * 50)

        t0 = datetime.now(timezone.utc)
        interr_cfg = self.config.get("interrogate", {})
        do_entities = interr_cfg.get("extract_entities", True)
        do_topic = interr_cfg.get("classify_topic", True)
        do_urgency = interr_cfg.get("tag_urgency", True)

        # Topic classification keywords
        topic_map = {
            "phk": ["phk", "pemutusan hubungan kerja", "pemecatan", "layoff"],
            "upah": ["upah", "gaji", "umk", "ump", "thr", "lembur", "take home pay"],
            "demo": ["demo", "aksi", "mogok", "unjuk rasa", "protes"],
            "kecelakaan_kerja": ["kecelakaan kerja", "k3", "keselamatan", "meninggal saat kerja"],
            "kebijakan": ["peraturan", "kebijakan", "regulasi", "omnibus", "uu cipta kerja", "perda"],
            "lowongan": ["lowongan", "rekrutmen", "hiring", "bursa kerja", "job fair"],
            "bpjs": ["bpjs", "jaminan sosial", "jamsostek"],
            "outsourcing": ["outsourcing", "alih daya", "kontrak kerja"],
        }

        # Urgency keywords
        urgency_high = ["phk massal", "mogok", "demo besar", "kecelakaan fatal", "tewas", "meninggal"]
        urgency_medium = ["phk", "demo", "unjuk rasa", "pelanggaran", "mangkir"]

        for art in articles:
            text = f"{art.get('title', '')} {art.get('body', '')}".lower()

            # Topic classification
            if do_topic:
                topics = []
                for topic, keywords in topic_map.items():
                    if any(kw in text for kw in keywords):
                        topics.append(topic)
                art["topics"] = topics if topics else ["umum"]

            # Urgency tagging
            if do_urgency:
                if any(kw in text for kw in urgency_high):
                    art["urgency"] = "HIGH"
                elif any(kw in text for kw in urgency_medium):
                    art["urgency"] = "MEDIUM"
                else:
                    art["urgency"] = "LOW"

            # Entity extraction (simple keyword-based)
            if do_entities:
                entities = {
                    "organizations": [],
                    "locations": [],
                }
                org_keywords = [
                    "disnaker", "kemnaker", "apindo", "kspi", "kspsi", "ksbsi",
                    "serikat pekerja", "federasi", "konfederasi", "bpjs ketenagakerjaan",
                ]
                for org in org_keywords:
                    if org in text:
                        entities["organizations"].append(org)

                geo_keywords = self.config.get("scorer", {}).get("keywords", {}).get("geographic", [])
                for loc in geo_keywords:
                    if loc.lower() in text:
                        entities["locations"].append(loc)

                art["entities"] = entities

        duration = (datetime.now(timezone.utc) - t0).total_seconds()
        self.stats["stage_interrogate"] = {
            "input": len(articles),
            "topics_assigned": sum(1 for a in articles if a.get("topics")),
            "urgency_high": sum(1 for a in articles if a.get("urgency") == "HIGH"),
            "urgency_medium": sum(1 for a in articles if a.get("urgency") == "MEDIUM"),
            "urgency_low": sum(1 for a in articles if a.get("urgency") == "LOW"),
            "duration_s": round(duration, 2),
        }

        logger.info(f"Interrogated {len(articles)} articles in {duration:.1f}s.")
        self.interrogated_articles = articles
        return articles

    def stage_save(self, articles: List[Dict[str, Any]]) -> int:
        """Stage 6: Persist articles via manager and generate report."""
        logger.info("=" * 50)
        logger.info("STAGE 6: SAVE — Persisting results and generating report")
        logger.info("=" * 50)

        t0 = datetime.now(timezone.utc)
        saved_count = 0

        for art in articles:
            try:
                self.manager.save_article(art)
                saved_count += 1
            except Exception as e:
                logger.error(f"Failed to save article '{art.get('title', '?')}': {e}")

        # Checkpoint after batch save
        self.manager.save_checkpoint()

        # Generate text report
        report_cfg = self.config.get("report", {})
        output_dir = Path(report_cfg.get("output_dir", "reports"))
        output_dir.mkdir(parents=True, exist_ok=True)
        top_n = report_cfg.get("top_n", 20)

        report_file = output_dir / f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        try:
            sorted_arts = sorted(articles, key=lambda a: a.get("relevance_score", 0), reverse=True)
            with open(report_file, "w", encoding="utf-8") as f:
                f.write(f"NAKER SENTINEL REPORT — {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
                f.write(f"Total articles: {len(articles)}\n")
                f.write("=" * 60 + "\n\n")
                for i, art in enumerate(sorted_arts[:top_n], 1):
                    f.write(f"#{i} [{art.get('relevance_score', 0):.3f}] {art.get('title', 'No title')}\n")
                    f.write(f"   URL: {art.get('url', '-')}\n")
                    f.write(f"   Topics: {', '.join(art.get('topics', []))}\n")
                    f.write(f"   Urgency: {art.get('urgency', '-')}\n\n")
            logger.info(f"Report saved to {report_file}")
        except Exception as e:
            logger.error(f"Failed to write report: {e}")

        duration = (datetime.now(timezone.utc) - t0).total_seconds()
        self.stats["stage_save"] = {
            "saved": saved_count,
            "failed": len(articles) - saved_count,
            "report_file": str(report_file),
            "duration_s": round(duration, 2),
        }

        logger.info(f"Saved {saved_count}/{len(articles)} articles in {duration:.1f}s.")
        return saved_count

    async def run(self):
        """Execute the full pipeline sequentially."""
        self.start_time = datetime.now(timezone.utc)
        logger.info("🚀 NAKER SENTINEL pipeline starting...")
        logger.info(f"Start time: {self.start_time.isoformat()}")

        try:
            # Stage 1: Scrape
            raw = await self.stage_scrape()
            if not raw:
                logger.warning("No articles scraped. Pipeline ends early.")
                return

            # Stage 2: Parse
            parsed = self.stage_parse(raw)
            if not parsed:
                logger.warning("No articles parsed successfully. Pipeline ends early.")
                return

            # Stage 3: Score
            scored = self.stage_score(parsed)

            # Stage 4: Filter
            filtered = self.stage_filter(scored)
            if not filtered:
                logger.warning("All articles filtered out. Nothing to save.")
                return

            # Stage 5: Interrogate
            interrogated = self.stage_interrogate(filtered)

            # Stage 6: Save
            self.stage_save(interrogated)

        except Exception as e:
            logger.critical(f"Pipeline failed with unhandled error: {e}", exc_info=True)
        finally:
            self.end_time = datetime.now(timezone.utc)
            total_s = (self.end_time - self.start_time).total_seconds()
            logger.info(f"Pipeline finished in {total_s:.1f}s.")

            summary = self._build_summary()
            self._print_summary(summary)

    def _build_summary(self) -> Dict[str, Any]:
        """Build a structured summary dict of the entire run."""
        total_s = 0.0
        if self.start_time and self.end_time:
            total_s = (self.end_time - self.start_time).total_seconds()

        final_articles = self.interrogated_articles or self.filtered_articles or []
        topic_dist = self._label_dist(final_articles, "topics")
        urgency_dist = self._label_dist(final_articles, "urgency")

        return {
            "run_start": self.start_time.isoformat() if self.start_time else None,
            "run_end": self.end_time.isoformat() if self.end_time else None,
            "total_duration_s": round(total_s, 2),
            "stages": self.stats,
            "final_article_count": len(final_articles),
            "topic_distribution": topic_dist,
            "urgency_distribution": urgency_dist,
            "top_articles": [
                {
                    "title": a.get("title", ""),
                    "score": a.get("relevance_score", 0),
                    "urgency": a.get("urgency", ""),
                    "topics": a.get("topics", []),
                }
                for a in sorted(final_articles, key=lambda x: x.get("relevance_score", 0), reverse=True)[:5]
            ],
        }

    def _label_dist(self, articles: List[Dict[str, Any]], key: str) -> Dict[str, int]:
        """Count distribution of a given label/key across articles."""
        dist: Counter = Counter()
        for art in articles:
            val = art.get(key)
            if isinstance(val, list):
                for v in val:
                    dist[str(v)] += 1
            elif val is not None:
                dist[str(val)] += 1
        return dict(dist.most_common())

    def _print_summary(self, summary: Dict[str, Any]):
        """Pretty-print the run summary to console and log."""
        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Duration       : {summary.get('total_duration_s', 0):.1f}s")
        logger.info(f"Final articles : {summary.get('final_article_count', 0)}")
        logger.info("")

        # Per-stage stats
        for stage_name, stage_stats in summary.get("stages", {}).items():
            if stage_stats:
                logger.info(f"  {stage_name}: {stage_stats}")

        # Distributions
        logger.info("")
        logger.info("Topic distribution:")
        for topic, count in summary.get("topic_distribution", {}).items():
            logger.info(f"  {topic:20s} : {count}")

        logger.info("Urgency distribution:")
        for urg, count in summary.get("urgency_distribution", {}).items():
            logger.info(f"  {urg:20s} : {count}")

        # Top articles
        logger.info("")
        logger.info("Top articles:")
        for i, art in enumerate(summary.get("top_articles", []), 1):
            logger.info(f"  {i}. [{art['score']:.3f}] {art['title'][:80]}")

        logger.info("=" * 60)


def load_config(path: str = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    """Load YAML configuration with fallback defaults."""
    config_path = Path(path)
    if config_path.exists():
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f) or {}
            logger.info(f"Config loaded from {path}")
            return config
        except yaml.YAMLError as e:
            logger.error(f"YAML parse error in {path}: {e}")
        except Exception as e:
            logger.error(f"Error loading config {path}: {e}")
    logger.warning("Using default configuration.")
    return get_default_config()


def get_default_config() -> Dict[str, Any]:
    """Return sensible default configuration."""
    return {
        "logging": {
            "level": "INFO",
            "file": "sentinel.log",
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        },
        "scraper": {
            "timeout": 30,
            "delay_range": [1.0, 3.0],
        },
        "scorer": {
            "weights": {
                "keyword": 0.35,
                "geographic": 0.20,
                "source_credibility": 0.15,
                "recency": 0.15,
                "negative_penalty": 0.15,
            },
            "keywords": {
                "primary": [
                    "ketenagakerjaan", "buruh", "pekerja", "tenaga kerja",
                    "PHK", "pemutusan hubungan kerja", "mogok", "upah",
                    "serikat pekerja", "hubungan industrial",
                ],
                "secondary": [
                    "outsourcing", "BPJS", "K3", "keselamatan kerja",
                    "lowongan", "pengangguran", "UMK", "UMP",
                    "kontrak kerja", "THR", "lembur", "cuti",
                ],
                "geographic": [
                    "bandung", "jawa barat", "jabar", "cimahi",
                    "sumedang", "garut", "cianjur", "bandung barat",
                    "bandung raya", "kota bandung", "kabupaten bandung",
                ],
            },
            "negative_patterns": [
                r"(?i)hoax|hoaks",
                r"(?i)clickbait",
                r"(?i)iklan\s+promo",
                r"(?i)judi\s+online",
                r"(?i)pinjaman\s+online\s+ilegal",
            ],
            "source_credibility": {
                "kompas_regional": 0.90,
                "detik_bandung": 0.85,
                "cnnindonesia": 0.85,
                "tempo_bisnis": 0.85,
                "pikiran_rakyat": 0.80,
                "bisnis_jabar": 0.80,
                "rri_jabar": 0.75,
                "tribun_jabar": 0.70,
                "galamedia": 0.65,
                "disnaker_bandung": 0.95,
            },
            "recency_half_life_days": 7.0,
            "relevance_threshold": 0.4,
        },
        "manager": {
            "checkpoint_dir": "checkpoints",
            "backup_count": 5,
        },
        "report": {
            "output_dir": "reports",
            "top_n": 20,
        },
        "filter": {
            "relevance_threshold": 0.4,
            "max_age_days": 30,
            "min_body_length": 100,
        },
        "interrogate": {
            "extract_entities": True,
            "classify_topic": True,
            "tag_urgency": True,
        },
    }


def build_cli() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="sentinel",
        description="NAKER SENTINEL — Labor news monitoring pipeline for Bandung region.",
    )
    parser.add_argument(
        "-c", "--config",
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to YAML config file (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable DEBUG logging level",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run pipeline without saving results (for testing)",
    )
    return parser


def main():
    """Entry point."""
    parser = build_cli()
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    # Override logging level if verbose
    if args.verbose:
        config.setdefault("logging", {})["level"] = "DEBUG"

    # Dry-run flag
    if args.dry_run:
        config["dry_run"] = True

    # Create and run pipeline
    sentinel = NakerSentinel(config)
    asyncio.run(sentinel.run())


if __name__ == "__main__":
    main()