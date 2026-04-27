# ============================================================
# sentinel.py - Menjalankan pipeline naker secara end-to-end dengan logging terperinci, penanganan error, dan laporan akhir.
# Desain berbasis class dengan pipeline yang diatur: scrape → parse → score → filter → interrogate → save.
# ============================================================

import asyncio
import argparse
import logging
import yaml
import sys
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from pathlib import Path
from collections import Counter
from .bandung_scraper import BandungScraper
from .parser import extract_article_content, parse_date_safe
from .scorer import RelevanceScorer
from .manager import DataManager
from src.ai_engine import BPS_AI_Engine

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
        Standardizing on BPS_AI_Engine for centralized LLM orchestration.
        """
        self.config = config or {}
        
        self.sites = [
            "bandung.go.id", "tempo.co", "tirto.id", "narasi.tv",
            "ayobandung.com", "pikiran-rakyat.com", "bandung.kompas.com",
            "disdagin.bandung.go.id", "cnnindonesia.com", "rri.co.id",
            "jabarprov.go.id", "bps.go.id", "kemnaker.go.id", "kompas.com", "detik.com",
            "radarbandung.id", "kumparan.com", "infobandungkota.com",
            "prfmnews.id", "kilasbandungnews.com", "bandungbergerak.id",
            "koranmandala.com", "jabarekspres.com", "jabar.tribunnews.com",
            "liputan6.com", "merdeka.com", "sindonews.com",
            "blogspot.com", "wordpress.com", "medium.com",
            "facebook.com", "twitter.com", "instagram.com"
        ]
        
        self.model_name = self.config.get("model_name", "bps-naker")
        self.ai_engine = BPS_AI_Engine()
        
        self.mode = self.config.get("mode", "live")
        self.start_date = self.config.get("start", "")
        self.end_date = self.config.get("end", "")
        
        self.edge_source_dir = str(Path.home() / "AppData" / "Local" / "Microsoft" / "Edge" / "User Data")

        self._setup_logging()

        self.scraper = BandungScraper(config.get("scraper", {}))
        self.scorer = RelevanceScorer(config.get("scorer", {}))
        self.manager = DataManager(config.get("manager", {}))

        self.raw_articles: List[Dict[str, Any]] = []
        self.parsed_articles: List[Dict[str, Any]] = []
        self.scored_articles: List[Dict[str, Any]] = []
        self.filtered_articles: List[Dict[str, Any]] = []
        self.interrogated_articles: List[Dict[str, Any]] = []

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

        root = logging.getLogger()
        root.setLevel(level)

        if not root.handlers:
            ch = logging.StreamHandler(sys.stdout)
            ch.setLevel(level)
            ch.setFormatter(logging.Formatter(log_format))
            root.addHandler(ch)

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
        """Stage 1: Discover, Deduplicate, and Fetch articles from all sources."""
        logger.info("==================================================")
        logger.info("STAGE 1: SCRAPE — Discovering and fetching articles")
        logger.info("==================================================")
        
        t0 = datetime.now(timezone.utc)
        
        discovered = await self.scraper.discover_articles()
        
        visited_urls = self.manager.load_visited_urls()
        new_articles = [art for art in discovered if art["url"] not in visited_urls]
        
        logger.info(f"Total Temuan: {len(discovered)} | Sudah Diproses (Dibuang): {len(discovered)-len(new_articles)} | Target Unduh: {len(new_articles)}")
        
        if not new_articles:
            logger.warning("Tidak ada artikel baru yang ditemukan. Pipeline dihentikan efisien.")
            duration = (datetime.now(timezone.utc) - t0).total_seconds()
            self.stats["stage_scrape"] = {
                "discovered": len(discovered),
                "fetched": 0, "success": 0, "failed": 0, "duration_s": round(duration, 2)
            }
            return []
            
        fetched = await self.scraper.fetch_all_articles(new_articles)
        successful = [a for a in fetched if a.get("fetch_success")]

        duration = (datetime.now(timezone.utc) - t0).total_seconds()
        self.stats["stage_scrape"] = {
            "discovered": len(discovered),
            "fetched": len(fetched),
            "success": len(successful),
            "failed": len(fetched) - len(successful),
            "duration_s": round(duration, 2),
        }

        logger.info(f"Berhasil mengunduh {len(successful)}/{len(fetched)} artikel dalam {duration:.1f} detik.")
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

        import re

        for art in articles:
            try:
                target_url = art.get("url", "")
                if "pikiran-rakyat.com" in target_url or "ayobandung.com" in target_url:
                    target_url = re.sub(r'/page/\d+', '', target_url)
                    if "?" not in target_url:
                        target_url += "?page=all"
                
                result = extract_article_content(
                    html=art.get("html", ""),
                    url=target_url,
                    selectors=art.get("selectors"),
                )
                art.update(result)
                
                art['content'] = art.get('body') or art.get('text') or art.get('snippet') or ""
                
                raw_date = art.get("published") or art.get("date") or ""
                art["date"] = parse_date_safe(raw_date)

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
        
        threshold = filter_cfg.get("relevance_threshold", 0.1)
        max_age_days = filter_cfg.get("max_age_days", 30)
        min_body_len = filter_cfg.get("min_body_length", 50)

        now = datetime.now(timezone.utc)
        filtered = []
        reasons_dropped: Dict[str, int] = Counter()

        for art in articles:
            if art.get("relevance_score", 0) < threshold:
                reasons_dropped["below_threshold"] += 1
                continue

            body = art.get("body", "")
            if len(body) < min_body_len:
                reasons_dropped["body_too_short"] += 1
                continue

            date_parsed = art.get("date_parsed")
            if date_parsed:
                try:
                    age_days = (now - date_parsed).total_seconds() / 86400.0
                    if age_days > max_age_days:
                        reasons_dropped["too_old"] += 1
                        continue
                except TypeError:
                    pass

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

    def stage_interrogate(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """
        Stage 4: Deep analysis using local SLM (Ollama).
        Mengirim teks artikel ke model bps-naker untuk klasifikasi KBLI,
        analisis dampak (bekerja/pengangguran), dan ekstraksi ringkasan.
        """
        if not self.config.get("interrogate", {}).get("extract_entities", True):
            return article

        import requests
        import json
        import re

        text = article.get("content") or article.get("snippet") or ""
            
        if not text:
            article["status_geografi"] = "Error: Teks Kosong"
            return article
            
        text_lower = text.lower()
        if len(text) > 1500:
            first_chunk = text[:800]
            match = re.search(r'bandung', text_lower[800:])
            if match:
                start_idx = 800 + max(0, match.start() - 350)
                end_idx = 800 + min(len(text_lower[800:]), match.start() + 350)
                second_chunk = text[start_idx:end_idx]
                truncated_text = first_chunk + "\n\n...[POTONGAN BUKTI LOKASI]...\n\n" + second_chunk
            else:
                truncated_text = text[:1500]
        else:
            truncated_text = text

        custom_prompt = f"""
        Lakukan audit investigatif pada teks berita berikut untuk laporan Fenomena Ketenagakerjaan BPS Kota Bandung.
        Keluarkan format JSON MURNI dengan keys:
        1. "status_geografi" (Valid Kota Bandung / Out of Jurisdiction / Irrelevant)
        2. "ringkasan_berita" (Satu paragraf padat merangkum kejadian utama ketenagakerjaan).
        3. "dampak_bekerja" (1 Naik / 2 Turun / 3 Tetap)
        4. "dampak_pengangguran" (1 Naik / 2 Turun / 3 Tetap)
        5. "kategori_kbli" (Pilih SATU Kategori Huruf A sampai U yang paling relevan beserta namanya. Misal: "C. Industri Pengolahan", "G. Perdagangan", dll).
        6. "confidence_score" (0-100, seberapa yakin Anda dengan analisis dampak naik/turun ini berdasarkan teks).
        
        ATURAN ANALISIS (HUKUM ABSOLUT):
        - Status Geofencing HARUS "Valid Kota Bandung" JIKA peristiwa terjadi secara fisik di Kota Bandung. Jika hanya menyebutkan "Jawa Barat" tanpa menyebut "Kota Bandung" secara eksplisit, maka statusnya adalah "Out of Jurisdiction".
        - Pekerja NAIK & Pengangguran TURUN jika: Pembukaan pabrik, job fair besar, ekspansi bisnis, proyek infrastruktur jalan.
        - Pekerja TURUN & Pengangguran NAIK jika: PHK massal, pabrik tutup, gulung tikar, gagal panen, omzet anjlok drastis.
        - Jika hanya membahas isu normatif (Tuntutan UMK, Aturan THR, Demo tanpa PHK), status keduanya adalah '3 Tetap'.
        - TOLAK JIKA (Irrelevant Context): Berita TIDAK ADA hubungannya dengan fenomena ketenagakerjaan makro (contoh: deskripsi jenis kursi kereta, kasus kriminalitas, atau tips harian). Anda WAJIB mengisi "status_geografi" dengan "Irrelevant Context" dan "kategori_kbli" dengan "N/A". Berita info lowongan kerja individual (cara melamar, link loker) juga harus ditolak.
        - [PENTING] Jika berita tentang "Job Fair Umum", "Bursa Kerja Lintas Sektor", atau "Lowongan Kerja Ribuan Orang", maka isi "kategori_kbli" dengan "N/A (Bursa Kerja Umum)", BUKAN Perdagangan, Penyewaan, atau Industri spesifik.

        
        Teks Berita:
        {truncated_text}
        """

        payload = {
            "model": getattr(self, 'model_name', 'bps-naker'),
            "prompt": custom_prompt,
            "format": "json",
            "stream": False
        }

        try:
            logger.info(f"Mengirim Context Window ke SLM untuk URL: {article.get('url', '')[:50]}...")
            response = requests.post(getattr(self, 'ollama_url', 'http://localhost:11434/api/generate'), json=payload, timeout=120)
            
            if response.status_code == 200:
                raw_json = response.json().get("response", "{}")
                try:
                    audit_result = json.loads(raw_json) if raw_json else {}
                    if not isinstance(audit_result, dict):
                        audit_result = {}

                    article["status_geografi"] = audit_result.get("status_geografi", "Unknown")
                    article["ringkasan_berita"] = audit_result.get("ringkasan_berita", "")
                    article["dampak_bekerja"] = audit_result.get("dampak_bekerja", "")
                    article["dampak_pengangguran"] = audit_result.get("dampak_pengangguran", "")
                    article["kategori_kbli"] = audit_result.get("kategori_kbli", "")
                    article["confidence_score"] = audit_result.get("confidence_score", "N/A")
                except json.JSONDecodeError:
                    logger.error(f"Format SLM non-JSON untuk URL: {article.get('url')}")
                    article["status_geografi"] = "Error: Format SLM non-JSON"
            else:
                logger.error(f"SLM menolak dengan status {response.status_code}")
                article["status_geografi"] = f"Error: SLM Status {response.status_code}"
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Gagal menghubungi Ollama: {e}")
            article["status_geografi"] = "Error: Daemon Ollama tertidur / Port tertutup."

        return article
    
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

        self.manager.save_checkpoint()
        
        try:
            import pandas as pd
            excel_dir = Path("data/naker/exports")
            excel_dir.mkdir(parents=True, exist_ok=True)
            excel_file = excel_dir / f"bps_audit_naker_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            
            df = pd.DataFrame(articles)
            
            if not df.empty:
                rename_map = {
                    "ringkasan_berita": "Ringkasan Berita/Informasi Utama",
                    "url": "Sumber Berita (URL)",
                    "date": "Tanggal Berita",
                    "dampak_bekerja": "Bekerja (1 Naik / 2 Turun / 3 Tetap)",
                    "dampak_pengangguran": "Pengangguran (1 Naik / 2 Turun / 3 Tetap)",
                    "kategori_kbli": "Kategori Lapangan Usaha (KBLI)",
                    "confidence_score": "Confidence Score (%)",
                    "status_geografi": "Status Geografi",
                    "title": "Judul Asli"
                }
                df = df.rename(columns=rename_map)
                
                expected_cols = list(rename_map.values())
                available_cols = [c for c in expected_cols if c in df.columns]
                
                df[available_cols].to_excel(excel_file, index=False, engine='openpyxl')
                logger.info(f"Excel Final berhasil dibuat: {excel_file}")
                
        except Exception as e:
            logger.error(f"Gagal membuat file Excel: {e}")

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
            logger.info(f"Report txt saved to {report_file}")
        except Exception as e:
            logger.error(f"Failed to write report txt: {e}")

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
        """Eksekusi Streaming Pipeline (Site-by-Site, Analisis Real-time & Checkpoint)."""
        from collections import defaultdict
        
        self.start_time = datetime.now(timezone.utc)
        logger.info("NAKER SENTINEL pipeline starting (Streaming Mode)...")
        
        raw_discovered = await self.scraper.discover_articles()
        visited_urls = self.manager.load_visited_urls()
        
        site_groups = defaultdict(list)
        for art in raw_discovered:
            if art["url"] not in visited_urls:
                site_groups[art["site"]].append(art)
                
        total_targets = sum(len(v) for v in site_groups.values())
        if total_targets == 0:
            logger.info(" [!] Tidak ada artikel baru. Selesai.")
            return

        logger.info(f" [>] Membidik {total_targets} artikel baru dari {len(site_groups)} situs.")
        
        await self.scraper.start_browser()
        final_articles = []
        checkpoint_count = 0
        
        try:
            for site, articles in site_groups.items():
                print(f"\n[>>>] MEMPROSES SITUS: {site.upper()} ({len(articles)} Artikel)")
                
                for art in articles:
                    title_short = art["title"][:55]
                    url = art["url"]
                    clickable_link = f"\033]8;;{url}\033\\[BACA ARTIKEL]\033]8;;\033\\"
                    
                    try:
                        rejected, reason = self.scorer.is_rejected_preflight(art["title"], url)
                        if rejected:
                            print(f" -> {title_short}...")
                            print(f"    [BLOCKED PRE-FLIGHT] {reason}. {clickable_link}")
                            continue

                        print(f" [>] Mengekstraksi [{site}]: {title_short}...")

                        html_data = await self.scraper.fetch_single_article(url)
                        if not html_data.get("fetch_success"):
                            print(f"     [FAILED] Gagal mengunduh halaman atau Timeout. {clickable_link}")
                            continue
                            
                        parsed = extract_article_content(html_data["html"], url)
                        text_content = parsed.get("text", "")
                        if not text_content or len(text_content) < 150:
                            print(f"     [SKIPPED] Konten terlalu pendek atau dilindungi paywall. {clickable_link}")
                            continue
                            
                        score, breakdown = self.scorer.calculate_v66_score(parsed.get("title", ""), url, text_content)
                        if score < 20: 
                            # Mengambil alasan dari breakdown (misal: "Noise/Kriminal (-30)")
                            skip_reason = list(breakdown.values())[0] if breakdown else "Tidak ada indikator ketenagakerjaan kuat"
                            print(f"     [SKIPPED] {skip_reason}. {clickable_link}")
                            continue
                            
                        truncated = text_content[:1500] 
                        
                        try:
                            ai_result = self.ai_engine.classify_naker(truncated)
                            
                            if not ai_result or not isinstance(ai_result, dict):
                                raise ValueError("AI Engine mengembalikan Null/Bukan Dictionary")
                                
                        except Exception as ai_error:
                            print(f"     [WARNING] Ollama Offline/Timeout: {ai_error}")
                            ai_result = {
                                "status_geografi": "Error: Ollama Offline",
                                "ringkasan_berita": "Gagal diekstrak karena Ollama Offline",
                                "dampak_bekerja": "3 Tetap",
                                "dampak_pengangguran": "3 Tetap",
                                "kategori_kbli": "Unknown",
                                "confidence_score": 0
                            }
                        
                        self._print_isolated_log(site, parsed.get("title", ""), url, score, ai_result)
                        
                        final_data = {**art, **parsed, "score": score, **ai_result}
                        final_articles.append(final_data)
                        self.manager.save_visited_urls_delta({url})
                        
                        checkpoint_count += 1
                        if checkpoint_count % 5 == 0:
                            self.manager.save_final(final_articles)
                            print(f"  [√] Incremental Checkpoint tersimpan ({checkpoint_count} artikel).")
                            
                    except Exception as loop_e:
                        print(f"     [ERROR] Insiden pada eksekusi artikel: {loop_e}. {clickable_link}")
                        if "Target page, context or browser has been closed" in str(loop_e):
                            print(f"     [!] Browser Context mati. Mencoba me-restart browser untuk situs selanjutnya...")
                            try:
                                await self.scraper.close_browser()
                            except:
                                pass
                            import asyncio
                            await asyncio.sleep(2)
                            await self.scraper.start_browser()
                        
        except KeyboardInterrupt:
            logger.warning("\n[!] INTERUPSI (CTRL+C). Mengamankan data dengan tenang...")
        except Exception as e:
            logger.error(f"Error Pipeline: {e}")
        finally:
            await self.scraper.close_browser()
            if final_articles:
                self.manager.save_final(final_articles)
                logger.info(f"Pipeline selesai. Master data berisi {len(final_articles)} hasil audit tersimpan.")

    def _print_isolated_log(self, site: str, title: str, url: str, score: int, ai_result: dict):
        """Mencetak log eksklusif dengan ANSI Escape Sequence untuk Clickable Link."""
        geo = ai_result.get("status_geografi", "Unknown")
        kbli = ai_result.get("kategori_kbli", "N/A")
        ringkasan = ai_result.get("ringkasan_berita", "-")
        
        clickable_link = f"\033]8;;{url}\033\\[BACA ARTIKEL]\033]8;;\033\\"
        
        print(f"\n" + "="*70)
        print(f"[+] SUMBER  : {site}")
        print(f"[-] JUDUL   : {title[:80]}...")
        print(f"[-] URL     : {clickable_link} -> {url[:40]}...")
        print(f"[>] SKOR    : {score}/100")
        print(f"[A] AI AUDIT: {geo} | KBLI: {kbli[:30]}")
        print(f"    Ringkas : {ringkasan}")
        print("="*70)

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
            "relevance_threshold": 0.1,
            "max_age_days": 30,
            "min_body_length": 50,
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
    parser.add_argument(
        "--mode", type=str, default="live", choices=["live", "history"],
        help="Mode eksekusi (live/history)"
    )
    parser.add_argument(
        "--start", type=str, default="", help="Format: YYYY-MM-DD"
    )
    parser.add_argument(
        "--end", type=str, default="", help="Format: YYYY-MM-DD"
    )
    parser.add_argument(
        "--merge", action="store_true", help="Satukan semua file audit menjadi satu Excel utama"
    )
    return parser


def main():
    """Entry point."""
    parser = build_cli()
    args = parser.parse_args()

    config = load_config(args.config)

    if args.verbose:
        config.setdefault("logging", {})["level"] = "DEBUG"

    config.setdefault("scraper", {})
    config["scraper"].update({
        "mode": args.mode,
        "start": args.start,
        "end": args.end
    })

    sentinel = NakerSentinel(config)
    
    try:
        asyncio.run(sentinel.run())
    except KeyboardInterrupt:
        print("\n" + "="*60)
        print("[INTERRUPT] Sinyal penghentian (CTRL+C) diterima.")
        print("[SYSTEM] Pipeline NAKER dihentikan secara aman.")
        print("[SYSTEM] Progress Anda tidak hilang. History URL telah disandikan di 'visited_url_naker.txt'.")
        print("="*60 + "\n")
    except Exception as e:
        logger.error(f"[FATAL] Pipeline gagal: {e}")

    if args.merge:
        print("\n[SYSTEM] Memulai Prosedur Penggabungan Data Audit...")
        try:
            sentinel.manager.merge_audit_files(start_date=args.start, end_date=args.end)
        except Exception as e:
            logger.error(f"Gagal menggabungkan file audit: {e}")

    sys.exit(0)

if __name__ == "__main__":
    main()