import os
import sys
import asyncio
import feedparser
import re
import urllib.parse
import base64
import pandas as pd
import requests
import json
import random
import argparse
import shutil
from datetime import datetime
from pathlib import Path
from newspaper import Article, Config
from playwright.async_api import async_playwright, TimeoutError, Error as PlaywrightError
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse
import warnings

warnings.filterwarnings("ignore")


class BPS_Sovereign_Sentinel:
    """
    Surgical Debugger V61 | The Sovereign Sentinel
    Fixes: __init__ ordering, run() restoration, async check_ollama,
           persistent dedup, race condition lock, _save_visited_urls delta-only.
    """

    def __init__(self, args):
        self.args = args

        self.sites = [
            "bandung.go.id", "jabarprov.go.id", "jabar.tribunnews.com",
            "tempo.co", "tirto.id", "narasi.tv", "ayobandung.com",
            "pikiran-rakyat.com", "bandung.kompas.com", "disdagin.bandung.go.id",
            "detik.com", "republika.co.id",
        ]

        # FIX #1: Semua path didefinisikan LEBIH DULU sebelum dipakai
        self.edge_source_dir = str(
            Path.home() / "AppData" / "Local" / "Microsoft" / "Edge" / "User Data"
        )
        self.workspace_dir = Path.cwd() / "data" / "edge_workspace"
        self.export_dir = Path.cwd() / "data" / "exports"
        os.makedirs(self.workspace_dir, exist_ok=True)
        os.makedirs(self.export_dir, exist_ok=True)

        # FIX #2: visited_file didefinisikan SETELAH export_dir ada
        self.visited_file = self.export_dir / "visited_urls.txt"

        # FIX #3: seen_urls dimuat dari file (persistent dedup), HANYA SEKALI
        self._urls_from_prev_session = self._load_visited_urls()
        self.seen_urls = set(self._urls_from_prev_session)  # copy untuk sesi ini
        self._new_urls_this_session: set[str] = set()       # hanya URL baru sesi ini

        self.session_data: list[dict] = []
        self.ollama_url = "http://localhost:11434/api/generate"
        self.model_name = "bps-auditor"

        # Semaphore: maks 2 tab browser serentak (hemat RAM)
        self.browser_semaphore = asyncio.Semaphore(2)
        # Lock: proteksi race condition pada seen_urls & session_data
        self._lock = asyncio.Lock()

        self.config = {
            "GEOGRAPHY": {
                "STRICT_ANCHORS": [
                    r"\bkota[\s\-]?bandung\b", r"\bpemkot[\s\-]?bandung\b",
                    r"\bwali[\s\-]?kota[\s\-]?bandung\b", r"\bbandung[\s\-]?kota\b",
                    r"\bdprd[\s\-]?kota[\s\-]?bandung\b",
                    r"\bdisdagin[\s\-]?kota[\s\-]?bandung\b",
                    r"\bdkpp[\s\-]?kota[\s\-]?bandung\b",
                ],
                "DISTRICTS": [
                    r"\bandir\b", r"\bastana[\s\-]?anyar\b", r"\bantapani\b",
                    r"\barcamanik\b", r"\bbabakan[\s\-]?ciparay\b",
                    r"\bbandung[\s\-]?kidul\b", r"\bbandung[\s\-]?kulon\b",
                    r"\bbandung[\s\-]?wetan\b", r"\bbatununggal\b",
                    r"\bbojongloa[\s\-]?kaler\b", r"\bbojongloa[\s\-]?kidul\b",
                    r"\bbuah[\s\-]?batu\b", r"\bcibeunying\b", r"\bcibiru\b",
                    r"\bcicendo\b", r"\bcidadap\b", r"\bcinambo\b", r"\bcoblong\b",
                    r"\bgedebage\b", r"\bkiara[\s\-]?condong\b", r"\blengkong\b",
                    r"\bmandalajati\b", r"\bpanyileukan\b", r"\brancasari\b",
                    r"\bregol\b", r"\bsukajadi\b", r"\bsukasari\b",
                    r"\bsumur[\s\-]?bandung\b", r"\bujung[\s\-]?berung\b",
                    r"\bcibaduyut\b", r"\bpasar[\s\-]?baru\b", r"\bsuci\b",
                    r"\bcigondewah\b", r"\bbinong\b",
                ],
                "BLACKLIST": [
                    r"\bkabupaten[\s\-]?bandung\b", r"\bbupati\b", r"\bsoreang\b",
                    r"\bkbb\b", r"\bbandung[\s\-]?barat\b", r"\blembang\b",
                    r"\bcimahi\b", r"\bmajalaya\b", r"\bpangalengan\b",
                    r"\bciwidey\b", r"\bpadalarang\b", r"\bpemkab\b",
                ],
            },
            "TRADE_FLUX": {
                "INTERNASIONAL": [
                    r"\bekspor\b", r"\bimpor\b", r"\bpasar[\s\-]?global\b",
                    r"\bpasar[\s\-]?internasional\b", r"\bmancanegara\b",
                    r"\bluar[\s\-]?negeri\b", r"\bbea[\s\-]?cukai\b",
                    r"\bkite[\s\-]?ikm\b",
                ],
                "ANTAR_DAERAH": [
                    r"\bpasokan\b", r"\bsuplai\b", r"\brantai[\s\-]?pasok\b",
                    r"\blogistik\b", r"\bdistribusi\b", r"\bantar[\s\-]?daerah\b",
                    r"\bantar[\s\-]?provinsi\b", r"\bkontainer\b", r"\bdry[\s\-]?port\b",
                ],
                "INDICATORS": [
                    r"\bkelangkaan\b", r"\bkenaikan[\s\-]?harga\b", r"\bstok\b",
                    r"\bfluktuasi\b", r"\bharga[\s\-]?eceran\b", r"\bhet\b",
                    r"\binflasi\b", r"\bdefisit\b", r"\bsurplus\b",
                ],
            },
            "COMMODITIES": [
                r"\bberas\b", r"\bjagung\b", r"\bminyak[\s\-]?goreng\b", r"\bgula\b",
                r"\bterigu\b", r"\bkedelai\b", r"\bcabai\b", r"\bbawang\b",
                r"\bsayur\b", r"\bdaging\b", r"\btelur\b", r"\bpakaian\b",
                r"\bgarmen\b", r"\bsepatu\b", r"\btekstil\b", r"\bsemen\b",
                r"\belpiji\b", r"\bbbm\b", r"\bkopi\b", r"\bkakao\b", r"\bkosmetik\b",
            ],
            "NOISE_WORDS": [
                r"\bpiala\b", r"\bliga\b", r"\bgempa\b", r"\bkecelakaan\b",
                r"\bpembunuhan\b", r"\bpersib\b", r"\bskandal\b", r"\bpilkada\b",
                r"\bkampanye\b", r"\bcapres\b", r"\bcawalkot\b", r"\bpartai\b",
                r"\bsnbt\b", r"\bunpad\b", r"\bkemendikdasmen\b", r"\bhakim\b",
                r"\bperadilan\b", r"\bkuliner\b", r"\bwisatawan\b", r"\bnarkoba\b",
                r"\btiket\b", r"\bkonser\b", r"\bsurat[\s\-]?suara\b",
                r"\bcpns\b", r"\blowongan\b", r"\bloker\b", r"\brekrutmen\b",
                r"\bmahasiswa\b", r"\bkampus\b", r"\bwisuda\b", r"\bseminar\b",
                r"\bwebinar\b", r"\bombudsman\b", r"\bppdb\b",
            ],
            "TITLE_BLACKLIST": [
                "pegawai", "sejarah", "visi", "misi", "tupoksi", "kontak", "gallery",
                "profil", "rencana strategis", "bab i", "powerpoint", "open data",
                "loker", "jurnal", "pengumuman", "layanan", "jadwal", "detail",
                "slidefabric", "kode pos", "beranda", "home", "indeks", "index",
                "kategori", "registrasi imei",
            ],
            "DOCUMENT_EXTENSIONS": [
                ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx"
            ],
        }

    # ─────────────────────────────────────────────
    # PERSISTENT DEDUP HELPERS (proper class methods)
    # ─────────────────────────────────────────────

    def _load_visited_urls(self) -> set:
        """Memuat URL yang sudah dikunjungi dari sesi sebelumnya."""
        if self.visited_file.exists():
            lines = self.visited_file.read_text(encoding="utf-8").splitlines()
            loaded = {line.strip() for line in lines if line.strip()}
            print(f"     [✓] Memuat {len(loaded)} URL dari sesi sebelumnya (dedup persisten).")
            return loaded
        return set()

    def _save_visited_urls(self):
        """FIX: Hanya menyimpan URL BARU sesi ini (delta), bukan semua URL ulang."""
        if not self._new_urls_this_session:
            return
        with open(self.visited_file, "a", encoding="utf-8") as f:
            for url in self._new_urls_this_session:
                f.write(url + "\n")
        print(f"     [✓] {len(self._new_urls_this_session)} URL baru disimpan ke dedup registry.")

    # ─────────────────────────────────────────────
    # SETUP & VALIDASI
    # ─────────────────────────────────────────────

    def prepare_workspace(self):
        print(" [>] Mengamankan Ruang Isolasi Browser...")
        source = Path(self.edge_source_dir) / "Default"
        target = self.workspace_dir / "Default"
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)
        try:
            shutil.copytree(
                source,
                target,
                ignore=shutil.ignore_patterns(
                    "SingletonLock", "lock",
                    "Cache", "Code Cache", "GPUCache",
                    "Service Worker", "CachedData", "Crashpad",
                ),
            )
            print("     [✓] Profile Edge berhasil dikopi (cache diexclude).")
        except Exception as e:
            print(f"     [!] Peringatan saat kopi profil: {e}")

    # FIX #4: Hanya ada SATU check_ollama, versi async
    async def check_ollama(self) -> bool:
        try:
            r = await asyncio.to_thread(
                requests.get, "http://localhost:11434/api/version", timeout=3
            )
            if r.status_code == 200:
                print("     [✓] Ollama Server siap beroperasi.")
                return True
        except Exception:
            pass
        print("     [!!!] Ollama TIDAK TERDETEKSI. Jalankan `ollama serve` di terminal terpisah.")
        return False

    # ─────────────────────────────────────────────
    # URL PROCESSING
    # ─────────────────────────────────────────────

    def _decode_google_url(self, url: str) -> str:
        real_url = url
        if "articles/CBM" in url:
            try:
                encoded_str = url.split("articles/")[1].split("?")[0]
                padding = 4 - (len(encoded_str) % 4)
                encoded_str += "=" * padding
                decoded_bytes = base64.urlsafe_b64decode(encoded_str)
                match = re.search(
                    rb"(https?://[a-zA-Z0-9\-\.\_\/\?\=\&\%\+]+)", decoded_bytes
                )
                if match:
                    real_url = match.group(1).decode("utf-8")
            except Exception:
                pass

        paginated_domains = [
            "tribunnews.com", "pikiran-rakyat.com", "ayobandung.com",
            "kompas.com", "tirto.id",  # FIX: tirto.id dikembalikan
        ]
        if any(domain in real_url for domain in paginated_domains):
            real_url = self._append_page_all(real_url)
        return real_url

    def _append_page_all(self, url: str) -> str:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        params["page"] = ["all"]
        new_query = urlencode(params, doseq=True)
        return urlunparse(parsed._replace(query=new_query))

    # ─────────────────────────────────────────────
    # FILTERING
    # ─────────────────────────────────────────────

    def is_rejected_preflight(self, title: str, url: str):
        title_lower = title.lower()
        url_lower = url.lower()
        # combined dipakai konsisten di semua cek
        combined = f"{title_lower} {url_lower}"

        if any(b in combined for b in self.config["TITLE_BLACKLIST"]):
            return True, "Halaman Statis/Administratif"
        for ext in self.config["DOCUMENT_EXTENSIONS"]:
            if url_lower.endswith(ext) or (ext + "?" in url_lower):
                return True, "Ekstensi Dokumen Non-Naratif"
        if any(re.search(n, combined) for n in self.config["NOISE_WORDS"]):
            return True, "Terdeteksi Noise Konteks"

        is_blacklisted = any(
            re.search(b, combined) for b in self.config["GEOGRAPHY"]["BLACKLIST"]
        )
        has_strong_anchor = any(
            re.search(a, combined) for a in self.config["GEOGRAPHY"]["STRICT_ANCHORS"]
        )
        if is_blacklisted and not has_strong_anchor:
            return True, "Fokus ke Wilayah Tetangga (Tanpa Anchor Kuat)"

        return False, "Aman"

    def verify_mime_type(self, url: str):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        for attempt in range(2):
            try:
                with requests.get(url, stream=True, headers=headers, timeout=8) as resp:
                    ct = resp.headers.get("Content-Type", "").lower()
                    if "application/" in ct and "xhtml" not in ct:
                        return False, f"Format Dokumen Ditolak ({ct})"
                    return True, "Valid HTML"
            except requests.exceptions.Timeout:
                if attempt == 1:
                    return True, "MIME Timeout (Bypass)"
            except Exception:
                return True, "MIME Error"
        return True, "MIME Error"

    def is_relevant_lexical(self, title: str, text: str):
        combined = f"{title} {text}".lower()

        if any(re.search(n, combined) for n in self.config["NOISE_WORDS"]):
            return False, "Terdeteksi Noise Konteks"

        is_blacklisted = any(
            re.search(b, combined) for b in self.config["GEOGRAPHY"]["BLACKLIST"]
        )
        has_strong_anchor = any(
            re.search(g, combined) for g in self.config["GEOGRAPHY"]["STRICT_ANCHORS"]
        )
        if is_blacklisted and not has_strong_anchor:
            return False, "Fokus ke Wilayah Tetangga"

        has_strict_geo = has_strong_anchor or any(
            re.search(d, combined) for d in self.config["GEOGRAPHY"]["DISTRICTS"]
        )
        if not has_strict_geo:
            return False, "Gagal Geofencing (Tidak eksplisit Kota Bandung)"

        has_trade_intl = any(
            re.search(t, combined) for t in self.config["TRADE_FLUX"]["INTERNASIONAL"]
        )
        has_trade_domestic = any(
            re.search(t, combined)
            for t in self.config["TRADE_FLUX"]["ANTAR_DAERAH"]
            + self.config["TRADE_FLUX"]["INDICATORS"]
        )
        has_commodity = any(
            re.search(c, combined) for c in self.config["COMMODITIES"]
        )

        if has_trade_intl:
            return True, "Lolos Leksikal: Perdagangan Internasional"
        elif has_trade_domestic and has_commodity:
            return True, "Lolos Leksikal: Perdagangan Domestik + Komoditas"
        return False, "Lolos Geografi, namun miskin indikator BPS"

    # ─────────────────────────────────────────────
    # BROWSER HELPERS
    # ─────────────────────────────────────────────

    async def network_interceptor(self, route):
        blocked = [
            "doubleclick.net", "googlesyndication.com", "ads-twitter.com",
            "gliastudios.com", "seedtag.com",
        ]
        if any(b in route.request.url.lower() for b in blocked):
            await route.abort()
            return
        await route.continue_()

    async def cloudflare_organic_wait(self, page):
        try:
            iframe = await page.wait_for_selector(
                'iframe[src*="cloudflare"], #challenge-running', timeout=5000
            )
            if iframe:
                print(
                    f"\a\n     [!!!] CLOUDFLARE pada {page.url[:40]}... Centang Captcha!"
                )
                await page.wait_for_function(
                    "document.querySelector('iframe[src*=\"cloudflare\"]') === null "
                    "&& document.querySelector('#challenge-running') === null",
                    timeout=60000,
                )
                await page.wait_for_timeout(3000)
        except Exception:
            pass

    async def execute_hydration_scroll(self, page):
        try:
            vh = await page.evaluate("window.innerHeight")
            for _ in range(5):
                await page.evaluate(f"window.scrollBy(0, {vh * 0.7})")
                await page.wait_for_timeout(800)
            await page.evaluate("window.scrollTo(0, 0)")
            await page.wait_for_timeout(1000)
        except Exception:
            pass

    async def extract_text_native(self, page) -> str:
        try:
            text = await page.evaluate(
                """() => {
                let body = document.querySelector(
                    '.content-text-editor, .baca-block, .txt-article, .detail__body-text, '
                    + '.entry-content, article, .read__content, .post-content, .detail-text'
                );
                if (body) {
                    body.querySelectorAll(
                        'script, style, iframe, .video, .ads, .baca-juga, '
                        + '[id*="gliastudios"], [class*="gliastudios"], .video-wrap'
                    ).forEach(j => j.remove());
                    return body.innerText;
                }
                return "";
            }"""
            )
            return text.strip()
        except Exception:
            return ""

    def clean_article_text(self, text: str) -> str:
        clean = re.sub(
            r"(?i)(baca juga|baca selengkapnya|artikel terkait|simak juga|penulis:|editor:).*?\n",
            "\n",
            text,
        )
        clean = re.sub(r"\n+", "\n", clean)
        return clean.strip()

    def smart_truncate(self, text: str) -> str:
        if len(text) <= 1500:
            return text
        first_chunk = text[:800]
        match = re.search(r"bandung", text[800:].lower())
        if match:
            start = 800 + max(0, match.start() - 350)
            end = 800 + min(len(text) - 800, match.start() + 350)
            return first_chunk + "\n\n...[POTONGAN BUKTI LOKASI]...\n\n" + text[start:end]
        return text[:1500]

    # ─────────────────────────────────────────────
    # AI AUDITOR
    # ─────────────────────────────────────────────

    async def interrogate_with_llama(self, article_text: str) -> dict:
        truncated = self.smart_truncate(article_text)
        print("     [>] Memproses SLM Inference via Ollama lokal...")

        prompt = f"""
Lakukan audit investigatif pada teks berita berikut untuk kebutuhan data Badan Pusat Statistik (BPS).

ATURAN YURISDIKSI SANGAT PENTING:
1. Status Geofencing HARUS "Valid Kota Bandung" JIKA peristiwa riil (arus barang, logistik, harga pasar) terjadi secara fisik di Kota Bandung.
2. TOLAK BERITA (Geofencing: "Out of Jurisdiction" atau "Irrelevant Context") JIKA teks tersebut hanya berupa:
   - Lowongan kerja atau rekrutmen (meskipun di instansi terkait).
   - Opini, seminar, diskusi kampus, atau pernyataan akademis tanpa data empiris kejadian di Kota Bandung.
   - Berita harga nasional (Kemendag/Pusat) tanpa peninjauan kondisi pasar di Kota Bandung.

Kembalikan HANYA objek JSON dengan format berikut (tanpa teks tambahan apapun):
{{
  "status_geografi": "...",
  "entitas_ditemukan": [...],
  "indikator_perdagangan": "...",
  "anomali_atau_hidden_agenda": "...",
  "skor_relevansi_bps": 0
}}

Teks Berita:
{truncated}
"""
        payload = {"model": self.model_name, "prompt": prompt, "format": "json", "stream": False}
        try:
            response = await asyncio.to_thread(
                requests.post, self.ollama_url, json=payload, timeout=120
            )
            if response.status_code == 200:
                raw = response.json().get("response", "{}")
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    return {"Error": "Format SLM non-JSON", "status_geografi": "Unknown"}
            return {"Error": f"SLM Status {response.status_code}", "status_geografi": "Unknown"}
        except requests.exceptions.RequestException:
            return {"Error": "Ollama Timeout/Failure.", "status_geografi": "Unknown"}

    # ─────────────────────────────────────────────
    # OUTPUT
    # ─────────────────────────────────────────────

    def save_checkpoint(self):
        if not self.session_data:
            print("\n[!] Tidak ada data untuk disimpan sesi ini.")
            return
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = self.export_dir / f"bps_audit_{timestamp}.xlsx"
        df = pd.DataFrame(self.session_data)
        cols = [
            "Tanggal Terbit Publikasi", "Tanggal Ekstraksi", "Sumber", "Judul", "URL",
            "Status Geografi", "Entitas Terdeteksi", "Indikator Dagang", "Anomali",
            "Skor", "Teks",
        ]
        df = df[[c for c in cols if c in df.columns]]
        writer = pd.ExcelWriter(filename, engine="xlsxwriter")
        df.to_excel(writer, index=False, sheet_name="Audit SLM")
        worksheet = writer.sheets["Audit SLM"]
        url_idx = df.columns.get_loc("URL")
        for row_num, url in enumerate(df["URL"]):
            if pd.notna(url) and str(url).startswith("http"):
                worksheet.write_url(row_num + 1, url_idx, str(url), string="BACA ARTIKEL")
        writer.close()
        print(f"\n[✓] CHECKPOINT SECURED: {len(self.session_data)} laporan → {filename.name}")
        if os.name == "nt":
            os.startfile(os.path.abspath(filename))

    # ─────────────────────────────────────────────
    # RSS FETCH
    # ─────────────────────────────────────────────

    def _build_search_query(self, site: str) -> str:
        base = f'site:{site} "Kota Bandung" (ekspor OR impor OR "bea cukai" OR logistik)'
        if self.args.start:
            base += f" after:{self.args.start}"
        if self.args.end:
            base += f" before:{self.args.end}"
        return urllib.parse.quote(base)

    async def fetch_rss(self, site: str):
        query = self._build_search_query(site)
        rss_url = f"https://news.google.com/rss/search?q={query}&hl=id&gl=ID&ceid=ID:id"
        try:
            feed = await asyncio.to_thread(feedparser.parse, rss_url)
            return site, feed.entries
        except Exception:
            return site, []

    # ─────────────────────────────────────────────
    # ARTICLE WORKER
    # ─────────────────────────────────────────────

    async def process_article(self, context, entry, site: str):
        """Pekerja Asinkron — dibatasi Semaphore, dilindungi Lock."""
        async with self.browser_semaphore:
            real_url = self._decode_google_url(entry.link)

            # FIX: Lock melindungi seen_urls dari race condition
            async with self._lock:
                if real_url in self.seen_urls:
                    return
                self.seen_urls.add(real_url)
                self._new_urls_this_session.add(real_url)

            is_rejected, reject_reason = self.is_rejected_preflight(entry.title, real_url)
            if is_rejected:
                print(f"  [PREFLIGHT] {entry.title[:55]}... → {reject_reason}")
                return

            is_html, _ = await asyncio.to_thread(self.verify_mime_type, real_url)
            if not is_html:
                return

            published_date = entry.get("published", "Tanggal Tidak Tersedia")
            print(f"\n [>] Menyadap [{site}]: {entry.title[:50]}...")

            page = await context.new_page()
            await page.route("**/*", self.network_interceptor)

            try:
                try:
                    await page.goto(real_url, wait_until="domcontentloaded", timeout=30000)
                    await page.wait_for_timeout(2000)
                except TimeoutError:
                    pass

                await self.cloudflare_organic_wait(page)
                await self.execute_hydration_scroll(page)

                content = await page.content()
                cfg = Config()
                cfg.fetch_images = False
                art = Article(page.url, config=cfg, language="id")
                try:
                    art.set_html(content)
                    art.parse()
                    text_newspaper = art.text
                except Exception:
                    text_newspaper = ""

                text_native = await self.extract_text_native(page)
                raw_text = text_newspaper if len(text_newspaper) > len(text_native) else text_native
                purified = self.clean_article_text(raw_text)
                char_count = len(purified)

                is_valid, reason = self.is_relevant_lexical(entry.title, purified)

                if is_valid and char_count > 400:
                    audit = await self.interrogate_with_llama(purified)
                    status_geo = audit.get("status_geografi", "Unknown")
                    print(f"     [SLM] Geofencing: {status_geo}")

                    if "Out of Jurisdiction" not in status_geo and "Irrelevant Context" not in status_geo:
                        # FIX: Lock melindungi session_data dari race condition
                        async with self._lock:
                            self.session_data.append({
                                "Tanggal Terbit Publikasi": published_date,
                                "Tanggal Ekstraksi": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "Sumber": site,
                                "Judul": entry.title,
                                "URL": real_url,
                                "Status Geografi": status_geo,
                                "Entitas Terdeteksi": ", ".join(audit.get("entitas_ditemukan", [])),
                                "Indikator Dagang": audit.get("indikator_perdagangan", ""),
                                "Anomali": audit.get("anomali_atau_hidden_agenda", ""),
                                "Skor": audit.get("skor_relevansi_bps", 0),
                                "Teks": purified[:1500],
                            })
                        print("     [SECURED] Lolos audit BPS & SLM.")
                    else:
                        print(f"     [REJECTED] {status_geo}")
                else:
                    print(f"     [SKIPPED] {reason if not is_valid else f'Teks kerdil ({char_count} kar)'}")

            except Exception as e:
                if "TargetClosedError" not in str(e) and "has been closed" not in str(e):
                    print(f"     [ERROR] {e}")
            finally:
                if not page.is_closed():
                    await page.close()
                await asyncio.sleep(random.uniform(2.0, 4.0))

    # ─────────────────────────────────────────────
    # MAIN ORCHESTRATOR
    # ─────────────────────────────────────────────

    async def run(self):
        # FIX: await check_ollama (bukan sync call)
        if not await self.check_ollama():
            sys.exit(1)

        print("\n" + "=" * 75)
        print(" SURGICAL DEBUGGER V61 | THE SOVEREIGN SENTINEL")
        if self.args.start or self.args.end:
            print(f" Rentang Waktu: {self.args.start} hingga {self.args.end}")
        print("=" * 75)

        self.prepare_workspace()

        # FASE 1: Panen RSS secara paralel
        print("\n[RADAR] Mengumpulkan tautan dari seluruh sumber secara paralel...")
        rss_results = await asyncio.gather(*[self.fetch_rss(s) for s in self.sites])

        all_entries = []
        for site, entries in rss_results:
            all_entries.extend([(site, e) for e in entries[:5]])
        random.shuffle(all_entries)  # Acak agar tidak bias ke satu situs
        print(f"[RADAR] {len(all_entries)} target potensial. Memulai ekstraksi asinkron...")

        context = None
        try:
            async with async_playwright() as p:
                context = await p.chromium.launch_persistent_context(
                    user_data_dir=str(self.workspace_dir),
                    channel="msedge",
                    headless=False,
                    args=["--disable-blink-features=AutomationControlled"],
                )

                # FASE 2: Ekstraksi paralel dibatasi Semaphore (maks 2 tab)
                await asyncio.gather(
                    *[self.process_article(context, entry, site) for site, entry in all_entries]
                )

        except KeyboardInterrupt:
            print("\n\n[!] INTERUPSI (CTRL+C) TERDETEKSI. Mengamankan data...")
        except Exception as e:
            print(f"\n[FATAL ERROR]: {e}")
        finally:
            self.save_checkpoint()
            self._save_visited_urls()  # Simpan hanya URL baru (delta)
            if context:
                try:
                    await context.close()
                except Exception:
                    pass
            sys.exit(0)


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BPS Investigative Scraper V61")
    parser.add_argument("--mode", type=str, default="live", help="Mode eksekusi (live/history)")
    parser.add_argument("--start", type=str, default="", help="Format: YYYY-MM-DD")
    parser.add_argument("--end", type=str, default="", help="Format: YYYY-MM-DD")
    args = parser.parse_args()

    asyncio.run(BPS_Sovereign_Sentinel(args).run())
