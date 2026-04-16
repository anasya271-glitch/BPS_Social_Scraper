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

class BPS_Absolute_Sentinel:
    """
    BMEIS (Bandung Municapality's Export and Import Phenomenon Scraper | v.65
    Modul aktif: Isolated Log Buffer, Aggressive Lexical Filter, Absolute SLM Prompt, Deterministic Caching.
    """
    def __init__(self, args):
        self.args = args
        self.sites = [
            "bandung.go.id",
            "tempo.co", "tirto.id", "narasi.tv", "ayobandung.com", "pikiran-rakyat.com", 
            "bandung.kompas.com", "disdagin.bandung.go.id",
            "radarbandung.id", "kumparan.com", "cnnindonesia.com",
            "rri.co.id", "infobandungkota.com", "prfmnews.id", 
            "kilasbandungnews.com", "bandungbergerak.id", "koranmandala.com", 
            "jabarekspres.com", "jabarprov.go.id", "jabar.tribunnews.com"
        ]
        
        self.edge_source_dir = str(Path.home() / "AppData" / "Local" / "Microsoft" / "Edge" / "User Data")
        self.workspace_dir = Path.cwd() / "data" / "edge_workspace"
        self.export_dir = Path.cwd() / "data" / "exports"
        self.visited_file = Path.cwd() / "visited_urls.txt"
        
        os.makedirs(self.workspace_dir, exist_ok=True)
        os.makedirs(self.export_dir, exist_ok=True)
        
        self.session_data = []
        
        self.permanent_visited_urls = set()
        self.session_active_urls = set()
        self.new_urls_to_save = []
        self.state_lock = asyncio.Lock()
        self.print_lock = asyncio.Lock()
        
        self.load_visited_urls()
        
        self.ollama_url = "http://localhost:11434/api/generate"
        self.model_name = "bps-auditor" 
        
        self.browser_semaphore = asyncio.Semaphore(2)
        
        self.config = {
            "GEOGRAPHY": {
                "STRICT_ANCHORS": [
                    r"\bkota[\s\-]?bandung\b", r"\bpemkot[\s\-]?bandung\b", 
                    r"\bwali[\s\-]?kota[\s\-]?bandung\b", r"\bbandung[\s\-]?kota\b", r"\bdprd[\s\-]?kota[\s\-]?bandung\b",
                    r"\bdisdagin[\s\-]?kota[\s\-]?bandung\b", r"\bdkpp[\s\-]?kota[\s\-]?bandung\b"
                ],
                "DISTRICTS": [
                    r"\bandir\b", r"\bastana[\s\-]?anyar\b", r"\bantapani\b", r"\barcamanik\b", r"\bbabakan[\s\-]?ciparay\b", 
                    r"\bbandung[\s\-]?kidul\b", r"\bbandung[\s\-]?kulon\b", r"\bbandung[\s\-]?wetan\b", r"\bbatununggal\b", 
                    r"\bbojongloa[\s\-]?kaler\b", r"\bbojongloa[\s\-]?kidul\b", r"\bbuah[\s\-]?batu\b", r"\bcibeunying\b", 
                    r"\bcibiru\b", r"\bcicendo\b", r"\bcidadap\b", r"\bcinambo\b", r"\bcoblong\b", 
                    r"\bgedebage\b", r"\bkiara[\s\-]?condong\b", r"\blengkong\b", r"\bmandalajati\b", 
                    r"\bpanyileukan\b", r"\brancasari\b", r"\bregol\b", r"\bsukajadi\b", r"\bsukasari\b", 
                    r"\bsumur[\s\-]?bandung\b", r"\bujung[\s\-]?berung\b",
                    r"\bcibaduyut\b", r"\bpasar[\s\-]?baru\b", r"\bsuci\b", r"\bcigondewah\b", r"\bbinong\b"
                ],
                "BLACKLIST": [
                    r"\bkabupaten[\s\-]?bandung\b", r"\bbupati\b", r"\bsoreang\b", r"\bkbb\b", 
                    r"\bbandung[\s\-]?barat\b", r"\blembang\b", r"\bcimahi\b", r"\bmajalaya\b", 
                    r"\bpangalengan\b", r"\bciwidey\b", r"\bpadalarang\b", r"\bpemkab\b",
                    r"\bbekasi\b", r"\bbogor\b", r"\bdepok\b", r"\bkarawang\b", r"\bpurwakarta\b",
                    r"\bsukabumi\b", r"\bciamis\b", r"\btasikmalaya\b", r"\bgarut\b", r"\bcirebon\b",
                    r"\bcianjur\b", r"\bindramayu\b", r"\bmajalengka\b", r"\bsumedang\b", r"\bsubang\b", r"\bkuningan\b",
                    r"\bjakarta\b", r"\bdki jakarta\b", r"\bjawa tengah\b", r"\bjateng\b", r"\bsemarang\b",
                    r"\bjawa timur\b", r"\bjatim\b", r"\bsurabaya\b", r"\bmalang\b", r"\bbanten\b", r"\btangerang\b",
                    r"\byogyakarta\b", r"\bdiy\b", r"\bjogja\b", r"\bbali\b", r"\bdenpasar\b",
                    r"\bsumatera\b", r"\bsumut\b", r"\bmedan\b", r"\bsumbar\b", r"\bpadang\b", r"\bsumsel\b", r"\bpalembang\b",
                    r"\briau\b", r"\bbatam\b", r"\blampung\b", r"\bbengkulu\b", r"\bjambi\b", r"\baceh\b",
                    r"\bkalimantan\b", r"\bkalbar\b", r"\bpontianak\b", r"\bkaltim\b", r"\bbalikpapan\b", r"\bsamarinda\b", 
                    r"\bkalsel\b", r"\bbanjarmasin\b", r"\bkalteng\b", r"\bpalangkaraya\b",
                    r"\bsulawesi\b", r"\bsulsel\b", r"\bmakassar\b", r"\bsulut\b", r"\bmanado\b", r"\bsulteng\b", r"\bpalu\b",
                    r"\bpapua\b", r"\bmaluku\b", r"\bambon\b", r"\bntb\b", r"\bmataram\b", r"\bntt\b", r"\bkupang\b"
                ]
            },
            "TRADE_FLUX": {
                "INTERNASIONAL": [
                    r"\bekspor\b", r"\bimpor\b", r"\bpasar[\s\-]?global\b", r"\bpasar[\s\-]?internasional\b", 
                    r"\bmancanegara\b", r"\bluar[\s\-]?negeri\b", r"\bbea[\s\-]?cukai\b", r"\bkite[\s\-]?ikm\b"
                ],
                "ANTAR_DAERAH": [
                    r"\bpasokan\b", r"\bsuplai\b", r"\brantai[\s\-]?pasok\b", r"\blogistik\b", 
                    r"\bdistribusi\b", r"\bantar[\s\-]?daerah\b", r"\bantar[\s\-]?provinsi\b", r"\bkontainer\b", r"\bdry[\s\-]?port\b"
                ],
                "INDICATORS": [
                    r"\bkelangkaan\b", r"\bkenaikan[\s\-]?harga\b", r"\bstok\b", r"\bfluktuasi\b", 
                    r"\bharga[\s\-]?eceran\b", r"\bhet\b", r"\binflasi\b", r"\bdefisit\b", r"\bsurplus\b"
                ]
            },
            "COMMODITIES": [
                r"\bberas\b", r"\bjagung\b", r"\bminyak[\s\-]?goreng\b", r"\bgula\b", r"\bterigu\b", r"\bkedelai\b",
                r"\bcabai\b", r"\bbawang\b", r"\bsayur\b", r"\bdaging\b", r"\btelur\b", r"\bpakaian\b", r"\bgarmen\b", 
                r"\bsepatu\b", r"\btekstil\b", r"\bsemen\b", r"\belpiji\b", r"\bbbm\b", r"\bkopi\b", r"\bkakao\b", r"\bkosmetik\b"
            ],
            "NOISE_WORDS": [
                r"\bpiala\b", r"\bliga\b", r"\bgempa\b", r"\bkecelakaan\b", r"\bpembunuhan\b", r"\bpersib\b", 
                r"\bskandal\b", r"\bpilkada\b", r"\bkampanye\b", r"\bcapres\b", r"\bcawalkot\b", r"\bpartai\b",
                r"\bhakim\b", r"\bperadilan\b", r"\bnarkoba\b", r"\btiket\b", r"\bkonser\b", r"\bsurat[\s\-]?suara\b",
                r"\bwisatawan\b", r"\bkuliner\b", r"\bbpbd\b", r"\bbencana\b", r"\bcuaca ekstrem\b", 
                r"\bpohon tumbang\b", r"\bhujan deras\b", r"\bkpu\b", r"\bbawaslu\b", r"\blogistik pemilu\b", 
                r"\bbunuh diri\b", r"\bpengeroyokan\b",
                r"\bcpns\b", r"\blowongan\b", r"\bloker\b", r"\brekrutmen\b",
                r"\bkampus\b", r"\buniversitas\b", r"\binstitut\b", r"\bpoliteknik\b", r"\bakademi\b", r"\bsekolah tinggi\b",
                r"\buin\b", r"\bitb\b", r"\bunpas\b", r"\bunpad\b", r"\bupi\b", r"\btelkom university\b", r"\bunpar\b", r"\bunisba\b",
                r"\bmahasiswa\b", r"\bdosen\b", r"\bguru besar\b", r"\brektor\b", r"\bdekan\b", 
                r"\bwisuda\b", r"\bdies natalis\b", r"\bsnbt\b", r"\bppdb\b", r"\bkemendikdasmen\b",
                r"\bseminar\b", r"\bwebinar\b", r"\bsimposium\b", r"\blokakarya\b", r"\bkonferensi\b", 
                r"\bsidang terbuka\b", r"\bskripsi\b", r"\btesis\b", r"\bdisertasi\b", r"\bsman\b", r"\bsmpn\b", 
                r"\bpramuka\b", r"\bjambore\b",
                r"\bmobil dinas\b", r"\bkendaraan dinas\b", r"\baparatur sipil negara\b", r"\basn\b", r"\bwfh\b", 
                r"\bapel pagi\b", r"\bmutasi jabatan\b", r"\brotasi jabatan\b", r"\bdisiplin pegawai\b", r"\bkinerja asn\b"
            ],
            "TITLE_BLACKLIST": [
                "pegawai", "sejarah", "visi", "misi", "tupoksi", "kontak", "gallery", 
                "profil", "rencana strategis", "bab i", "powerpoint", "open data", 
                "loker", "jurnal", "pengumuman", "layanan", "jadwal", "detail", "slidefabric",
                "kode pos", "beranda", "home", "indeks", "index", "kategori", "registrasi imei"
            ],
            "DOCUMENT_EXTENSIONS": [
                ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx"
            ]
        }

    def load_visited_urls(self):
        if self.visited_file.exists():
            with open(self.visited_file, "r", encoding="utf-8") as f:
                for line in f:
                    url = line.strip()
                    if url:
                        self.permanent_visited_urls.add(url)
        print(f" [>] Mengamankan cache historis: {len(self.permanent_visited_urls)} URL tersimpan di memori lokal.")

    async def _commit_to_permanent_blacklist(self, url):
        async with self.state_lock:
            if url not in self.permanent_visited_urls:
                self.permanent_visited_urls.add(url)
                self.new_urls_to_save.append(url)

    def _save_visited_urls_delta(self):
        if not self.new_urls_to_save: return
        with open(self.visited_file, "a", encoding="utf-8") as f:
            for url in self.new_urls_to_save:
                f.write(url + "\n")

    def prepare_workspace(self):
        print(" [>] Sinkronisasi Ruang Isolasi Browser...")
        source = Path(self.edge_source_dir) / "Default"
        target = self.workspace_dir / "Default"
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)
        try:
            shutil.copytree(source, target, ignore=shutil.ignore_patterns("SingletonLock", "lock"))
        except: pass

    def check_ollama(self):
        try:
            r = requests.get("http://localhost:11434/api/version", timeout=3)
            if r.status_code == 200:
                print("     [✓] Ollama Server (Local AI) siap beroperasi.")
                return True
        except:
            print("     [!!!] Ollama TIDAK TERDETEKSI. Harap eksekusi `ollama serve` di terminal terpisah.")
            return False

    def format_hyperlink(self, url, text):
        return f"\033]8;;{url}\033\\{text}\033]8;;\033\\"

    def _decode_google_url(self, url):
        real_url = url
        if "articles/CBM" in url:
            try:
                encoded_str = url.split("articles/")[1].split("?")[0]
                padding = 4 - (len(encoded_str) % 4)
                encoded_str += "=" * padding
                decoded_bytes = base64.urlsafe_b64decode(encoded_str)
                match = re.search(rb'(https?://[a-zA-Z0-9\-\.\_\/\?\=\&\%\+]+)', decoded_bytes)
                if match: real_url = match.group(1).decode('utf-8')
            except Exception: pass
        
        if any(domain in real_url for domain in ["tribunnews.com", "pikiran-rakyat.com", "ayobandung.com", "kompas.com", "tirto.id"]):
            parsed = urlparse(real_url)
            params = parse_qs(parsed.query)
            params['page'] = ['all']
            new_query = urlencode(params, doseq=True)
            real_url = urlunparse(parsed._replace(query=new_query))
            
        return real_url

    def is_rejected_preflight(self, title, url):
        title_lower = title.lower()
        url_lower = url.lower()
        combined = f"{title} {url}".lower()
        
        has_strong_anchor = any(re.search(a, title_lower) for a in self.config["GEOGRAPHY"]["STRICT_ANCHORS"])
        has_trade_intl = any(re.search(t, title_lower) for t in self.config["TRADE_FLUX"]["INTERNASIONAL"])
        has_trade_dom = any(re.search(t, title_lower) for t in self.config["TRADE_FLUX"]["ANTAR_DAERAH"] + self.config["TRADE_FLUX"]["INDICATORS"])
        has_commodity = any(re.search(c, title_lower) for c in self.config["COMMODITIES"])
        has_trade = has_trade_intl or (has_trade_dom and has_commodity)
        
        if any(b in title_lower for b in self.config["TITLE_BLACKLIST"]): 
            return True, "Halaman Statis/Administratif"
        for ext in self.config["DOCUMENT_EXTENSIONS"]:
            if url_lower.endswith(ext) or (ext + "?" in url_lower): 
                return True, "Ekstensi Dokumen Non-Naratif"

        is_noise = any(re.search(n, title_lower) for n in self.config["NOISE_WORDS"]) or any(re.search(n, combined) for n in self.config["NOISE_WORDS"])
        if is_noise and not (has_strong_anchor and has_trade): 
            return True, "Terdeteksi Noise Konteks (Akademis/Kriminal/Bencana/Birokrasi)"
        
        is_blacklisted = any(re.search(b, title_lower) for b in self.config["GEOGRAPHY"]["BLACKLIST"])
        if is_blacklisted and not has_strong_anchor: 
            return True, "Terdeteksi Wilayah Tetangga/Provinsi Lain (Tanpa Anchor Kuat)"
        
        return False, "Aman"

    def verify_mime_type(self, url):
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        for attempt in range(2):
            try:
                with requests.get(url, stream=True, headers=headers, timeout=5) as response:
                    content_type = response.headers.get('Content-Type', '').lower()
                    if 'application/' in content_type and 'xhtml' not in content_type:
                        return False, f"Format Dokumen Ditolak ({content_type})"
                    return True, "Valid HTML"
            except requests.exceptions.Timeout:
                if attempt == 1: return True, "MIME Timeout"
            except: return True, "MIME Error"
        return True, "MIME Error"

    async def network_interceptor(self, route):
        url = route.request.url.lower()
        if any(ad in url for ad in ["doubleclick.net", "googlesyndication.com", "ads-twitter.com", "gliastudios.com", "seedtag.com"]):
            await route.abort()
            return
        await route.continue_()

    async def execute_hydration_scroll(self, page):
        try:
            viewport_height = await page.evaluate("window.innerHeight")
            for _ in range(5):
                await page.evaluate(f"window.scrollBy(0, {viewport_height * 0.7})")
                await page.wait_for_timeout(800) 
            await page.evaluate("window.scrollTo(0, 0)")
            await page.wait_for_timeout(1000)
        except: pass

    async def extract_text_native(self, page):
        try:
            text = await page.evaluate('''() => {
                let articleBody = document.querySelector('.content-text-editor, .baca-block, .txt-article, .detail__body-text, .entry-content, article, .read__content, .post-content, .detail-text');
                if (articleBody) {
                    let junks = articleBody.querySelectorAll('script, style, iframe, .video, .ads, .baca-juga, [id*="gliastudios"], [class*="gliastudios"], .video-wrap');
                    junks.forEach(j => j.remove());
                    return articleBody.innerText;
                }
                return "";
            }''')
            return text.strip()
        except: return ""

    def clean_article_text(self, text):
        clean_text = re.sub(r'(?i)(baca juga|baca selengkapnya|artikel terkait|simak juga|penulis:|editor:).*?\n', '\n', text)
        clean_text = re.sub(r'\n+', '\n', clean_text)
        return clean_text.strip()

    def is_relevant_lexical(self, title, text):
        combined = f"{title} {text}".lower()
        has_strong_anchor = any(re.search(g, combined) for g in self.config["GEOGRAPHY"]["STRICT_ANCHORS"])
        has_strict_geo = has_strong_anchor or any(re.search(d, combined) for d in self.config["GEOGRAPHY"]["DISTRICTS"])

        has_trade_intl = any(re.search(t, combined) for t in self.config["TRADE_FLUX"]["INTERNASIONAL"])
        has_trade_domestic = any(re.search(t, combined) for t in self.config["TRADE_FLUX"]["ANTAR_DAERAH"] + self.config["TRADE_FLUX"]["INDICATORS"])
        has_commodity = any(re.search(c, combined) for c in self.config["COMMODITIES"])
        has_trade = has_trade_intl or (has_trade_domestic and has_commodity)

        is_noise = any(re.search(n, combined) for n in self.config["NOISE_WORDS"])
        
        if is_noise and not (has_strong_anchor and has_trade): 
            return False, "Terdeteksi Noise Akademis/Kriminal/Bencana/Birokrasi (Tanpa Anchor Dagang Kuat)"
        
        is_blacklisted = any(re.search(b, combined) for b in self.config["GEOGRAPHY"]["BLACKLIST"])
        if is_blacklisted and not has_strong_anchor: 
            return False, "Fokus ke Wilayah/Provinsi Lain (Tanpa Entitas Kuat Kota Bandung)"

        if not has_strict_geo: 
            return False, "Gagal Geofencing (Tidak eksplisit menyebut Kota Bandung)"

        if has_trade: 
            return True, "Lolos Leksikal: Perdagangan/Logistik Komoditas"
            
        return False, "Lolos Geografi, namun miskin indikator BPS"

    def smart_truncate(self, text):
        text_lower = text.lower()
        if len(text) <= 1500: return text
            
        first_chunk = text[:800]
        match = re.search(r'bandung', text_lower[800:])
        if match:
            start_idx = 800 + max(0, match.start() - 350)
            end_idx = 800 + min(len(text_lower[800:]), match.start() + 350)
            second_chunk = text[start_idx:end_idx]
            return first_chunk + "\n\n...[POTONGAN BUKTI LOKASI]...\n\n" + second_chunk
        else:
            return text[:1500]

    async def interrogate_with_llama(self, article_text, task_log):
        truncated_text = self.smart_truncate(article_text)
        task_log.append("     [>] Mengirim Smart Context Window ke SLM (Ollama)...")
        
        custom_prompt = f"""
        Lakukan audit investigatif pada teks berita berikut untuk kebutuhan Badan Pusat Statistik (BPS).
        Keluarkan format JSON MURNI dengan keys: "status_geografi", "entitas_ditemukan", "indikator_perdagangan", "anomali_atau_hidden_agenda", "skor_relevansi_bps".
        
        ATURAN YURISDIKSI SANGAT PENTING (HUKUM ABSOLUT):
        1. Status Geofencing HARUS "Valid Kota Bandung" HANYA JIKA peristiwa riil (arus barang, logistik, inflasi pasar) terjadi secara fisik di Kota Bandung.
        2. TOLAK BERITA (Geofencing: "Out of Jurisdiction" atau "Irrelevant Context") JIKA:
           - Berita berupa kebijakan administratif internal instansi (seperti aturan penggunaan mobil dinas, jam kerja, WFH, apel pagi, disiplin ASN) yang tidak memengaruhi ketersediaan barang di pasar.
           - Berupa opini, seminar, diskusi kampus, dies natalis, atau pernyataan akademis tanpa data empiris kejadian di lapangan Kota Bandung.
           - Merupakan rilis data Kementerian/Pusat (harga nasional/SP2KP) tanpa mencantumkan kondisi lapangan (wawancara pedagang/harga spesifik) di pasar wilayah Kota Bandung.
        
        Teks Berita:
        {truncated_text}
        """
        
        payload = {
            "model": self.model_name,
            "prompt": custom_prompt,
            "format": "json",
            "stream": False
        }
        try:
            response = await asyncio.to_thread(requests.post, self.ollama_url, json=payload, timeout=120)
            if response.status_code == 200:
                raw_json = response.json().get("response", "{}")
                try: return json.loads(raw_json)
                except json.JSONDecodeError: return {"Error": "Format SLM non-JSON"}
            else: return {"Error": f"SLM Status {response.status_code}"}
        except requests.exceptions.RequestException:
            return {"Error": "Daemon Ollama tertidur / Port tertutup."}

    def save_checkpoint(self):
        if not self.session_data: return
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = self.export_dir / f"bps_audit_historis_{timestamp}.xlsx"
        df = pd.DataFrame(self.session_data)
        cols = ["Tanggal Terbit Publikasi", "Tanggal Ekstraksi", "Sumber", "Judul", "URL", "Status Geografi", "Entitas Terdeteksi", "Indikator Dagang", "Anomali", "Skor", "Teks"]
        df = df[[c for c in cols if c in df.columns]]

        writer = pd.ExcelWriter(filename, engine='xlsxwriter')
        df.to_excel(writer, index=False, sheet_name='Audit SLM')
        workbook = writer.book
        worksheet = writer.sheets['Audit SLM']
        url_idx = df.columns.get_loc("URL")
        for row_num, url in enumerate(df["URL"]):
            if pd.notna(url) and str(url).startswith("http"):
                worksheet.write_url(row_num + 1, url_idx, str(url), string="BACA ARTIKEL")
        writer.close()

    def _build_search_query(self, site):
        base_query = f'site:{site} "Kota Bandung" (ekspor OR impor OR "bea cukai" OR logistik)'
        if self.args.start: base_query += f' after:{self.args.start}'
        if self.args.end: base_query += f' before:{self.args.end}'
        return urllib.parse.quote(base_query)

    async def fetch_rss(self, site):
        query = self._build_search_query(site)
        rss_url = f"https://news.google.com/rss/search?q={query}&hl=id&gl=ID&ceid=ID:id"
        try:
            feed = await asyncio.to_thread(feedparser.parse, rss_url)
            return site, feed.entries
        except: return site, []

    async def process_article(self, context, entry, site, real_url):
        task_log = []
        link_text = self.format_hyperlink(real_url, "[BACA ARTIKEL]")
        
        async with self.browser_semaphore:
            async with self.state_lock:
                if real_url in self.session_active_urls:
                    return
                self.session_active_urls.add(real_url)

            is_rejected, reject_reason = self.is_rejected_preflight(entry.title, real_url)
            if is_rejected:
                task_log.append(f"\n -> {entry.title[:60]}...")
                task_log.append(f"    [BLOCKED PRE-FLIGHT] {reject_reason}. {link_text}")
                await self._commit_to_permanent_blacklist(real_url)
                
                async with self.print_lock: print("\n".join(task_log))
                return

            is_html, mime_reason = await asyncio.to_thread(self.verify_mime_type, real_url)
            if not is_html:
                task_log.append(f"\n -> {entry.title[:60]}...")
                task_log.append(f"    [BLOCKED] {mime_reason}. {link_text}")
                await self._commit_to_permanent_blacklist(real_url)
                
                async with self.print_lock: print("\n".join(task_log))
                return

            published_date = entry.get("published", "Tanggal Tidak Tersedia")
            task_log.append(f"\n [>] Mengekstraksi [{site}]: {entry.title[:50]}...")
            
            page = await context.new_page()
            await page.route("**/*", self.network_interceptor)
            
            pacing_type = "fast"
            try:
                try:
                    await page.goto(real_url, wait_until="domcontentloaded", timeout=30000)
                    await page.wait_for_timeout(2000)
                except TimeoutError: pass 
                    
                try:
                    iframe = await page.wait_for_selector('iframe[src*="cloudflare"], #challenge-running', timeout=4000)
                    if iframe:
                        async with self.print_lock:
                            print(f"\a\n     [!!!] CLOUDFLARE TERDETEKSI pada {page.url[:40]}... Tahan & Centang Captcha!")
                        await page.wait_for_function("document.querySelector('iframe[src*=\"cloudflare\"]') === null && document.querySelector('#challenge-running') === null", timeout=60000)
                        await page.wait_for_timeout(3000)
                except: pass

                await self.execute_hydration_scroll(page)
                
                content = await page.content()
                config = Config()
                config.fetch_images = False
                article = Article(page.url, config=config, language='id')
                try:
                    article.set_html(content)
                    article.parse()
                    text_newspaper = article.text
                except: text_newspaper = ""
                
                text_native = await self.extract_text_native(page)
                
                raw_text = text_newspaper if len(text_newspaper) > len(text_native) else text_native
                purified_text = self.clean_article_text(raw_text)
                char_count = len(purified_text)
                
                is_valid, reason = self.is_relevant_lexical(entry.title, purified_text)
                
                if is_valid:
                    if char_count > 400:
                        task_log.append(f"     [+] {reason}. Integritas: {char_count} kar.")
                        
                        audit_result = await self.interrogate_with_llama(purified_text, task_log)
                        status_geo = audit_result.get("status_geografi", "Unknown")
                        task_log.append(f"     [SLM JUDGE] Geofencing: {status_geo}")
                        
                        if "Out of Jurisdiction" not in status_geo and "Irrelevant Context" not in status_geo:
                            self.session_data.append({
                                "Tanggal Terbit Publikasi": published_date,
                                "Tanggal Ekstraksi": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "Sumber": site,
                                "Judul": entry.title,
                                "URL": real_url, 
                                "Status Geografi": status_geo,
                                "Entitas Terdeteksi": ", ".join(audit_result.get("entitas_ditemukan", [])),
                                "Indikator Dagang": audit_result.get("indikator_perdagangan", ""),
                                "Anomali": audit_result.get("anomali_atau_hidden_agenda", ""),
                                "Skor": audit_result.get("skor_relevansi_bps", 0),
                                "Teks": purified_text[:1500] 
                            })
                            task_log.append(f"     [SECURED] Lolos audit BPS & SLM. {link_text}")
                            pacing_type = "normal" 
                            await self._commit_to_permanent_blacklist(real_url)
                        else:
                            task_log.append(f"     [REJECTED] SLM Menolak: {status_geo}. {link_text}")
                            pacing_type = "normal" 
                            await self._commit_to_permanent_blacklist(real_url)
                    else:
                        task_log.append(f"     [SKIPPED] Teks kerdil ({char_count} kar). Indikasi Paywall/Error. {link_text}")
                        pacing_type = "fast"
                else:
                    task_log.append(f"     [SKIPPED] {reason}. {link_text}")
                    pacing_type = "fast"
                    await self._commit_to_permanent_blacklist(real_url)

            except Exception as e:
                if "TargetClosedError" not in str(e) and "has been closed" not in str(e):
                    task_log.append(f"     [ERROR Ekstraksi] {e}. {link_text}")
                pacing_type = "fast"
            finally:
                if not page.is_closed(): await page.close()
                
                if task_log:
                    async with self.print_lock:
                        print("\n".join(task_log))
                
                if pacing_type == "normal":
                    await asyncio.sleep(random.uniform(3.0, 5.0))
                else:
                    await asyncio.sleep(random.uniform(1.5, 2.5))

    async def run(self):
        if not self.check_ollama():
            sys.exit(1)

        print("\n" + "="*75)
        print(" BMEIS (Bandung Municapality's Export and Import Phenomenon Scraper | v.65) ")
        if self.args.start or self.args.end:
            print(f" Rentang Waktu: {self.args.start} hingga {self.args.end}")
        print("="*75)

        self.prepare_workspace()
        
        print("\n[RADAR] Mengumpulkan heuristik intelijen dari seluruh sumber secara paralel...")
        rss_tasks = [self.fetch_rss(site) for site in self.sites]
        rss_results = await asyncio.gather(*rss_tasks)
        
        all_entries = []
        for site, entries in rss_results:
            all_entries.extend([(site, e) for e in entries[:15]]) 
            
        new_targets = []
        cached_count = 0
        for site, entry in all_entries:
            real_url = self._decode_google_url(entry.link)
            if real_url in self.permanent_visited_urls:
                cached_count += 1
            else:
                new_targets.append((site, entry, real_url))
                
        print(f"[RADAR] Menemukan {len(all_entries)} target potensial ({cached_count} sudah diaudit sebelumnya, {len(new_targets)} target baru).")
        
        if not new_targets:
            print("[✓] Semua target hari ini sudah diekstraksi. Menutup sistem dengan senyap.")
            sys.exit(0)
            
        print("[RADAR] Memulai pembedahan asinkron untuk target baru...\n")

        context = None
        try:
            async with async_playwright() as p:
                context = await p.chromium.launch_persistent_context(
                    user_data_dir=str(self.workspace_dir), 
                    channel="msedge", 
                    headless=False,
                    args=["--disable-blink-features=AutomationControlled"]
                )

                tasks = [self.process_article(context, entry, site, real_url) for site, entry, real_url in new_targets]
                await asyncio.gather(*tasks)

        except KeyboardInterrupt:
            print("\n\n[!] INTERUPSI (CTRL+C) TERDETEKSI. Mengamankan data dengan tenang...")
        except Exception as e:
            print(f"\n[FATAL ERROR]: {e}")
        finally:
            self.save_checkpoint()
            self._save_visited_urls_delta()
            if context:
                try: await context.close()
                except: pass
            sys.exit(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BPS Investigative Scraper")
    parser.add_argument('--mode', type=str, default='live', help='Mode eksekusi (live/history)')
    parser.add_argument('--start', type=str, default='', help='Format: YYYY-MM-DD')
    parser.add_argument('--end', type=str, default='', help='Format: YYYY-MM-DD')
    args = parser.parse_args()
    
    asyncio.run(BPS_Absolute_Sentinel(args).run())