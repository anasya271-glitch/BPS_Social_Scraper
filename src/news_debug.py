import os
import asyncio
import feedparser
import re
import urllib.parse
import base64
import pandas as pd
import random
from datetime import datetime
from urllib.parse import urlparse
from pathlib import Path
from newspaper import Article, Config
from playwright.async_api import async_playwright, TimeoutError
import warnings
import aiohttp
import json
from email.utils import parsedate_to_datetime # Untuk parsing format waktu RSS

warnings.filterwarnings("ignore")

class BPSProxyValidator:
    """
    Surgical Debugger V31: The Proxy Validator
    Mendeteksi fenomena perdagangan melalui Proxy Indicators, Logistical Chokepoints, dan Industrial Friction.
    """
    def __init__(self):
        self.sites = [
            # "tempo.co/tag/kota-bandung", "narasi.tv/tags/kota-bandung", "bandung.kompas.com", 
            # "kabarbandung.pikiran-rakyat.com", "radarbandung.id", "bandung.go.id", 
            # "kumparan.com/topic/bandung", "cnnindonesia.com/tag/bandung",
            "tirto.id/q/bandung-pU" 
            #"rri.co.id/bandung", "infobandungkota.com", "ayobandung.com", "prfmnews.id", 
            #"kilasbandungnews.com", "bandungbergerak.id", "koranmandala.com", 
            #"jabarekspres.com", "jabar.tribunnews.com"
        ]
        self.edge_source_dir = r"C:\Users\MYPC PRO L7\AppData\Local\Microsoft\Edge\User Data"
        self.profile_name = "Default" 
        
        self.workspace_dir = Path.cwd() / "data" / "edge_workspace"
        self.export_dir = Path.cwd() / "data" / "exports"
        os.makedirs(self.workspace_dir, exist_ok=True)
        os.makedirs(self.export_dir, exist_ok=True)
        
        self.session_data = []

        # Kamus Leksikal V31: Investigative Proxy Mapping
        self.config = {
            "GEOGRAPHY": {
                "STRICT_ANCHORS": [r"\bkota[\s\-]?bandung\b", r"\bpemkot[\s\-]?bandung\b", r"\bwali[\s\-]?kota[\s\-]?bandung\b"],
                "DISTRICTS": [
                    r"\bandir\b", r"\bastana[\s\-]?anyar\b", r"\bantapani\b", r"\barcamanik\b", r"\bbabakan[\s\-]?ciparay\b", 
                    r"\bbandung[\s\-]?kidul\b", r"\bbandung[\s\-]?kulon\b", r"\bbandung[\s\-]?wetan\b", r"\bbatununggal\b", 
                    r"\bbojongloa[\s\-]?kaler\b", r"\bbojongloa[\s\-]?kidul\b", r"\bbuah[\s\-]?batu\b", r"\bcibeunying\b", 
                    r"\bcibiru\b", r"\bcicendo\b", r"\bcidadap\b", r"\bcinambo\b", r"\bcoblong\b", 
                    r"\bgedebage\b", r"\bkiara[\s\-]?condong\b", r"\blengkong\b", r"\bmandalajati\b", 
                    r"\bpanyileukan\b", r"\brancasari\b", r"\bregol\b", r"\bsukajadi\b", r"\bsukasari\b", 
                    r"\bsumur[\s\-]?bandung\b", r"\bujung[\s\-]?berung\b", r"\bpasar baru\b", r"\bcibaduyut\b"
                ],
                "BLACKLIST": [r"\bkabupaten[\s\-]?bandung\b", r"\bbupati\b", r"\bsoreang\b", r"\bkbb\b", r"\bbandung[\s\-]?barat\b", r"\blembang\b"]
            },
            "TRADE_FLUX": {
                "INTERNASIONAL": [r"\bekspor\b", r"\bimpor\b", r"\bpasar global\b", r"\bmancanegara\b", r"\bluar negeri\b"],
                "ANTAR_DAERAH": [r"\bpasokan\b", r"\bsuplai\b", r"\bdistribusi\b", r"\brantai pasok\b", r"\blogistik\b", r"\bdikirim ke\b"],
                "INDICATORS": [
                    r"\bharga\b.{0,30}(?:naik|melonjak|meroket|melambung|turun|anjlok|mahal|murah)\b",
                    r"(?:kenaikan|penurunan|lonjakan)\b.{0,30}\bharga\b",
                    r"\bkelangkaan\b", r"\bstok\b", r"\bfluktuasi\b", r"\bharga eceran\b", r"\bhet\b", r"\binflasi\b", r"\bdefisit\b", r"\bsurplus\b"
                ],
                # PROXY INVESTIGATIF BARU
                "LOGISTICS_PROXY": [r"\bkontainer\b", r"\bgudang\b", r"\bpelabuhan\b", r"\bbea cukai\b", r"\bdry port\b", r"\bkargo\b", r"\bbongkar muat\b"],
                "INDUSTRIAL_FRICTION": [r"\bphk\b", r"\bpabrik tutup\b", r"\bgulung tikar\b", r"\bsepi pembeli\b", r"\bpesanan turun\b", r"\bdaya beli\b"],
                "UNDERGROUND_ECONOMY": [r"\bthrifting\b", r"\bbaju bekas\b", r"\bilegal\b", r"\bpenyelundupan\b", r"\bsitaan\b", r"\bpemusnahan\b"]
            },
            "COMMODITIES": [
                r"\bberas\b", r"\bjagung\b", r"\bminyak[\s\-]?goreng\b", r"\bgula\b", r"\bterigu\b", r"\bkedelai\b",
                r"\bcabai\b", r"\bbawang\b", r"\bsayur\b", r"\bdaging\b", r"\btelur\b", r"\btahu\b", r"\btempe\b",
                r"\bplastik\b", r"\bkemasan\b", r"\bpakaian\b", r"\btekstil\b", r"\bgarmen\b", r"\bsepatu\b", r"\bsemen\b", r"\belpiji\b", r"\bbbm\b"
            ],
            "NOISE_WORDS": [
                r"\bjokowi\b", r"\bpiala\b", r"\bliga\b", r"\bgempa\b", r"\bkecelakaan\b", 
                r"\bpembunuhan\b", r"\bskandal\b", r"\bsepakbola\b", r"\bpersib\b", r"\bdpr\b", r"\bkpk\b", 
                r"\bptun\b", r"\bpanglima\b", r"\bpolri\b", r"\bsnbt\b", r"\bunpad\b", r"\bkemendikdasmen\b", 
                r"\bhakim\b", r"\bperadilan\b", r"\bfilm\b", r"\bimsakiyah\b", r"\bnu\b", r"\bkonser\b", 
                r"\btiket\b", r"\bevent\b", r"\bband\b", r"\bmusik\b", r"\bcampus\b", r"\bpilkada\b", r"\bkampanye\b"
            ]
        }

    async def evaluate_with_slm(self, title, text):
        """
        Semantic Extraction menggunakan Local SLM via Ollama.
        """
        # Potong teks menjadi 1500 karakter awal untuk menghemat Context Window & VRAM
        truncated_text = text[:1500] 
        
        prompt = f"""
        Anda adalah Auditor Investigatif dan Ahli Statistik. Evaluasi artikel berita ini secara ketat.
        
        Aturan Lolos (HARUS MEMENUHI SEMUA):
        1. Membahas aktivitas ekonomi MAKRO (Ekspor, Impor, Logistik Skala Besar, Distribusi Komoditas Antar-Daerah). ABAIKAN harga eceran, pedagang kecil, atau ritel.
        2. Lokasi insiden ATAU entitas yang terdampak HARUS SECARA SPESIFIK berada di "Kota Bandung" (Abaikan jika fokusnya di Kabupaten Bandung, Garut, Bogor, atau sekadar level Jawa Barat).
        
        Judul: {title}
        Teks: {truncated_text}
        
        Jawab HANYA menggunakan JSON valid dengan format: {{"relevan": true/false, "alasan": "satu kalimat penjelasan teknis mengapa lolos/ditolak"}}
        """

        url = "http://localhost:11434/api/generate"
        payload = {
            "model": "llama3", # Ubah ke 'phi3' jika menggunakan model phi3
            "prompt": prompt,
            "format": "json",
            "stream": False,
            "options": {"temperature": 0.0} # Nol untuk analisis deterministik (tanpa halusinasi)
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        data = await response.json()
                        result = json.loads(data['response'])
                        return result.get('relevan', False), result.get('alasan', "Parsing error")
        except Exception as e:
            return False, f"SLM Connection Error: {e}"

    def prepare_workspace(self):
        os.system('taskkill /F /IM msedge.exe /T >nul 2>&1')
        source = Path(self.edge_source_dir) / self.profile_name
        target = self.workspace_dir / self.profile_name
        os.system(f'robocopy "{source}" "{target}" /E /XF SingletonLock lock /R:1 /W:1 >nul 2>&1')

    def format_hyperlink(self, url, text):
        return f"\033]8;;{url}\033\\{text}\033]8;;\033\\"

    def _decode_google_url(self, url):
        if "articles/CBM" in url:
            try:
                encoded_str = url.split("articles/")[1].split("?")[0]
                padding = 4 - (len(encoded_str) % 4)
                encoded_str += "=" * padding
                decoded_bytes = base64.urlsafe_b64decode(encoded_str)
                match = re.search(rb'(https?://[a-zA-Z0-9\-\.\_\/\?\=\&\%\+]+)', decoded_bytes)
                if match:
                    return match.group(1).decode('utf-8')
            except Exception:
                pass
        return url

    async def network_interceptor(self, route):
        url = route.request.url
        if route.request.resource_type == "image":
            await route.abort()
            return
            
        ad_domains = ["doubleclick.net", "googlesyndication.com", "ads-twitter.com", "insurads.com", "richaudience.com", "googleads"]
        if any(ad in url for ad in ad_domains):
            await route.abort()
            return
            
        await route.continue_()

    async def manual_cloudflare_bypass(self, page):
        try:
            iframe = await page.wait_for_selector('iframe[src*="cloudflare"]', timeout=5000)
            if iframe:
                print("\a", end="") 
                print("     [!] INTERVENSI DIBUTUHKAN: Silakan centang Cloudflare di peramban!")
                await page.wait_for_function("document.querySelector('iframe[src*=\"cloudflare\"]') === null", timeout=60000)
                print("     [~] Akses diberikan. Melanjutkan operasi...")
                await page.wait_for_timeout(2000)
        except Exception:
            pass

    async def unhide_dom_elements(self, page):
        await page.evaluate("""
            const elements = document.querySelectorAll('*');
            elements.forEach(el => {
                const style = window.getComputedStyle(el);
                if (style.maxHeight !== 'none' || style.overflow === 'hidden') {
                    el.style.maxHeight = 'none';
                    el.style.overflow = 'visible';
                }
            });
        """)

    async def simulate_adaptive_scroll(self, page):
        viewport_height = await page.evaluate("window.innerHeight")
        for _ in range(8):
            await page.evaluate(f"window.scrollBy(0, {viewport_height * 0.8})")
            await page.wait_for_timeout(1000)
            is_bottom = await page.evaluate("(window.innerHeight + window.scrollY) >= (document.body.offsetHeight - 100)")
            if is_bottom:
                break
        await page.evaluate("window.scrollBy(0, -400)")
        await page.wait_for_timeout(1000)

    def is_content_strictly_relevant(self, title, text):
        combined_text = f"{title} {text}".lower()
        
        # 1. Hard Reject
        all_noise = self.config["NOISE_WORDS"] + self.config["GEOGRAPHY"]["BLACKLIST"]
        for noise in all_noise:
            if re.search(noise, combined_text):
                return False, f"Terdeteksi Noise/Blacklist: '{noise}'"

        # 2. Dual-Lock System Expanded
        has_flux_or_proxy = False
        matched_indicator = ""
        for category, patterns in self.config["TRADE_FLUX"].items():
            for pattern in patterns:
                if re.search(pattern, combined_text):
                    has_flux_or_proxy = True
                    matched_indicator = pattern
                    break
            if has_flux_or_proxy:
                break
                
        has_commodity = any(re.search(pattern, combined_text) for pattern in self.config["COMMODITIES"])
        has_geo = any(re.search(pattern, combined_text) for pattern in self.config["GEOGRAPHY"]["STRICT_ANCHORS"] + self.config["GEOGRAPHY"]["DISTRICTS"])
        
        if has_flux_or_proxy and (has_commodity or has_geo):
            return True, f"Lolos via indikator: {matched_indicator}"
            
        if has_flux_or_proxy:
            return False, "Ada Indikator, namun tidak mendeteksi Komoditas/Geo Bandung"
            
        return False, "Tidak memiliki Indikator Ekonomi/Proksi BPS"

    def save_to_excel(self):
        if not self.session_data:
            return print("\n[!] Resolusi Nol. Data Vault kosong.")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = self.export_dir / f"bps_ekspor_impor_{timestamp}.xlsx"
        
        df = pd.DataFrame(self.session_data)
        writer = pd.ExcelWriter(filename, engine='xlsxwriter')
        df.to_excel(writer, index=False, sheet_name='Data Statistik BPS')
        
        workbook  = writer.book
        worksheet = writer.sheets['Data Statistik BPS']
        
        header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
            worksheet.set_column(col_num, col_num, 20)
            
        url_col_idx = df.columns.get_loc('URL Berita')
        for row_num, url in enumerate(df['URL Berita']):
            worksheet.write_url(row_num + 1, url_col_idx, url, string="BACA ARTIKEL")
            
        writer.close()
        
        print(f"\n[!] DATA SECURED: {len(self.session_data)} Artikel bersih berhasil diarsipkan.")
        print("    Menjalankan Injeksi OS untuk memuat Excel...")
        
        try:
            abs_path = os.path.abspath(filename)
            os.startfile(abs_path)
        except Exception as e:
            print(f"    [Peringatan] Gagal memicu Excel otomatis: {e}")

    async def run(self):
        print("\n" + "="*75)
        print(" BPS SURGICAL DEBUGGER V31 | THE PROXY VALIDATOR")
        print("="*75)
        
        self.prepare_workspace()
        
        async with async_playwright() as p:
            context = await p.chromium.launch_persistent_context(
                user_data_dir=str(self.workspace_dir),
                channel="msedge",
                headless=False,
                args=["--disable-blink-features=AutomationControlled"]
            )

            for site in self.sites:
                print(f"\n[RADAR] Membuka Segel Target: {site}")
                
                # REVISI STRATEGIS: Memaksa Google mencari proksi spesifik, bukan sekadar "Bandung"
                query = urllib.parse.quote(f'site:{site} "Bandung" (ekspor OR impor OR pabrik OR gudang OR bea cukai OR phk OR kontainer OR thrifting)')
                rss_url = f"https://news.google.com/rss/search?q={query}&hl=id&gl=ID&ceid=ID:id"
                feed = await asyncio.to_thread(feedparser.parse, rss_url)

                if not feed.entries:
                    print("  [!] Tidak ada berita di portal ini.")
                    continue

                valid_count = 0
                target_quota = 10 # Diturunkan menjadi 2 per situs untuk efisiensi di 18 target
                
                print(f"  [>] Menelusuri tautan untuk mencari {target_quota} artikel valid...")

                for entry in feed.entries:
                    if valid_count >= target_quota:
                        break

                    real_url = self._decode_google_url(entry.link)
                    print(f"\n  -> {entry.title[:60]}...")
                    clickable_terminal = self.format_hyperlink(real_url, "[BACA ARTIKEL ASLI]")
                    print(f"     L {clickable_terminal}")
                    
                    page = await context.new_page()
                    await page.route("**/*", self.network_interceptor)
                    
                    try:
                        try:
                            await page.goto(real_url, wait_until="domcontentloaded", timeout=25000)
                        except TimeoutError:
                            pass
                            
                        await self.manual_cloudflare_bypass(page)
                        await self.unhide_dom_elements(page)
                        await self.simulate_adaptive_scroll(page)
                        
                        raw_html = await page.content()
                        
                        config = Config()
                        config.fetch_images = False 
                        article = Article(page.url, config=config, language='id')
                        article.set_html(raw_html)
                        article.parse()
                        
                        char_count = len(article.text)
                        
                        if char_count > 400:
                            # 1. Lexical Pre-filter (Kecepatan)
                            is_lexically_valid, lex_reason = self.is_content_strictly_relevant(entry.title, article.text)
                            
                            if is_lexically_valid:
                                print(f"     [!] Lolos Pre-Filter ({lex_reason}). Mengirim ke SLM Judge...")
                                # 2. Semantic Judge (Akurasi Contextual)
                                is_semantically_valid, slm_reason = await self.evaluate_with_slm(entry.title, article.text)
                                
                                if is_semantically_valid:
                                    valid_count += 1
                                    print(f"     [SUCCESS] SLM Approved: {slm_reason}. [Terkumpul: {valid_count}/{target_quota}]")
                                    
                                    # Ekstraksi Tanggal Berita Asli dari RSS Feed
                                    try:
                                        pub_date = parsedate_to_datetime(entry.published).strftime("%Y-%m-%d %H:%M")
                                    except:
                                        pub_date = article.publish_date.strftime("%Y-%m-%d %H:%M") if article.publish_date else "Tanggal Tidak Ditemukan"

                                    self.session_data.append({
                                        "Tanggal Ekstraksi": datetime.now().strftime("%Y-%m-%d %H:%M"),
                                        "Tanggal Berita": pub_date, # <--- Kolom Baru
                                        "Sumber Domain": site,
                                        "Judul Berita": entry.title,
                                        "URL Berita": real_url,
                                        "Alasan Lolos (SLM)": slm_reason, # <--- Audit Trail Baru
                                        "Isi Berita": article.text
                                    })
                                else:
                                    print(f"     [REJECTED BY SLM] {slm_reason}")
                            else:
                                print(f"     [SKIPPED] Gagal Pre-Filter: {lex_reason}")
                        else:
                            print(f"     [FAIL] Kapasitas teks tidak memadai ({char_count} Karakter).")
                            
                    except Exception as e:
                        print(f"     [ERROR] Anomali Skrip: {e}")
                    finally:
                        await page.close()

                if valid_count < target_quota:
                    print(f"  [!] Feed habis. Hanya menemukan {valid_count} dari {target_quota} artikel valid.")

            await context.close()
            self.save_to_excel()
            
        print("\n    [Menyelesaikan eksekusi, merelay kontrol ke OS...]")
        await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(BPSProxyValidator().run())