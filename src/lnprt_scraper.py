import feedparser
import trafilatura
import pandas as pd
import re
import os
import urllib.parse
from datetime import datetime
import warnings
import json
import requests
import asyncio
from playwright.async_api import async_playwright
import random

warnings.filterwarnings("ignore")

class BPSNewsMiner:
    def __init__(self):
        self.output_path = "data/news_results/"
        self.checkpoint_dir = "data/checkpoints/"
        os.makedirs(self.output_path, exist_ok=True)
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        
        self.checkpoint_file = os.path.join(self.checkpoint_dir, "checkpoint_news.json")
        self.processed_urls = self._load_checkpoint()

        self.priority_sites = [
            "tirto.id/q/bandung-pU", "tempo.co/tag/kota-bandung", "narasi.tv/tags/kota-bandung", "bandung.kompas.com", 
            "kabarbandung.pikiran-rakyat.com", "radarbandung.id", "bandung.go.id", 
            "kumparan.com/topic/bandung", "cnnindonesia.com/tag/bandung", 
            "rri.co.id/bandung", "infobandungkota.com", "ayobandung.com", "prfmnews.id", 
            "kilasbandungnews.com", "bandungbergerak.id", "koranmandala.com", 
            "jabarekspres.com", "jabar.tribunnews.com"
        ]
        
        self.config = {
            "GEOGRAPHY": {
                "STRICT_ANCHORS": [r"\bkota[\s\-]?bandung\b", r"\bpemkot[\s\-]?bandung\b", r"\bwali[\s\-]?kota[\s\-]?bandung\b"],
                "BROAD_ANCHOR": [r"\bbandung\b", r"\bbdg\b"],
                "DISTRICTS": [
                    r"\bandir\b", r"\bastana[\s\-]?anyar\b", r"\bantapani\b", r"\barcamanik\b", r"\bbabakan[\s\-]?ciparay\b", 
                    r"\bbandung[\s\-]?kidul\b", r"\bbandung[\s\-]?kulon\b", r"\bbandung[\s\-]?wetan\b", r"\bbatununggal\b", 
                    r"\bbojongloa[\s\-]?kaler\b", r"\bbojongloa[\s\-]?kidul\b", r"\bbuah[\s\-]?batu\b", r"\bcibeunying\b", 
                    r"\bcibiru\b", r"\bcicendo\b", r"\bcidadap\b", r"\bcinambo\b", r"\bcoblong\b", 
                    r"\bgedebage\b", r"\bkiara[\s\-]?condong\b", r"\blengkong\b", r"\bmandalajati\b", 
                    r"\bpanyileukan\b", r"\brancasari\b", r"\bregol\b", r"\bsukajadi\b", r"\bsukasari\b", 
                    r"\bsumur[\s\-]?bandung\b", r"\bujung[\s\-]?berung\b"
                ],
                "BLACKLIST": [r"\bkabupaten[\s\-]?bandung\b", r"\bbupati\b", r"\bsoreang\b", r"\bkbb\b", r"\bbandung[\s\-]?barat\b", r"\blembang\b", r"\bcimahi\b"]
            },
            "TRADE_FLUX": {
                "INTERNASIONAL": [r"\bekspor\b", r"\bimpor\b", r"\bpasar global\b", r"\bpasar internasional\b", r"\bmancanegara\b", r"\bmenembus pasar\b", r"\bluar negeri\b"],
                "ANTAR_DAERAH": [r"\bpasokan dari\b", r"\bdatangkan dari\b", r"\bsuplai dari\b", r"\bdikirim dari\b", r"\bmasuk ke bandung\b", r"\bdistribusi dari\b", r"\bdikirim ke\b", r"\bpasok ke\b", r"\bdistribusi ke\b", r"\bkeluar bandung\b", r"\bantar daerah\b", r"\bantar provinsi\b"],
                "INDICATORS": [r"\bkelangkaan\b", r"\bkenaikan harga\b", r"\bstok\b", r"\bfluktuasi\b", r"\bharga eceran\b", r"\bhet\b", r"\binflasi\b"]
            },
            "COMMODITIES": [
                r"\bberas\b", r"\bjagung\b", r"\bminyak[\s\-]?goreng\b", r"\bgula[\s\-]?pasir\b", r"\bterigu\b", r"\bkedelai\b",
                r"\bcabai\b", r"\bbawang\b", r"\bsayur\b", r"\bdaging\b", r"\btelur\b", r"\bpakaian\b", r"\bsepatu\b", 
                r"\bsemen\b", r"\belpiji\b", r"\bbbm\b", r"\bproduk umkm\b", r"\bkerajinan\b", r"\bkopi\b", r"\btekstil\b"
            ],
            "NOISE_WORDS": [
                r"\bprabowo\b", r"\bjokowi\b", r"\bpiala\b", r"\bliga\b", r"\bgempa\b", r"\bkecelakaan\b", 
                r"\bpembunuhan\b", r"\bskandal\b", r"\bsepakbola\b", r"\bpersib\b", r"\bdpr\b", r"\bkpk\b", 
                r"\bmenteri\b", r"\bkementerian\b", r"\bptun\b", r"\bpanglima\b", r"\bpolri\b", r"\bpresiden\b", 
                r"\bsnbt\b", r"\bunpad\b", r"\bkemendikdasmen\b", r"\bhakim\b", r"\bperadilan\b"
            ]
        }

    def _load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    return set(data) if isinstance(data, list) else set()
            except (json.JSONDecodeError, ValueError):
                return set()
        return set()

    def _save_checkpoint(self, url):
        self.processed_urls.add(url)
        with open(self.checkpoint_file, 'w') as f:
            json.dump(list(self.processed_urls), f, indent=4)

    def _triage_title(self, title):
        title_lower = title.lower()
        if any(re.search(word, title_lower) for word in self.config["NOISE_WORDS"]):
            return False
        return True

    async def _async_smart_fetcher(self, url, browser_context):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        try:
            res = await asyncio.to_thread(requests.get, url, headers=headers, timeout=15)
            text = trafilatura.extract(res.text)
            
            if not text or len(text) < 300:
                page = await browser_context.new_page()
                await page.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "media", "font"] else route.continue_())
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                text = trafilatura.extract(await page.content())
                await page.close()
                
            return text if text else "EKSTRAKSI_GAGAL"
        except Exception: 
            return "EKSTRAKSI_GAGAL"

    def _calculate_relevance(self, title, text):
        content = f"{title} {text}".lower()
        score = 0
        matches = {"Geo": [], "Flux": [], "Commodity": []}
        has_strict_geo = False

        for anchor in self.config["GEOGRAPHY"]["STRICT_ANCHORS"] + self.config["GEOGRAPHY"]["DISTRICTS"]:
            finds = re.findall(anchor, content)
            if finds:
                score += (15 * len(finds))
                matches["Geo"].append(anchor.replace(r"[\s\-]?", " ").replace(r"\b", ""))
                has_strict_geo = True
                
        broad_finds = re.findall(self.config["GEOGRAPHY"]["BROAD_ANCHOR"][0], content)
        if broad_finds:
            score += (5 * len(broad_finds))

        has_blacklist = any(re.search(b, content) for b in self.config["GEOGRAPHY"]["BLACKLIST"])
        if has_blacklist and not has_strict_geo:
            return -100, matches, "Ditolak (Bias Kabupaten/Regional Lain)"

        for flux in self.config["TRADE_FLUX"]["INTERNASIONAL"]:
            finds = re.findall(flux, content)
            if finds:
                score += (20 * len(finds))
                matches["Flux"].append(flux.replace(r"[\s\-]?", " ").replace(r"\b", ""))
                
        for flux in self.config["TRADE_FLUX"]["ANTAR_DAERAH"] + self.config["TRADE_FLUX"]["INDICATORS"]:
            finds = re.findall(flux, content)
            if finds:
                score += (10 * len(finds))
                matches["Flux"].append(flux.replace(r"[\s\-]?", " ").replace(r"\b", ""))

        for item in self.config["COMMODITIES"]:
            finds = re.findall(item, content)
            if finds:
                score += (5 * len(finds))
                matches["Commodity"].append(item.replace(r"[\s\-]?", " ").replace(r"\b", ""))

        if score >= 40 and matches["Flux"] and matches["Commodity"]:
            status = "Prioritas Tinggi (Validasi Ekspor/Impor BPS)"
        elif score >= 15 and (matches["Flux"] or matches["Commodity"]):
            status = "Potensi Data (Geografi Terverifikasi)"
        else:
            status = "Ditolak (Skor Relevansi Marginal)"

        return score, matches, status

    async def _process_single_article(self, entry, domain, context, semaphore, results_list):
        async with semaphore:
            await asyncio.sleep(random.uniform(1.0, 3.0))
            
            print(f"     ├── [ASYNC] Membedah: [{domain}] {entry.title[:40]}...")
            full_text = await self._async_smart_fetcher(entry.link, context)
            self._save_checkpoint(entry.link)
            
            if full_text == "EKSTRAKSI_GAGAL": return

            score, matches, status = self._calculate_relevance(entry.title, full_text)

            if "Ditolak" not in status:
                results_list.append({
                    "Sumber": domain,
                    "Status": status, "Skor": score, "Tanggal": entry.get('published', ''),
                    "Judul": entry.title, "Link": entry.link, "Isi": full_text[:1000]
                })

    async def run_mining_async(self, start, end):
        print(f"\n[SYSTEM] Memulai Macro-Micro Economic Tracking ({start} s/d {end})")
        
        base_query = '("Kota Bandung" OR "Pemkot Bandung" OR "UMKM Bandung") AND (pasokan OR impor OR ekspor OR stok OR harga OR komoditas OR mancanegara)'
        grouped_candidates = {}
        
        print("\n[FASE 1] Menyapu Metadata RSS (Aman dari IP Block)...")
        for site in self.priority_sites:
            domain = site.split("/")[0]
            full_query = f"{base_query} site:{domain} after:{start} before:{end}"
            encoded_query = urllib.parse.quote(full_query)
            rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=id&gl=ID&ceid=ID:id"
            
            await asyncio.sleep(random.uniform(2.0, 4.0)) 
            feed = await asyncio.to_thread(feedparser.parse, rss_url)
            
            valid_entries = []
            for entry in feed.entries:
                if entry.link in self.processed_urls:
                    continue
                if self._triage_title(entry.title):
                    valid_entries.append((entry, domain))
            
            if valid_entries:
                print(f"  ├─ {domain}: Lolos Triage {len(valid_entries)} artikel.")
                grouped_candidates[domain] = valid_entries
                
        candidate_articles = []
        max_debug_limit = 30
        
        while len(candidate_articles) < max_debug_limit and grouped_candidates:
            added = False
            for domain in list(grouped_candidates.keys()):
                if grouped_candidates[domain]:
                    candidate_articles.append(grouped_candidates[domain].pop(0))
                    added = True
                
                if len(candidate_articles) >= max_debug_limit:
                    break
                    
                if not grouped_candidates[domain]:
                    del grouped_candidates[domain]
            
            if not added:
                break
        
        total_candidates = len(candidate_articles)
        if total_candidates == 0:
            print("\n[INFO] Tidak ada kandidat baru yang lolos Triage.")
            return

        print(f"\n[FASE 3] Ekstraksi Asinkronus ({total_candidates} Artikel Diversifikasi) dimulai...")
        
        all_results = []
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context()
                concurrency_limit = asyncio.Semaphore(4)
                
                tasks = [
                    asyncio.create_task(self._process_single_article(entry, domain, context, concurrency_limit, all_results))
                    for entry, domain in candidate_articles
                ]
                await asyncio.gather(*tasks)
                await browser.close()
        except asyncio.CancelledError:
            print("\n[!] Interupsi Manual. Menutup peramban dengan aman...")

        if all_results:
            df = pd.DataFrame(all_results).sort_values(by="Skor", ascending=False)
            fname = f"{self.output_path}/NEWS_ASYNC_AUDIT_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            df.to_excel(fname, index=False)
            print(f"\n[SUCCESS] Laporan disimpan: {fname}")
        else:
            print(f"\n[INFO] Ekstraksi selesai. Dari {total_candidates} sampel, tidak ada data yang memenuhi ambang batas baru relevansi ekspor-impor BPS.")

    def run(self, start, end):
        try:
            asyncio.run(self.run_mining_async(start, end))
        except KeyboardInterrupt:
            print("\n[!] Dihentikan secara paksa oleh Pengguna. Data yang terekstrak telah disimpan.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="BPS Async Intelligence Miner")
    parser.add_argument('--mode', choices=['history'], required=True)
    parser.add_argument('--start', type=str, required=True)
    parser.add_argument('--end', type=str, required=True)
    args = parser.parse_args()

    miner = BPSNewsMiner()
    miner.run(start=args.start, end=args.end)