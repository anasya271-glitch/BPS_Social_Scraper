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
        self.processed_data = self._load_checkpoint()

        self.priority_sites = [
            "tempo.co/tag/kota-bandung", "narasi.tv/tags/kota-bandung", "bandung.kompas.com", 
            "kabarbandung.pikiran-rakyat.com", "radarbandung.id", "bandung.go.id", 
            "kumparan.com/topic/bandung", "cnnindonesia.com/tag/bandung", "tirto.id/q/bandung-pU", 
            "rri.co.id/bandung", "infobandungkota.com", "ayobandung.com", "prfmnews.id", 
            "kilasbandungnews.com", "bandungbergerak.id", "koranmandala.com", 
            "jabarekspres.com", "jabar.tribunnews.com"
        ]
        
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
                    r"\bsumur[\s\-]?bandung\b", r"\bujung[\s\-]?berung\b"
                ],
                "BLACKLIST": [r"\bkabupaten[\s\-]?bandung\b", r"\bbupati\b", r"\bsoreang\b", r"\bkbb\b", r"\bbandung[\s\-]?barat\b", r"\blembang\b", r"\bcimahi\b"]
            },
            "TRADE_FLUX": {
                "INTERNASIONAL": [r"\bekspor\b", r"\bimpor\b", r"\bpasar global\b", r"\bmancanegara\b", r"\bluar negeri\b"],
                "ANTAR_DAERAH": [r"\bpasokan\b", r"\bsuplai\b", r"\bdistribusi\b", r"\bdikirim\b", r"\bmasuk ke\b", r"\bkeluar dari\b"],
                # Semantic Proximity: Menangkap "Harga [Kata] Naik/Meroket/Turun"
                "INDICATORS": [
                    r"\bharga\b.{0,30}(?:naik|melonjak|meroket|melambung|turun|anjlok|mahal|murah)\b",
                    r"(?:kenaikan|penurunan|lonjakan)\b.{0,30}\bharga\b",
                    r"\bkelangkaan\b", r"\bstok\b", r"\bfluktuasi\b", r"\bharga eceran\b", r"\bhet\b", r"\binflasi\b"
                ]
            },
            # Lexical Expansion: Penambahan elemen UMKM mikro
            "COMMODITIES": [
                r"\bberas\b", r"\bjagung\b", r"\bminyak[\s\-]?goreng\b", r"\bgula\b", r"\bterigu\b", r"\bkedelai\b",
                r"\bcabai\b", r"\bbawang\b", r"\bsayur\b", r"\bdaging\b", r"\btelur\b", r"\btahu\b", r"\btempe\b",
                r"\bplastik\b", r"\bcup\b", r"\bkemasan\b", r"\bpakaian\b", r"\bsepatu\b", r"\bsemen\b", r"\belpiji\b", r"\bbbm\b"
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
                    if isinstance(data, dict):
                        return {"links": set(data.get("links", [])), "titles": set(data.get("titles", []))}
            except (json.JSONDecodeError, ValueError):
                pass
        return {"links": set(), "titles": set()}

    def _save_checkpoint(self, link, title):
        self.processed_data["links"].add(link)
        self.processed_data["titles"].add(title.strip().lower())
        with open(self.checkpoint_file, 'w') as f:
            json.dump({"links": list(self.processed_data["links"]), "titles": list(self.processed_data["titles"])}, f, indent=4)

    def _triage_title(self, title):
        title_lower = title.lower()
        if any(re.search(word, title_lower) for word in self.config["NOISE_WORDS"]):
            return False
        return True

    async def _async_smart_fetcher(self, url, browser_context):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
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
        except Exception: return "EKSTRAKSI_GAGAL"

    def _calculate_relevance(self, title, text):
        content = f"{title} {text}".lower()
        score = 0
        matches = {"Geo": [], "Flux": [], "Commodity": [], "Penalti": []}
        
        has_strict_geo = False
        geo_score = 0

        # 1. Evaluasi Geografis Inti
        for anchor in self.config["GEOGRAPHY"]["STRICT_ANCHORS"] + self.config["GEOGRAPHY"]["DISTRICTS"]:
            finds = re.findall(anchor, content)
            if finds:
                geo_score += (15 * len(finds))
                matches["Geo"].append(anchor.replace(r"[\s\-]?", " ").replace(r"\b", ""))
                has_strict_geo = True

        # 2. Dynamic Penalty Logic
        blacklist_finds = []
        for b in self.config["GEOGRAPHY"]["BLACKLIST"]:
            finds = re.findall(b, content)
            if finds:
                blacklist_finds.extend(finds)

        if blacklist_finds:
            if not has_strict_geo:
                return -100, matches, "Ditolak (Eksklusif Luar Wilayah)"
            else:
                # Toleransi: Kurangi skor, tapi jangan bunuh datanya
                penalty = 5 * len(blacklist_finds)
                geo_score -= penalty
                matches["Penalti"].append(f"-{penalty} (Noise Sidebar)")

        score += geo_score

        # 3. Dimensi Trade Flux & Proximity
        for flux in self.config["TRADE_FLUX"]["INTERNASIONAL"]:
            finds = re.findall(flux, content)
            if finds:
                score += (20 * len(finds))
                matches["Flux"].append(flux.replace(r"[\s\-]?", " ").replace(r"\b", ""))
                
        for flux in self.config["TRADE_FLUX"]["ANTAR_DAERAH"] + self.config["TRADE_FLUX"]["INDICATORS"]:
            finds = re.findall(flux, content)
            if finds:
                score += (10 * len(finds))
                # Ambil representasi singkat jika regex kompleks
                match_str = "fluktuasi/harga" if "harga" in flux else flux.replace(r"[\s\-]?", " ").replace(r"\b", "")
                matches["Flux"].append(match_str)

        # 4. Pengenalan Komoditas Komprehensif
        for item in self.config["COMMODITIES"]:
            finds = re.findall(item, content)
            if finds:
                score += (5 * len(finds))
                matches["Commodity"].append(item.replace(r"[\s\-]?", " ").replace(r"\b", ""))

        # Executive Validation
        if score >= 35 and matches["Flux"] and matches["Commodity"]:
            status = "Prioritas Tinggi (Sinyal Ekonomi Kuat)"
        elif score >= 15 and (matches["Flux"] or matches["Commodity"]):
            status = "Potensi Data (Rantai Pasok Marginal)"
        else:
            status = "Ditolak (Skor Tidak Memadai)"

        return score, matches, status

    async def _process_single_article(self, entry, domain, context, semaphore, results_list):
        async with semaphore:
            await asyncio.sleep(random.uniform(1.0, 2.0))
            print(f"     ├── [MEMBEDAH] {entry.title[:60]}...")
            full_text = await self._async_smart_fetcher(entry.link, context)
            
            self._save_checkpoint(entry.link, entry.title)
            
            if full_text == "EKSTRAKSI_GAGAL": return

            score, matches, status = self._calculate_relevance(entry.title, full_text)

            if "Ditolak" not in status:
                results_list.append({
                    "Sumber": domain, "Status": status, "Skor": score, "Tanggal": entry.get('published', ''),
                    "Judul": entry.title, "Link": entry.link,
                    "Geografi": ", ".join(set(matches["Geo"])),
                    "Komoditas": ", ".join(set(matches["Commodity"])),
                    "Indikator": ", ".join(set(matches["Flux"])),
                    "Penalti": ", ".join(matches["Penalti"]),
                    "Isi": full_text[:1500]
                })

    async def run_mining_async(self, start, end):
        print(f"\n[SYSTEM] Memulai Sequential Exhaustion Mining ({start} s/d {end})")
        base_query = '("Kota Bandung" OR "Pemkot Bandung" OR "UMKM Bandung" OR "Pasar")'
        all_results = []
        
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                concurrency_limit = asyncio.Semaphore(5)
                
                for site in self.priority_sites:
                    domain = site.split("/")[0]
                    print(f"\n[RADAR] Memindai portal: {domain}")
                    
                    full_query = f"{base_query} site:{domain} after:{start} before:{end}"
                    encoded_query = urllib.parse.quote(full_query)
                    rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=id&gl=ID&ceid=ID:id"
                    
                    await asyncio.sleep(random.uniform(1.5, 3.0)) 
                    feed = await asyncio.to_thread(feedparser.parse, rss_url)
                    
                    valid_entries = []
                    skipped_count = 0
                    for entry in feed.entries:
                        title_clean = entry.title.strip().lower()
                        if entry.link in self.processed_data["links"] or title_clean in self.processed_data["titles"]:
                            skipped_count += 1
                            continue
                        if self._triage_title(entry.title):
                            valid_entries.append(entry)
                    
                    print(f"  ├─ Ditemukan: {len(feed.entries)} | Dilewati (Terekam): {skipped_count} | Siap Dibedah: {len(valid_entries)}")
                    
                    if valid_entries:
                        context = await browser.new_context()
                        tasks = [asyncio.create_task(self._process_single_article(entry, domain, context, concurrency_limit, all_results)) for entry in valid_entries]
                        await asyncio.gather(*tasks)
                        await context.close()
                        print(f"  └─ Selesai memproses {domain}.")
                
                await browser.close()
        except asyncio.CancelledError:
            print("\n[!] Interupsi Manual. Menyelesaikan penyimpanan data...")

        if all_results:
            df = pd.DataFrame(all_results).sort_values(by="Skor", ascending=False)
            fname = f"{self.output_path}/NEWS_FULL_AUDIT_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            df.to_excel(fname, index=False)
            print(f"\n[SUCCESS] Proses sapu bersih selesai. Laporan disimpan: {fname}")
        else:
            print("\n[INFO] Operasi selesai. Tidak ada artikel yang menembus ambang batas.")

    def run(self, start, end):
        try:
            asyncio.run(self.run_mining_async(start, end))
        except KeyboardInterrupt:
            print("\n[!] Dihentikan secara paksa oleh Pengguna. Data yang terekstrak tetap aman.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="BPS Enterprise Intelligence Miner")
    parser.add_argument('--mode', choices=['history'], required=True)
    parser.add_argument('--start', type=str, required=True)
    parser.add_argument('--end', type=str, required=True)
    args = parser.parse_args()
    miner = BPSNewsMiner()
    miner.run(start=args.start, end=args.end)