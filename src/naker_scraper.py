# naker_scraper.py
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
import signal
from datetime import datetime
from pathlib import Path
from newspaper import Article, Config
from playwright.async_api import async_playwright, TimeoutError, Error as PlaywrightError
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse
from tqdm.asyncio import tqdm as async_tqdm
import warnings

warnings.filterwarnings("ignore")


class BPS_Naker_Sentinel:
    """
    Surgical Precision Debugger V66 (Ketenagakerjaan Edition) | The Precision Sentinel
    
    Modul aktif:
    - Tiered Scoring System (0-100 relevance score)
    - 40+ Euphemism Detection (PHK, rekrutmen, isu ketenagakerjaan)
    - Multi-Window Smart Truncate
    - Incremental Checkpoint (every 50 articles)
    - Graceful Shutdown Handler
    - Audit File Merger
    - Progress Bar Tracking
    - Few-Shot SLM Prompting
    """
    
    def __init__(self, args):
        self.args = args
        
        self.sites = [
            "bandung.go.id",
            "jabarprov.go.id",
            "jabar.tribunnews.com",
            "tempo.co",
            "tirto.id",
            "narasi.tv",
            "ayobandung.com",
            "pikiran-rakyat.com",
            "bandung.kompas.com",
            "disnaker.bandung.go.id",
            "detik.com",
            "liputan6.com"
        ]
        
        self.edge_source_dir = str(Path.home() / "AppData" / "Local" / "Microsoft" / "Edge" / "User Data")
        self.workspace_dir = Path.cwd() / "data" / "edge_workspace"
        self.export_dir = Path.cwd() / "data" / "naker"
        self.visited_file = Path.cwd() / "visited_naker_urls.txt"
        
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
        self.model_name = "bps-naker"
        
        self.browser_semaphore = asyncio.Semaphore(2)
        
        # Shutdown handler
        self.shutdown_requested = False
        signal.signal(signal.SIGINT, self._signal_handler)
        
        # Statistics tracking
        self.stats = {
            'total_scanned': 0,
            'rejected_preflight': 0,
            'rejected_lexical': 0,
            'accepted_slm': 0,
            'rejected_slm': 0,
            'geo_explicit': 0,
            'geo_kecamatan': 0,
            'dampak_positif': 0,
            'dampak_negatif': 0,
            'dampak_isu': 0,
            'confidence_scores': []
        }
        
        # Enhanced Configuration with Scoring Matrix
        self.config = {
            "SCORING_MATRIX": {
                # Geographic Signals (Max 40 points)
                'explicit_kota_bandung': 40,      # "Kota Bandung", "Pemkot Bandung"
                'kecamatan_mention': 30,          # Cicendo, Coblong, etc
                'bandung_generic': 20,            # Just "Bandung"
                'bandung_with_context': 35,       # "Bandung" + street/landmark
                
                # Naker Signals (Max 40 points)
                'naker_explicit_3plus': 40,       # >=3 keywords
                'naker_explicit_2': 30,           # 2 keywords
                'naker_explicit_1': 20,           # 1 keyword
                'naker_euphemism': 35,            # Indirect terms
                
                # Penalties (Negative)
                'blacklist_strong': -60,          # "Kabupaten Bandung" + blacklist
                'blacklist_weak': -30,            # Province mention
                'noise_detected': -25,            # Spam/academic
                'dual_mention_unclear': -15,      # Kota+Kab both mentioned ambiguously
                
                # Bonuses
                'domain_credibility': 10,         # Kompas, Tempo, govt
                'title_match': 10,                # Title has geo+naker
            },
            
            "GEOGRAPHY": {
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
                    r"\bjl\.?\s?asia\s?afrika\b",
                    r"\bjl\.?\s?dago\b",
                    r"\bjl\.?\s?braga\b",
                    r"\balun[\s\-]?alun\b",
                    r"\bgedung\s?sate\b",
                    r"\btrans\s?studio\b",
                    r"\bgasibu\b",
                ],
                
                "BLACKLIST": [
                    r"\bkabupaten[\s\-]?bandung\b",
                    r"\bkab\.?\s?bandung\b",
                    r"\bbupati\s+bandung\b",
                    r"\bsoreang\b",
                    r"\bkbb\b",
                    r"\bbandung[\s\-]?barat\b",
                    r"\blembang\b",
                    r"\bcimahi\b",
                    r"\bmajalaya\b",
                    r"\bpangalengan\b",
                    r"\bciwidey\b",
                    r"\bpadalarang\b",
                    r"\bpemkab\b",
                    # Other cities/provinces
                    r"\bbekasi\b", r"\bbogor\b", r"\bdepok\b", r"\bkarawang\b",
                    r"\bpurwakarta\b", r"\bsukabumi\b", r"\bciamis\b", r"\btasikmalaya\b",
                    r"\bgarut\b", r"\bcirebon\b", r"\bcianjur\b", r"\bindramayu\b",
                    r"\bjakarta\b", r"\bdki jakarta\b", r"\bjawa tengah\b", r"\bjateng\b",
                    r"\bsemarang\b", r"\bjawa timur\b", r"\bjatim\b", r"\bsurabaya\b",
                ],
            },
            
            "NAKER_POSITIF": [
                # Explicit Hiring
                r"\bbuka lowongan\b",
                r"\blowongan kerja\b",
                r"\blowongan pabrik\b",
                r"\brekrutmen\b",
                r"\brekrutmen massal\b",
                r"\brekrutmen terbuka\b",
                r"\bpenerimaan karyawan\b",
                r"\bpenerimaan pegawai\b",
                r"\bpenerimaan tenaga kerja\b",
                r"\bterima karyawan\b",
                
                # Job Fair
                r"\bjob fair\b",
                r"\bbursa kerja\b",
                r"\bbursa kerja khusus\b",
                r"\bbkk\b",
                r"\bexpo kerja\b",
                r"\bpameran kerja\b",
                
                # Hiring Language
                r"\bhiring\b",
                r"\bwe are hiring\b",
                r"\bdibutuhkan segera\b",
                r"\bbutuh tenaga\b",
                r"\bmembuka posisi\b",
                r"\bkarir terbuka\b",
                r"\bcareer opportunity\b",
                
                # Expansion Indicators
                r"\bpadat karya\b",
                r"\bserap tenaga kerja\b",
                r"\bpenyerapan tenaga kerja\b",
                r"\bmembuka lapangan kerja\b",
                r"\btambah karyawan\b",
                r"\bpenambahan pekerja\b",
                r"\bpenambahan sdm\b",
                r"\bpenambahan tenaga\b",
                r"\bpabrik baru\b",
                r"\bbuka pabrik\b",
                r"\bpabrik ekspansi\b",
                r"\bpeningkatan produksi\b",
                r"\bpeningkatan kapasitas\b",
                r"\bekspansi bisnis\b",
                r"\bproyek infrastruktur\b",
                r"\binvestasi baru\b",
                r"\binvestasi pabrik\b",
                
                # Training/Internship
                r"\bpelatihan kerja\b",
                r"\bmagang\b",
                r"\bpraktek kerja\b",
                r"\bapprentice\b",
                r"\bvokasi\b",
            ],
            
            "NAKER_NEGATIF": [
                # Explicit Terms
                r"\bphk\b",
                r"\bpemutusan hubungan kerja\b",
                r"\bdirumahkan\b",
                r"\bpemecatan\b",
                r"\bdipecat\b",
                
                # Euphemisms (CRITICAL EXPANSION)
                r"\brasionalisasi karyawan\b",
                r"\brasionalisasi tenaga kerja\b",
                r"\brasionalisasi\s+sdm\b",
                r"\bright[\s\-]?sizing\b",
                r"\bdown[\s\-]?sizing\b",
                r"\brestrukturisasi organisasi\b",
                r"\brefisiensi\s?sdm\b",
                r"\befisiensi\s?sumber\s?daya\s?manusia\b",
                r"\bpengurangan tenaga kerja\b",
                r"\bpengurangan karyawan\b",
                r"\bpenyesuaian struktur\b",
                r"\boptimalisasi tenaga kerja\b",
                r"\bpemangkasan\s+pegawai\b",
                r"\bpemangkasan\s+karyawan\b",
                r"\breduksi\s+tenaga\s+kerja\b",
                r"\breduksi\s+sdm\b",
                
                # Contract/Operational
                r"\btidak diperpanjang kontrak\b",
                r"\bkontrak tidak dilanjutkan\b",
                r"\bkontrak habis\b",
                r"\bkontrak berakhir\b",
                r"\bberhenti beroperasi\b",
                r"\bpenutupan operasional\b",
                r"\bpenghentian produksi\b",
                r"\bpenghentian operasi\b",
                r"\bsuspend\s?operasi\b",
                r"\btutup sementara\b",
                
                # Company Closure
                r"\bpabrik tutup\b",
                r"\btutup pabrik\b",
                r"\bgulung tikar\b",
                r"\bbangkrut\b",
                r"\bpailit\b",
                r"\blikuidasi\b",
                r"\bpenutupan perusahaan\b",
                r"\bpenutupan pabrik\b",
                
                # Workforce Reduction
                r"\bkurangi karyawan\b",
                r"\bkurangi tenaga\b",
                r"\bpemotongan\s?pegawai\b",
                r"\bpengurangan\s?shift\b",
                r"\bpemotongan\s?jam\s?kerja\b",
                r"\bpengurangan\s?jam\s?kerja\b",
                
                # Indirect Indicators
                r"\bgagal panen\b",
                r"\bpuso\b",
                r"\bomzet turun drastis\b",
                r"\bsepi pembeli\b",
                r"\bsepi order\b",
                r"\bkehilangan kontrak\b",
                r"\brelokasi pabrik\b",
                r"\bpindah pabrik\b",
            ],
            
            "NAKER_ISU": [
                # Wages
                r"\bupah minimum\b",
                r"\bump\b",
                r"\bumk\b",
                r"\bumr\b",
                r"\bumkm\b",
                r"\bgaji\b",
                r"\bpeningkatan upah\b",
                r"\bkenaikan upah\b",
                r"\bupah buruh\b",
                r"\bupah pekerja\b",
                
                # Benefits
                r"\btunjangan\b",
                r"\bthr\b",
                r"\btunjangan hari raya\b",
                r"\bbpjs ketenagakerjaan\b",
                r"\bjamsostek\b",
                r"\bpesangon\b",
                r"\buang pesangon\b",
                r"\bbonus karyawan\b",
                
                # Labor Actions
                r"\bdemo buruh\b",
                r"\bunjuk rasa buruh\b",
                r"\bpemogokan\b",
                r"\baksi mogok\b",
                r"\bmogok kerja\b",
                r"\bserikat pekerja\b",
                r"\bserikat buruh\b",
                
                # Employment Terms
                r"\boutsourcing\b",
                r"\bpekerja kontrak\b",
                r"\bpkwt\b",
                r"\bpkwtt\b",
                r"\bkontrak kerja\b",
                r"\bperjanjian kerja\b",
                r"\bhak pekerja\b",
                r"\bkesejahteraan buruh\b",
                r"\bcuti bersama\b",
                r"\blembur\b",
                r"\bk3\b",
                r"\bkeselamatan kerja\b",
            ],
            
            "NOISE_WORDS": [
                # Sports (CRITICAL: Persib often mentions "recruitment")
                r"\bpiala\b", r"\bliga\b", r"\bpersib\b", r"\bmaung\b",
                r"\btransfer pemain\b", r"\brekrut pemain\b",
                
                # Disaster/Crime
                r"\bgempa\b", r"\bkecelakaan\b", r"\bpembunuhan\b",
                r"\bnarkoba\b", r"\bbencana\b",
                
                # Politics (unless labor-related)
                r"\bpilkada\b", r"\bkampanye\b", r"\bcapres\b", r"\bcawalkot\b",
                r"\bpartai\b", r"\bkpu\b", r"\bbawaslu\b",
                
                # Academic (unless job fair)
                r"\bkampus\b", r"\buniversitas\b", r"\binstitut\b", r"\bpoliteknik\b",
                r"\bmahasiswa\b", r"\bdosen\b", r"\brektor\b", r"\bwisuda\b",
                r"\bsnbt\b", r"\bppdb\b", r"\bseminar\b", r"\bwebinar\b",
                
                # ASN/Government (unless private sector labor)
                r"\basn\b", r"\bcpns\b", r"\bpppk\b", r"\bmutasi jabatan\b",
                r"\brotasi jabatan\b", r"\bkinerja asn\b",
                
                # Generic spam
                r"\bsyarat pendaftaran\b", r"\bcara melamar\b",
                r"\blink pendaftaran\b", r"\bkirim lamaran\b",
            ],
            
            "TITLE_BLACKLIST": [
                "pegawai", "sejarah", "visi", "misi", "tupoksi",
                "kontak", "gallery", "profil", "bab i", "powerpoint",
                "open data", "jurnal", "pengumuman", "layanan",
                "jadwal", "beranda", "home", "indeks", "index",
            ],
            
            "DOCUMENT_EXTENSIONS": [
                ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx"
            ],
            
            "CREDIBLE_DOMAINS": [
                "kompas.com", "tempo.co", "tirto.id", "detik.com",
                "liputan6.com", "bandung.go.id", "jabarprov.go.id",
                "disnaker.bandung.go.id"
            ]
        }
        
        # PRE-COMPILE ALL REGEX PATTERNS (15-20% speedup)
        self.compiled_patterns = self._compile_all_patterns()
    
    def _compile_all_patterns(self):
        """Pre-compile all regex patterns for performance"""
        compiled = {
            'strict_anchors': [re.compile(p, re.I) for p in self.config['GEOGRAPHY']['STRICT_ANCHORS']],
            'districts': [re.compile(p, re.I) for p in self.config['GEOGRAPHY']['DISTRICTS']],
            'landmarks': [re.compile(p, re.I) for p in self.config['GEOGRAPHY']['LANDMARKS']],
            'blacklist': [re.compile(p, re.I) for p in self.config['GEOGRAPHY']['BLACKLIST']],
            'naker_positif': [re.compile(p, re.I) for p in self.config['NAKER_POSITIF']],
            'naker_negatif': [re.compile(p, re.I) for p in self.config['NAKER_NEGATIF']],
            'naker_isu': [re.compile(p, re.I) for p in self.config['NAKER_ISU']],
            'noise': [re.compile(p, re.I) for p in self.config['NOISE_WORDS']],
        }
        return compiled
    
    def _signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully"""
        print("\n\n [!] SHUTDOWN SIGNAL RECEIVED. Saving data gracefully...")
        self.shutdown_requested = True
    
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
        if not self.new_urls_to_save:
            return
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
        except:
            pass
    
    def check_ollama(self):
        try:
            r = requests.get("http://localhost:11434/api/version", timeout=3)
            if r.status_code == 200:
                print(" [] Ollama Server (Local AI) siap beroperasi.")
                return True
        except:
            print(" [!!!] Ollama TIDAK TERDETEKSI. Harap eksekusi `ollama serve` di terminal terpisah.")
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
                if match:
                    real_url = match.group(1).decode('utf-8')
            except Exception:
                pass
        
        if any(domain in real_url for domain in ["tribunnews.com", "pikiran-rakyat.com", "ayobandung.com", "kompas.com", "tirto.id"]):
            parsed = urlparse(real_url)
            params = parse_qs(parsed.query)
            params['page'] = ['all']
            new_query = urlencode(params, doseq=True)
            real_url = urlunparse(parsed._replace(query=new_query))
        
        return real_url
    
    def calculate_relevance_score(self, title, url, text=""):
        """
        Calculate relevance score (0-100) based on tiered scoring matrix
        
        Returns: (score, breakdown_dict)
        """
        score = 0
        breakdown = {}
        
        combined = f"{title} {url} {text}".lower()
        
        # === GEOGRAPHIC SCORING ===
        has_strict_anchor = any(p.search(combined) for p in self.compiled_patterns['strict_anchors'])
        has_kecamatan = any(p.search(combined) for p in self.compiled_patterns['districts'])
        has_landmark = any(p.search(combined) for p in self.compiled_patterns['landmarks'])
        has_bandung_generic = bool(re.search(r'\bbandung\b', combined))
        
        if has_strict_anchor:
            score += self.config['SCORING_MATRIX']['explicit_kota_bandung']
            breakdown['geo'] = "Explicit Kota Bandung (+40)"
        elif has_kecamatan:
            score += self.config['SCORING_MATRIX']['kecamatan_mention']
            breakdown['geo'] = "Kecamatan Kota Bandung (+30)"
        elif has_landmark:
            score += self.config['SCORING_MATRIX']['bandung_with_context']
            breakdown['geo'] = "Bandung + Landmark (+35)"
        elif has_bandung_generic:
            score += self.config['SCORING_MATRIX']['bandung_generic']
            breakdown['geo'] = "Generic Bandung (+20)"
        else:
            breakdown['geo'] = "No geographic signal (0)"
        
        # === NAKER SCORING ===
        naker_pos_count = sum(1 for p in self.compiled_patterns['naker_positif'] if p.search(combined))
        naker_neg_count = sum(1 for p in self.compiled_patterns['naker_negatif'] if p.search(combined))
        naker_isu_count = sum(1 for p in self.compiled_patterns['naker_isu'] if p.search(combined))
        
        total_naker = naker_pos_count + naker_neg_count + naker_isu_count
        
        if total_naker >= 3:
            score += self.config['SCORING_MATRIX']['naker_explicit_3plus']
            breakdown['naker'] = f"Naker Keywords >=3 (+40) [{naker_pos_count}P/{naker_neg_count}N/{naker_isu_count}I]"
        elif total_naker == 2:
            score += self.config['SCORING_MATRIX']['naker_explicit_2']
            breakdown['naker'] = f"Naker Keywords 2 (+30) [{naker_pos_count}P/{naker_neg_count}N/{naker_isu_count}I]"
        elif total_naker == 1:
            score += self.config['SCORING_MATRIX']['naker_explicit_1']
            breakdown['naker'] = f"Naker Keywords 1 (+20) [{naker_pos_count}P/{naker_neg_count}N/{naker_isu_count}I]"
        else:
            breakdown['naker'] = "No Naker keywords (0)"
        
        # === PENALTIES ===
        has_blacklist = any(p.search(combined) for p in self.compiled_patterns['blacklist'])
        has_noise = any(p.search(combined) for p in self.compiled_patterns['noise'])
        
        # Special case: Dual mention (Kota + Kabupaten both mentioned)
        has_kota_mention = bool(re.search(r'\bkota\s+bandung\b', combined))
        has_kab_mention = bool(re.search(r'\b(kabupaten|kab\.?)\s+bandung\b', combined))
        
        if has_blacklist:
            if has_strict_anchor:
                # Kota + Kab both mentioned - penalty but not auto-reject
                score += self.config['SCORING_MATRIX']['dual_mention_unclear']
                breakdown['penalty'] = "Dual Kota+Kab mention (-15)"
            else:
                # Only Kab mentioned - strong reject
                score += self.config['SCORING_MATRIX']['blacklist_strong']
                breakdown['penalty'] = "Kabupaten/Provinsi lain (-60)"
        
        if has_noise and total_naker < 2:
            score += self.config['SCORING_MATRIX']['noise_detected']
            breakdown['penalty'] = breakdown.get('penalty', '') + " Noise detected (-25)"
        
        # === BONUSES ===
        domain = urlparse(url).netloc
        if any(d in domain for d in self.config['CREDIBLE_DOMAINS']):
            score += self.config['SCORING_MATRIX']['domain_credibility']
            breakdown['bonus'] = "Credible domain (+10)"
        
        if has_strict_anchor and total_naker >= 1:
            score += self.config['SCORING_MATRIX']['title_match']
            breakdown['bonus'] = breakdown.get('bonus', '') + " Title has Geo+Naker (+10)"
        
        return max(0, min(100, score)), breakdown
    
    def is_rejected_preflight(self, title, url):
        """Quick pre-flight rejection based on title + URL only"""
        title_lower = title.lower()
        url_lower = url.lower()
        
        # Document extensions
        for ext in self.config["DOCUMENT_EXTENSIONS"]:
            if url_lower.endswith(ext) or (ext + "?" in url_lower):
                return True, "Ekstensi Dokumen Non-Naratif"
        
        # Static pages
        if any(b in title_lower for b in self.config["TITLE_BLACKLIST"]):
            return True, "Halaman Statis/Administratif"
        
        # Quick score check (no need to fetch content)
        score, _ = self.calculate_relevance_score(title, url)
        
        if score < 30:
            return True, f"Relevance Score Terlalu Rendah ({score}/100)"
        
        return False, f"Pass Pre-flight (Score: {score}/100)"
    
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
                if attempt == 1:
                    return True, "MIME Timeout"
            except:
                return True, "MIME Error"
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
        except:
            pass
    
    async def extract_text_native(self, page):
        try:
            text = await page.evaluate('''() => {
                let articleBody = document.querySelector('.content-text-editor, .baca-block, .txt-article, .detail__body-text, .entry-content, article.read__content, .post-content, .detail-text');
                if (articleBody) {
                    let junks = articleBody.querySelectorAll('script, style, iframe, .video, .ads, .baca-juga, [id*="gliastudios"], [class*="gliastudios"], .video-wrap');
                    junks.forEach(j => j.remove());
                    return articleBody.innerText;
                }
                return "";
            }''')
            return text.strip()
        except:
            return ""
    
    def clean_article_text(self, text):
        clean_text = re.sub(r'(?i)(baca juga|baca selengkapnya|artikel terkait|simak juga|penulis:|editor:).*?\n', '\n', text)
        clean_text = re.sub(r'\n+', '\n', clean_text)
        return clean_text.strip()
    
    def smart_truncate_v2(self, text, target_length=1500):
        """
        Multi-window context extraction based on keyword density
        Priority: Paragraphs with high geo + naker signals
        """
        if len(text) <= target_length:
            return text
        
        paragraphs = [p.strip() for p in text.split('\n\n') if len(p.strip()) > 50]
        
        if not paragraphs:
            return text[:target_length]
        
        scored_paragraphs = []
        
        for i, para in enumerate(paragraphs):
            score = 0
            para_lower = para.lower()
            
            # Geographic signals
            if any(p.search(para_lower) for p in self.compiled_patterns['strict_anchors']):
                score += 40
            elif any(p.search(para_lower) for p in self.compiled_patterns['districts']):
                score += 30
            elif re.search(r'\bbandung\b', para_lower):
                score += 15
            
            # Naker signals
            naker_count = sum(1 for p in self.compiled_patterns['naker_positif'] if p.search(para_lower))
            naker_count += sum(1 for p in self.compiled_patterns['naker_negatif'] if p.search(para_lower))
            naker_count += sum(1 for p in self.compiled_patterns['naker_isu'] if p.search(para_lower))
            
            score += naker_count * 15
            
            # Prefer earlier paragraphs (context setting)
            if i < 3:
                score += 10
            
            scored_paragraphs.append((score, i, para))
        
        # Take top 3 highest-scoring paragraphs
        top_paras = sorted(scored_paragraphs, key=lambda x: x[0], reverse=True)[:3]
        # Re-sort by original position to maintain flow
        top_paras = sorted(top_paras, key=lambda x: x[1])
        
        selected_text = "\n\n".join([p[2] for p in top_paras])
        
        if len(selected_text) > target_length:
            return selected_text[:target_length] + "..."
        
        return selected_text
    
    async def interrogate_with_llama(self, article_text, task_log):
        """
        Enhanced SLM interrogation with few-shot prompting (5-7 examples)
        """
        truncated_text = self.smart_truncate_v2(article_text)
        
        task_log.append(" [>] Mengirim Smart Context Window ke Hakim SLM (Ollama)...")
        
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
- Status Geofencing HARUS "Valid Kota Bandung" JIKA peristiwa terjadi secara fisik di Kota Bandung.
- Pekerja NAIK & Pengangguran TURUN jika: Pembukaan pabrik, job fair besar, ekspansi bisnis, rekrutmen massal.
- Pekerja TURUN & Pengangguran NAIK jika: PHK massal, pabrik tutup, gulung tikar, rasionalisasi karyawan.
- Jika hanya membahas isu normatif (Tuntutan UMK, Aturan THR, Demo tanpa PHK), status keduanya adalah '3 Tetap'.
- TOLAK JIKA (Irrelevant): Hanya berita info lowongan kerja individual (cara melamar, link loker, syarat CPNS) yang tidak berdampak pada ekonomi makro. Atau jika peristiwa tidak terjadi di Kota Bandung.
- TOLAK JIKA menyebut "Kabupaten Bandung" tanpa menyebut "Kota Bandung" secara eksplisit.

CONTOH ANALISIS YANG BENAR:

Contoh 1:
Teks: "PHK massal menimpa 500 karyawan pabrik tekstil di Kecamatan Cicendo, Kota Bandung akibat penurunan order ekspor."
Output:
{{
  "status_geografi": "Valid Kota Bandung",
  "ringkasan_berita": "Pabrik tekstil di Kecamatan Cicendo, Kota Bandung melakukan PHK massal terhadap 500 karyawan karena order ekspor menurun drastis.",
  "dampak_bekerja": "2 Turun",
  "dampak_pengangguran": "1 Naik",
  "kategori_kbli": "C. Industri Pengolahan",
  "confidence_score": 95
}}

Contoh 2:
Teks: "Bupati Bandung meresmikan job fair di Soreang dengan 50 perusahaan peserta."
Output:
{{
  "status_geografi": "Out of Jurisdiction",
  "ringkasan_berita": "Event terjadi di Kabupaten Bandung (Soreang), bukan Kota Bandung.",
  "dampak_bekerja": "3 Tetap",
  "dampak_pengangguran": "3 Tetap",
  "kategori_kbli": "N/A",
  "confidence_score": 0
}}

Contoh 3:
Teks: "Disnaker Kota Bandung menggelar job fair dengan 100 perusahaan, diperkirakan tersedia 3000 lowongan kerja di kawasan Gedebage."
Output:
{{
  "status_geografi": "Valid Kota Bandung",
  "ringkasan_berita": "Disnaker Kota Bandung menyelenggarakan job fair di Gedebage dengan 100 perusahaan peserta dan menyediakan 3000 lowongan kerja.",
  "dampak_bekerja": "1 Naik",
  "dampak_pengangguran": "2 Turun",
  "kategori_kbli": "N. Aktivitas Penyewaan dan Sewa Guna Usaha Tanpa Hak Opsi, Ketenagakerjaan, Agen Perjalanan dan Penunjang Usaha Lainnya",
  "confidence_score": 88
}}

Contoh 4:
Teks: "Ribuan buruh di Kota Bandung demo menuntut kenaikan UMK 2026 sebesar 10 persen di depan Gedung Sate."
Output:
{{
  "status_geografi": "Valid Kota Bandung",
  "ringkasan_berita": "Ribuan buruh di Kota Bandung melakukan aksi demonstrasi menuntut kenaikan Upah Minimum Kota sebesar 10 persen di depan Gedung Sate.",
  "dampak_bekerja": "3 Tetap",
  "dampak_pengangguran": "3 Tetap",
  "kategori_kbli": "N/A (Isu Normatif)",
  "confidence_score": 90
}}

Contoh 5:
Teks: "Perusahaan manufaktur di kawasan industri Bandung membuka 200 posisi operator produksi dengan gaji kompetitif."
Output:
{{
  "status_geografi": "Valid Kota Bandung",
  "ringkasan_berita": "Perusahaan manufaktur di kawasan industri Kota Bandung membuka rekrutmen 200 posisi operator produksi.",
  "dampak_bekerja": "1 Naik",
  "dampak_pengangguran": "2 Turun",
  "kategori_kbli": "C. Industri Pengolahan",
  "confidence_score": 85
}}

Contoh 6:
Teks: "Rasionalisasi karyawan dilakukan PT XYZ Bandung, 120 pekerja kontrak tidak diperpanjang kontraknya akibat efisiensi operasional."
Output:
{{
  "status_geografi": "Valid Kota Bandung",
  "ringkasan_berita": "PT XYZ Bandung melakukan rasionalisasi dengan tidak memperpanjang kontrak 120 pekerja sebagai bagian dari efisiensi operasional.",
  "dampak_bekerja": "2 Turun",
  "dampak_pengangguran": "1 Naik",
  "kategori_kbli": "C. Industri Pengolahan",
  "confidence_score": 92
}}

Contoh 7:
Teks: "Lowongan CPNS 2026 untuk Pemkot Bandung dibuka dengan formasi 50 orang. Syarat: S1, IPK min 3.0. Daftar di link ini."
Output:
{{
  "status_geografi": "Irrelevant",
  "ringkasan_berita": "Berita hanya berisi informasi cara pendaftaran CPNS, bukan fenomena ketenagakerjaan makro.",
  "dampak_bekerja": "3 Tetap",
  "dampak_pengangguran": "3 Tetap",
  "kategori_kbli": "N/A",
  "confidence_score": 0
}}

SEKARANG ANALISIS TEKS BERIKUT_
{truncated_text}
"""

        payload = {
            "model": self.model_name,
            "prompt": custom_prompt,
            "format": "json",
            "stream": False
        }

        # Retry mechanism (2 attempts)
        for attempt in range(2):
            try:
                response = await asyncio.to_thread(requests.post, self.ollama_url, json=payload, timeout=120)
                if response.status_code == 200:
                    raw_json = response.json().get("response", "{}")
                    try:
                        return json.loads(raw_json)
                    except json.JSONDecodeError as e:
                        task_log.append(f"    [!] SLM JSON Parse Error: {str(e)[:100]}")
                        return {"Error": "Format SLM non-JSON"}
                else:
                    task_log.append(f"    [!] SLM Status {response.status_code}")
                    return {"Error": f"SLM Status {response.status_code}"}
            except requests.exceptions.RequestException as e:
                if attempt == 0:
                    task_log.append(f"    [!] SLM Timeout, retrying...")
                    await asyncio.sleep(5)
                else:
                    task_log.append(f"    [!] SLM Failed after 2 attempts")
                    return {"Error": "Daemon Ollama tertidur / Port tertutup."}

        return {"Error": "Unknown SLM error"}

    def save_checkpoint(self, final=False, checkpoint_num=None):
        """
        Save checkpoint with atomic write + backup mechanism
        
        Args:
            final (bool): Is this the final save?
            checkpoint_num (int): Checkpoint number for incremental saves
        """
        if not self.session_data:
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if final:
            filename = self.export_dir / f"bps_audit_naker_{timestamp}.xlsx"
        else:
            filename = self.export_dir / f"bps_audit_naker_{timestamp}_checkpoint{checkpoint_num}.xlsx"

        tmp_filename = filename.with_suffix('.tmp')

        df = pd.DataFrame(self.session_data)
        
        cols = [
            "Ringkasan Berita/Informasi Utama",
            "Sumber Berita (URL)",
            "Tanggal Berita",
            "Bekerja (1 Naik / 2 Turun / 3 Tetap)",
            "Pengangguran (1 Naik / 2 Turun / 3 Tetap)",
            "Kategori Lapangan Usaha (KBLI)",
            "Confidence Score (%)",
            "Status Geografi"
        ]
        
        df = df.rename(columns={
            "ringkasan_berita": "Ringkasan Berita/Informasi Utama",
            "URL": "Sumber Berita (URL)",
            "Tanggal Terbit Publikasi": "Tanggal Berita",
            "dampak_bekerja": "Bekerja (1 Naik / 2 Turun / 3 Tetap)",
            "dampak_pengangguran": "Pengangguran (1 Naik / 2 Turun / 3 Tetap)",
            "kategori_kbli": "Kategori Lapangan Usaha (KBLI)",
            "confidence_score": "Confidence Score (%)",
            "Status Geografi": "Status Geografi"
        })
        
        df = df[[c for c in cols if c in df.columns]]

        try:
            # Step 1: Write to temporary file
            writer = pd.ExcelWriter(tmp_filename, engine='xlsxwriter')
            df.to_excel(writer, index=False, sheet_name='Fenomena Naker')
            
            workbook = writer.book
            worksheet = writer.sheets['Fenomena Naker']
            
            # Dynamic column sizing
            for idx, col in enumerate(df.columns):
                max_length = max(
                    df[col].astype(str).apply(len).max(),
                    len(col)
                )
                worksheet.set_column(idx, idx, min(max_length + 2, 60))
            
            # Hyperlink for URL column
            if "Sumber Berita (URL)" in df.columns:
                url_idx = df.columns.get_loc("Sumber Berita (URL)")
                for row_num, url in enumerate(df["Sumber Berita (URL)"]):
                    if pd.notna(url) and str(url).startswith("http"):
                        worksheet.write_url(row_num + 1, url_idx, str(url), string="BACA ARTIKEL")
            
            writer.close()

            # Step 2: Validate tmp file
            try:
                pd.read_excel(tmp_filename, nrows=1)
            except Exception as e:
                print(f"\n [!!!] CRITICAL: Temporary file corrupted. Keeping backup.")
                # Emergency JSON dump
                json_backup = filename.with_suffix('.json')
                with open(json_backup, 'w', encoding='utf-8') as f:
                    json.dump(self.session_data, f, ensure_ascii=False, indent=2)
                print(f" [>] Emergency JSON backup saved: {json_backup.name}")
                return

            # Step 3: Rename tmp to final
            if tmp_filename.exists():
                tmp_filename.rename(filename)
                print(f"\n [] {'Final Export' if final else f'Checkpoint {checkpoint_num}'} saved: {filename.name}")

        except Exception as e:
            print(f"\n [!!!] SAVE ERROR: {e}")
            # Emergency JSON dump
            json_backup = filename.with_suffix('.json')
            with open(json_backup, 'w', encoding='utf-8') as f:
                json.dump(self.session_data, f, ensure_ascii=False, indent=2)
            print(f" [>] Emergency JSON backup saved: {json_backup.name}")

    def _extract_timestamp_from_filename(self, filename):
        """Extract timestamp from filename for merge precedence"""
        match = re.search(r'(\d{8}_\d{6})', filename)
        if match:
            return datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
        return datetime.min

    def merge_audit_files(self, file_paths=None, output_name=None):
        """
        Consolidate multiple BPS Naker audit files into single master file
        
        Strategy:
        1. Auto-detect all audit files in export_dir if no paths provided
        2. Load & parse each Excel file
        3. Deduplicate by URL (keep latest by filename timestamp)
        4. Validate schema consistency
        5. Sort by publication date
        6. Export consolidated file
        
        Parameters:
            file_paths (list): Specific files to merge, or None = auto-detect all
            output_name (str): Custom output filename
            
        Returns:
            dict: Statistics of merge operation
        """
        print("\n" + "="*75)
        print(" MERGE AUDIT FILES - STARTING CONSOLIDATION")
        print("="*75)

        # STEP 1: File Discovery
        if not file_paths:
            file_paths = sorted(
                [f for f in self.export_dir.glob("bps_audit_naker_*.xlsx") 
                 if "MASTER" not in f.name and "checkpoint" not in f.name]
            )
        
        if not file_paths:
            print(" [!] No audit files found in export directory.")
            return None

        print(f" [>] Found {len(file_paths)} audit files to merge...")

        # STEP 2: Load All Files
        all_data = []
        for fpath in file_paths:
            try:
                df = pd.read_excel(fpath)
                # Extract timestamp from filename for precedence
                timestamp = self._extract_timestamp_from_filename(fpath.name)
                df['_source_file'] = fpath.name
                df['_file_timestamp'] = timestamp
                all_data.append(df)
                print(f"    [] Loaded: {fpath.name} ({len(df)} records)")
            except Exception as e:
                print(f"    [!] Failed to load {fpath.name}: {e}")

        if not all_data:
            print(" [!] No valid files could be loaded.")
            return None

        # STEP 3: Concatenate
        combined_df = pd.concat(all_data, ignore_index=True)
        original_count = len(combined_df)

        # STEP 4: Deduplicate (keep latest by file timestamp)
        combined_df = combined_df.sort_values('_file_timestamp', ascending=False)
        combined_df = combined_df.drop_duplicates(subset=['Sumber Berita (URL)'], keep='first')
        deduplicated_count = len(combined_df)
        duplicates_removed = original_count - deduplicated_count

        # STEP 5: Sort by Publication Date
        if 'Tanggal Berita' in combined_df.columns:
            combined_df = combined_df.sort_values('Tanggal Berita', ascending=False)

        # STEP 6: Clean temporary columns
        combined_df = combined_df.drop(columns=['_source_file', '_file_timestamp'])

        # STEP 7: Export
        if not output_name:
            output_name = f"bps_audit_naker_MASTER_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        output_path = self.export_dir / output_name

        try:
            writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
            combined_df.to_excel(writer, index=False, sheet_name='Fenomena Naker')
            
            workbook = writer.book
            worksheet = writer.sheets['Fenomena Naker']
            
            # Dynamic column sizing
            for idx, col in enumerate(combined_df.columns):
                max_length = max(
                    combined_df[col].astype(str).apply(len).max(),
                    len(col)
                )
                worksheet.set_column(idx, idx, min(max_length + 2, 60))
            
            # Hyperlinks
            if "Sumber Berita (URL)" in combined_df.columns:
                url_idx = combined_df.columns.get_loc("Sumber Berita (URL)")
                for row_num, url in enumerate(combined_df["Sumber Berita (URL)"]):
                    if pd.notna(url) and str(url).startswith("http"):
                        worksheet.write_url(row_num + 1, url_idx, str(url), string="BACA ARTIKEL")
            
            writer.close()
            
        except Exception as e:
            print(f"\n [!!!] MERGE ERROR: {e}")
            return None

        # STEP 8: Summary Report
        print(f"\n {'='*75}")
        print(f" MERGE AUDIT FILES - SUMMARY")
        print(f" {'='*75}")
        print(f"  Files Merged          : {len(file_paths)}")
        print(f"  Total Records (Raw)   : {original_count}")
        print(f"  Duplicates Removed    : {duplicates_removed}")
        print(f"  Final Unique Records  : {deduplicated_count}")
        print(f"  Output File           : {output_path.name}")
        print(f" {'='*75}\n")

        return {
            'total_files_merged': len(file_paths),
            'total_records': deduplicated_count,
            'duplicates_removed': duplicates_removed,
            'output_file': output_path
        }

    def _build_search_query(self, site):
        base_query = f'site:{site} "Kota Bandung" (PHK OR "lowongan kerja" OR "job fair" OR "pabrik tutup" OR UMK OR buruh OR pengangguran OR "tenaga kerja" OR "padat karya")'
        if self.args.start:
            base_query += f' after:{self.args.start}'
        if self.args.end:
            base_query += f' before:{self.args.end}'
        return urllib.parse.quote(base_query)

    async def fetch_rss(self, site):
        query = self._build_search_query(site)
        rss_url = f"https://news.google.com/rss/search?q={query}&hl=id&gl=ID&ceid=ID:id"
        try:
            feed = await asyncio.to_thread(feedparser.parse, rss_url)
            return site, feed.entries
        except:
            return site, []

    async def fetch_with_backoff(self, page, url, max_retries=3):
        """Fetch with exponential backoff for rate limiting"""
        for attempt in range(max_retries):
            try:
                response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                if response and response.status in [429, 503]:
                    wait_time = 2 ** attempt
                    print(f"    [RATE LIMIT] Status {response.status}. Waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                return response
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(2)
        raise Exception("Max retries exceeded")

    async def process_article(self, context, entry, site, real_url):
        task_log = []
        link_text = self.format_hyperlink(real_url, "[BACA ARTIKEL]")
        
        async with self.browser_semaphore:
            async with self.state_lock:
                if real_url in self.session_active_urls:
                    return
                self.session_active_urls.add(real_url)

            # Pre-flight check
            is_rejected, reject_reason = self.is_rejected_preflight(entry.title, real_url)
            if is_rejected:
                task_log.append(f"\n  -> {entry.title[:60]}...")
                task_log.append(f"     [BLOCKED PRE-FLIGHT] {reject_reason}. {link_text}")
                await self._commit_to_permanent_blacklist(real_url)
                async with self.state_lock:
                    self.stats['rejected_preflight'] += 1
                async with self.print_lock:
                    print("\n".join(task_log))
                return

            # MIME check
            is_html, mime_reason = await asyncio.to_thread(self.verify_mime_type, real_url)
            if not is_html:
                task_log.append(f"\n  -> {entry.title[:60]}...")
                task_log.append(f"     [BLOCKED] {mime_reason}. {link_text}")
                await self._commit_to_permanent_blacklist(real_url)
                async with self.print_lock:
                    print("\n".join(task_log))
                return

            published_date = entry.get("published", "Tanggal Tidak Tersedia")
            task_log.append(f"\n  [>] Mengekstraksi [{site}]: {entry.title[:50]}...")

            page = await context.new_page()
            await page.route("**/*", self.network_interceptor)
            pacing_type = "fast"

            try:
                # Enhanced error handling
                try:
                    await self.fetch_with_backoff(page, real_url)
                    await page.wait_for_timeout(2000)
                except TimeoutError:
                    task_log.append(f"      [TIMEOUT] Page load >30s. {link_text}")
                    pacing_type = "fast"
                    return
                except PlaywrightError as e:
                    if "net::ERR_NAME_NOT_RESOLVED" in str(e):
                        task_log.append(f"      [DNS ERROR] Domain tidak bisa diresolve. {link_text}")
                    elif "net::ERR_CONNECTION_REFUSED" in str(e):
                        task_log.append(f"      [CONNECTION REFUSED] Server menolak. {link_text}")
                    else:
                        task_log.append(f"      [NETWORK ERROR] {str(e)[:100]}")
                    pacing_type = "fast"
                    return

                # Cloudflare handling
                try:
                    iframe = await page.wait_for_selector('iframe[src*="cloudflare"], #challenge-running', timeout=4000)
                    if iframe:
                        async with self.print_lock:
                            print(f"\a\n  [!!!] CLOUDFLARE TERDETEKSI pada {page.url[:40]}... Tahan & Centang Captcha!")
                        await page.wait_for_function(
                            "document.querySelector('iframe[src*=\"cloudflare\"]') === null && document.querySelector('#challenge-running') === null",
                            timeout=120000
                        )
                        await page.wait_for_timeout(3000)
                except:
                    pass

                await self.execute_hydration_scroll(page)
                content = await page.content()

                # Extract text
                config = Config()
                config.fetch_images = False
                article = Article(page.url, config=config, language='id')
                
                try:
                    article.set_html(content)
                    article.parse()
                    text_newspaper = article.text
                except:
                    text_newspaper = ""

                text_native = await self.extract_text_native(page)
                raw_text = text_newspaper if len(text_newspaper) > len(text_native) else text_native
                purified_text = self.clean_article_text(raw_text)
                char_count = len(purified_text)

                # Calculate relevance score with full text
                score, breakdown = self.calculate_relevance_score(entry.title, real_url, purified_text)
                
                task_log.append(f"      [SCORE] Relevance: {score}/100 | Geo: {breakdown.get('geo', 'N/A')} | Naker: {breakdown.get('naker', 'N/A')}")
                
                # Tiered decision
                if score >= 70:
                    # AUTO PASS - High confidence
                    task_log.append(f"      [AUTO-PASS] Score >= 70. Integritas: {char_count} kar.")
                    decision = "send_to_slm"
                elif score >= 50:
                    # CONDITIONAL - Check content quality
                    if char_count > 400:
                        task_log.append(f"      [CONDITIONAL] Score 50-69. Content sufficient ({char_count} kar).")
                        decision = "send_to_slm"
                    else:
                        task_log.append(f"      [SKIPPED] Score 50-69 but text too short ({char_count} kar).")
                        decision = "skip"
                elif score >= 30:
                    # BORDERLINE - Let SLM judge ambiguous cases
                    if char_count > 400:
                        task_log.append(f"      [BORDERLINE] Score 30-49. Sending to SLM for judgment.")
                        decision = "send_to_slm"
                    else:
                        task_log.append(f"      [SKIPPED] Score too low + insufficient text.")
                        decision = "skip"
                else:
                    # AUTO REJECT
                    task_log.append(f"      [AUTO-REJECT] Score < 30. {link_text}")
                    decision = "reject"

                if decision == "send_to_slm":
                    audit_result = await self.interrogate_with_llama(purified_text, task_log)
                    
                    status_geo = audit_result.get("status_geografi", "Unknown")
                    confidence = audit_result.get("confidence_score", "N/A")
                    
                    task_log.append(f"      [SLM JUDGE] Geofencing: {status_geo} | Confidence: {confidence}%")

                    if "Out of Jurisdiction" not in status_geo and "Irrelevant" not in status_geo:
                        self.session_data.append({
                            "Tanggal Terbit Publikasi": published_date,
                            "URL": real_url,
                            "Status Geografi": status_geo,
                            "ringkasan_berita": audit_result.get("ringkasan_berita", ""),
                            "dampak_bekerja": audit_result.get("dampak_bekerja", ""),
                            "dampak_pengangguran": audit_result.get("dampak_pengangguran", ""),
                            "kategori_kbli": audit_result.get("kategori_kbli", ""),
                            "confidence_score": confidence
                        })
                        
                        task_log.append(f"      [SECURED] Lolos audit NAKER BPS & SLM. {link_text}")
                        pacing_type = "normal"
                        
                        # Update stats
                        async with self.state_lock:
                            self.stats['accepted_slm'] += 1
                            if "Kota Bandung" in status_geo:
                                self.stats['geo_explicit'] += 1
                            
                            dampak_bekerja = audit_result.get("dampak_bekerja", "")
                            if "1" in str(dampak_bekerja):
                                self.stats['dampak_positif'] += 1
                            elif "2" in str(dampak_bekerja):
                                self.stats['dampak_negatif'] += 1
                            else:
                                self.stats['dampak_isu'] += 1
                            
                            if isinstance(confidence, (int, float)):
                                self.stats['confidence_scores'].append(float(confidence))
                        
                        await self._commit_to_permanent_blacklist(real_url)
                    else:
                        task_log.append(f"      [REJECTED] SLM Menolak: {status_geo}. {link_text}")
                        pacing_type = "normal"
                        async with self.state_lock:
                            self.stats['rejected_slm'] += 1
                        await self._commit_to_permanent_blacklist(real_url)
                
                elif decision == "skip":
                    async with self.state_lock:
                        self.stats['rejected_lexical'] += 1
                
                elif decision == "reject":
                    async with self.state_lock:
                        self.stats['rejected_lexical'] += 1
                    await self._commit_to_permanent_blacklist(real_url)

            except Exception as e:
                if "TargetClosedError" not in str(e) and "has been closed" not in str(e):
                    task_log.append(f"      [ERROR Ekstraksi] {e}. {link_text}")
                    pacing_type = "fast"
            finally:
                if not page.is_closed():
                    await page.close()

            if task_log:
                async with self.print_lock:
                    print("\n".join(task_log))

            if pacing_type == "normal":
                await asyncio.sleep(random.uniform(3.0, 5.0))
            else:
                await asyncio.sleep(random.uniform(1.5, 2.5))
            
            # Check shutdown request
            if self.shutdown_requested:
                raise KeyboardInterrupt

    def print_session_summary(self):
        """Print comprehensive session statistics"""
        print("\n" + "="*75)
        print(" SESSION SUMMARY (NAKER SENTINEL V66)")
        print("="*75)
        print(f"  Total Targets Scanned     : {self.stats['total_scanned']}")
        print(f"  Rejected Pre-flight       : {self.stats['rejected_preflight']} ({self.stats['rejected_preflight']/max(self.stats['total_scanned'],1)*100:.1f}%)")
        print(f"  Lexical Filter Rejected   : {self.stats['rejected_lexical']} ({self.stats['rejected_lexical']/max(self.stats['total_scanned'],1)*100:.1f}%)")
        print(f"  SLM Accepted (Saved)      : {self.stats['accepted_slm']} ({self.stats['accepted_slm']/max(self.stats['total_scanned'],1)*100:.1f}%)")
        print(f"  SLM Rejected              : {self.stats['rejected_slm']} ({self.stats['rejected_slm']/max(self.stats['total_scanned'],1)*100:.1f}%)")
        
        if self.stats['confidence_scores']:
            avg_conf = sum(self.stats['confidence_scores']) / len(self.stats['confidence_scores'])
            print(f"  Avg Confidence Score      : {avg_conf:.1f}%")
        
        print(f"\n  Geographic Coverage:")
        print(f"    Kota Bandung Eksplisit  : {self.stats['geo_explicit']} ({self.stats['geo_explicit']/max(self.stats['accepted_slm'],1)*100:.1f}%)")
        
        print(f"\n  NAKER Breakdown:")
        print(f"    ↑ Dampak Positif        : {self.stats['dampak_positif']}")
        print(f"    ↓ Dampak Negatif        : {self.stats['dampak_negatif']}")
        print(f"     Isu Normatif          : {self.stats['dampak_isu']}")
        
        print(f"\n  Checkpoint Saves          : Auto (every 50 articles)")
        print("="*75)

    async def run(self):
        if not self.check_ollama():
            sys.exit(1)

        # Handle merge command
        if self.args.merge:
            self.merge_audit_files()
            return

        print("\n" + "="*75)
        print("  SURGICAL PRECISION DEBUGGER V66 (NAKER EDITION)")
        print("  THE PRECISION SENTINEL - Zero False Positive/Negative Target")
        if self.args.start or self.args.end:
            print(f"  Rentang Waktu: {self.args.start} hingga {self.args.end}")
        print("="*75)

        self.prepare_workspace()

        print("\n[RADAR] Mengumpulkan heuristik intelijen Ketenagakerjaan dari seluruh sumber...")
        rss_tasks = [self.fetch_rss(site) for site in self.sites]
        rss_results = await asyncio.gather(*rss_tasks)

        all_entries = []
        for site, entries in rss_results:
            all_entries.extend([(site, e) for e in entries[:15]])

        # PRE-FILTER: Remove already visited URLs BEFORE spawning tasks
        new_targets = []
        cached_count = 0
        
        for site, entry in all_entries:
            real_url = self._decode_google_url(entry.link)
            if real_url in self.permanent_visited_urls:
                cached_count += 1
            else:
                new_targets.append((site, entry, real_url))

        self.stats['total_scanned'] = len(all_entries)

        print(f"[RADAR] Menemukan {len(all_entries)} target potensial ({cached_count} sudah diaudit sebelumnya, {len(new_targets)} target baru).")
        
        if not new_targets:
            print("[] Semua target hari ini sudah diekstraksi. Menutup sistem dengan anggun.")
            sys.exit(0)

        print("[RADAR] Memulai pembedahan asinkron untuk target NAKER baru...\n")

        context = None
        try:
            async with async_playwright() as p:
                context = await p.chromium.launch_persistent_context(
                    user_data_dir=str(self.workspace_dir),
                    channel="msedge",
                    headless=False,
                    args=["--disable-blink-features=AutomationControlled"]
                )

                tasks = []
                for site, entry, real_url in new_targets:
                    tasks.append(self.process_article(context, entry, site, real_url))

                # Process with progress bar
                print("\n[PROGRESS] Processing articles...")
                with async_tqdm(total=len(tasks), desc="Articles", unit="article") as pbar:
                    for i, task in enumerate(asyncio.as_completed(tasks)):
                        await task
                        pbar.update(1)
                        
                        # Incremental checkpoint every 50 articles
                        if (i + 1) % 50 == 0:
                            checkpoint_num = (i + 1) // 50
                            self.save_checkpoint(final=False, checkpoint_num=checkpoint_num)
                            print(f"\n  [] Incremental checkpoint {checkpoint_num} saved ({i+1} articles)")

        except KeyboardInterrupt:
            print("\n\n[!] INTERUPSI (CTRL+C) TERDETEKSI. Mengamankan data dengan tenang...")
        except Exception as e:
            print(f"\n[FATAL ERROR]: {e}")
        finally:
            # Final save
            self.save_checkpoint(final=True)
            self._save_visited_urls_delta()
            
            # Print summary
            self.print_session_summary()
            
            if context:
                try:
                    await context.close()
                except:
                    pass
            
            sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BPS Naker Investigative Scraper V66")
    parser.add_argument('--mode', type=str, default='live', help='Mode eksekusi (live/history)')
    parser.add_argument('--start', type=str, default='', help='Format: YYYY-MM-DD')
    parser.add_argument('--end', type=str, default='', help='Format: YYYY-MM-DD')
    parser.add_argument('--merge', action='store_true', help='Merge all audit files into master file')
    
    args = parser.parse_args()
    
    asyncio.run(BPS_Naker_Sentinel(args).run())
