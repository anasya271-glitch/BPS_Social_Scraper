import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import urllib3
import time
import os
import logging

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TradePhenomenonExtractor:
    """
    Standard: Audit-Ready & Contextual.
    Mengekstrak metrik perdagangan dan rute geografis dari situs perusahaan.
    """
    def __init__(self, input_file, output_file):
        logging.info("Memuat data hasil Automated Dorking...")
        self.df = pd.read_csv(input_file) if input_file.endswith('.csv') else pd.read_excel(input_file)
        self.output_file = output_file
        
        # Kamus Leksikal Intelijen Geografis (G20, ASEAN, Target Pasar Utama)
        self.target_markets = [
            'usa', 'america', 'united states', 'europe', 'japan', 'china', 
            'korea', 'middle east', 'australia', 'singapore', 'malaysia', 
            'germany', 'uk', 'united kingdom', 'africa', 'asia', 'global'
        ]
        
        # Kamus Metrik Kapasitas
        self.capacity_metrics = [r'\b\d+\s*tons\b', r'\b\d+\s*teu\b', r'\b\d+\s*containers\b', r'capacity\s*of\s*\d+']

    def extract_phenomenon(self, url):
        """Melakukan Deep Semantic Crawling untuk menemukan fenomena ekspor/impor."""
        if pd.isna(url) or url == "TIDAK DITEMUKAN" or url == "-":
            return "-", "-"

        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            response = requests.get(url, headers=headers, timeout=8, verify=False)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                # Fokus pada tag paragraf dan list untuk menghindari noise dari navigasi web
                text_blocks = soup.find_all(['p', 'li', 'span', 'div'])
                clean_text = " ".join([block.get_text(separator=' ', strip=True).lower() for block in text_blocks])
                
                # 1. Ekstraksi Rute Geografis
                found_markets = set()
                for market in self.target_markets:
                    if re.search(r'\b' + market + r'\b', clean_text):
                        found_markets.add(market.title())
                
                # 2. Ekstraksi Kapasitas/Volume
                found_capacities = set()
                for pattern in self.capacity_metrics:
                    matches = re.findall(pattern, clean_text)
                    found_capacities.update(matches)

                # Formulasi Output Fenomena
                rute_ekspor = ", ".join(found_markets) if found_markets else "TIDAK ADA KLAIM PASAR"
                metrik_skala = ", ".join(found_capacities) if found_capacities else "TIDAK ADA METRIK"
                
                return rute_ekspor, metrik_skala
            else:
                return f"HTTP {response.status_code}", "-"
        except Exception:
            return "KONEKSI GAGAL", "-"

    def execute_extraction(self):
        logging.info("Memulai Ekstraksi Fenomena Perdagangan...")
        
        # Inisialisasi kolom baru
        self.df['Klaim_Rute_Pasar'] = ""
        self.df['Klaim_Kapasitas_Web'] = ""
        self.df['Indikator_Asimetri'] = ""

        total = len(self.df)
        for index, row in self.df.iterrows():
            perusahaan = row.get('Nama_Perusahaan', 'UNKNOWN')
            website = row.get('Website_Resmi (Prediksi)', '-')
            kategori_bps = str(row.get('Kategori', '')).upper()
            
            logging.info(f"[{index+1}/{total}] Membedah Semantik: {perusahaan}")
            
            rute, kapasitas = self.extract_phenomenon(website)
            
            self.df.at[index, 'Klaim_Rute_Pasar'] = rute
            self.df.at[index, 'Klaim_Kapasitas_Web'] = kapasitas
            
            # Analisis Diskursus Kritis (CDA) Otomatis
            asimetri = "SEJALAN"
            if kategori_bps == "IMPOR" and rute not in ["TIDAK ADA KLAIM PASAR", "KONEKSI GAGAL", "-", "HTTP 403", "HTTP 404"]:
                # Jika BPS mencatat mereka Importir, tapi web mereka mengklaim ekspor ke negara lain
                asimetri = "ANOMALI: IMPORTIR MENGKLAIM EKSPOR/RE-EKSPOR"
            elif kategori_bps == "EKSPOR" and rute == "TIDAK ADA KLAIM PASAR":
                asimetri = "UNDER-REPORTING DIGITAL / BROKER POTENTIAL"
                
            self.df.at[index, 'Indikator_Asimetri'] = asimetri
            
            time.sleep(1) # Jeda untuk etika scraping

        self.df.to_excel(self.output_file, index=False)
        logging.info(f"Ekskavasi Selesai. Laporan Fenomena disimpan di: {self.output_file}")
        os.startfile(os.path.abspath(self.output_file))

if __name__ == "__main__":
    # Gunakan file hasil dorking Anda sebagai input
    FILE_INPUT = "Hasil_Automated_Dorking_BPS.xlsx"
    FILE_OUTPUT = "Laporan_Fenomena_Perdagangan_Bandung.xlsx"
    
    if os.path.exists(FILE_INPUT):
        engine = TradePhenomenonExtractor(FILE_INPUT, FILE_OUTPUT)
        engine.execute_extraction()
    else:
        logging.error("File input tidak ditemukan.")