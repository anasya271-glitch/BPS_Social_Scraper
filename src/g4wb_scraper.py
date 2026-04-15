import pandas as pd
import time
import random
import re
import os
import logging
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class G4WBFastHarvester:
    """
    Standard: Audit-Ready, High-Speed, Surgical Extraction.
    Dilengkapi dengan Resource Blocker untuk kecepatan maksimal dan ekstraktor spesifik.
    """
    def __init__(self, output_path, max_pages=179):
        self.output_path = output_path
        self.max_pages = max_pages
        self.base_url = "https://www.go4worldbusiness.com/find"

    def sanitize_text(self, text):
        if not text: return "-"
        text_str = str(text).replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')
        clean_str = re.sub(r'\s+', ' ', text_str)
        clean_str = re.sub(r'(?i)Supplier From\s*', '', clean_str)
        return clean_str.strip()

    def block_aggressively(self, route):
        """
        SPEED OPTIMIZATION (Revisi):
        Hanya memblokir gambar, media, dan font. 
        Script & CSS dibiarkan hidup agar data bisa ter-render.
        """
        if route.request.resource_type in ["image", "media", "font"]:
            route.abort()
        else:
            route.continue_()

    def execute_harvest(self):
        logging.info(f"[SISTEM] Memulai Ekstraksi Bedah Cepat untuk {self.max_pages} Halaman...")
        all_results = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                viewport={'width': 1366, 'height': 768}
            )
            page = context.new_page()
            
            # Terapkan pemblokir resource untuk mempercepat pemuatan halaman
            page.route("**/*", self.block_aggressively)

            for current_page in range(1, self.max_pages + 1):
                logging.info(f"[MENYAPU HALAMAN] {current_page} / {self.max_pages}")
                
                target_url = f"{self.base_url}?searchText=bandung&pg_buyers=1&pg_suppliers={current_page}&_format=html&BuyersOrSuppliers=suppliers"
                
                try:
                    # Kembali ke networkidle agar JavaScript selesai bekerja
                    page.goto(target_url, wait_until="networkidle", timeout=45000)
                    
                    # SMART WAITING: Memaksa mesin menunggu maksimal 10 detik 
                    # sampai minimal satu kartu supplier benar-benar muncul di layar.
                    try:
                        page.wait_for_selector("div.search-result, div.supplier-card, div.product-item", timeout=10000)
                    except Exception:
                        logging.warning("Elemen kartu supplier tidak muncul tepat waktu. Mungkin halaman kosong.")

                    soup = BeautifulSoup(page.content(), 'html.parser')
                    page_data = []
                    
                    supplier_cards = soup.find_all('div', class_=re.compile(r'search-result|supplier-card|product-item'))
                    if not supplier_cards:
                        supplier_cards = soup.find_all('li', class_=re.compile(r'row|item'))

                    for card in supplier_cards:
                        try:
                            # 1. NAMA PRODUK (Menggunakan class spesifik temuan Anda)
                            title_elem = card.find(['h2', 'a'], class_=re.compile(r'product-title|entity-row-title'))
                            if not title_elem: # Fallback jika class berubah
                                title_elem = card.find(['h2', 'h3'])
                            title = self.sanitize_text(title_elem.get_text(separator=' ')) if title_elem else "-"
                            
                            # 2. LOKASI
                            loc_elem = card.find(string=re.compile(r'Supplier From', re.IGNORECASE))
                            location = self.sanitize_text(loc_elem.parent.get_text(separator=' ')) if loc_elem and loc_elem.parent else "-"
                            
                            # 3. DESKRIPSI (Menggunakan class text-wrap temuan Anda)
                            desc_elem = card.find('div', class_=re.compile(r'text-wrap'))
                            if not desc_elem: # Fallback ke paragraf jika text-wrap tidak ada
                                desc_elem = card.find('p')
                            # Menggunakan separator=' ' sangat penting agar <br> berubah menjadi spasi
                            desc = self.sanitize_text(desc_elem.get_text(separator=' ')) if desc_elem else "-"
                            
                            # 4. HARGA
                            price_elem = card.find('div', class_=re.compile(r'product-price'))
                            price = self.sanitize_text(price_elem.get_text(separator=' ')) if price_elem else "-"
                            
                            # 5. TANGGAL
                            date_elem = card.find('div', class_=re.compile(r'text-right'))
                            date_posted = self.sanitize_text(date_elem.get_text(separator=' ')) if date_elem else "-"
                            
                            page_data.append({
                                'Nama_Produk_Perusahaan': title,
                                'Lokasi': location,
                                'Harga_Ditawarkan': price,
                                'Tanggal_Aktivitas': date_posted,
                                # Menyimpan deskripsi utuh tanpa dipotong
                                'Deskripsi_Profil': desc 
                            })
                        except Exception:
                            continue
                            
                    if page_data:
                        all_results.extend(page_data)
                        logging.info(f" -> Berhasil mengamankan {len(page_data)} entitas bersih.")
                    else:
                        logging.warning(" -> Tidak ada entitas ditemukan di halaman ini.")

                except Exception as e:
                    logging.error(f"Kegagalan navigasi pada halaman {current_page}: {e}")

                # SPEED OPTIMIZATION: Jeda diturunkan menjadi 1.5 - 3.5 detik.
                # Karena kita tidak memuat gambar/iklan, server mereka tidak akan terlalu curiga.
                time.sleep(random.uniform(1.5, 3.5))

            browser.close()

        if all_results:
            df = pd.DataFrame(all_results).drop_duplicates()
            df.to_excel(self.output_path, index=False)
            logging.info(f"[OPERASI SUKSES] {len(df)} Data intelijen diamankan di: {self.output_path}")
            os.startfile(os.path.abspath(self.output_path))
        else:
            logging.error("Gagal menarik data.")

if __name__ == "__main__":
    FILE_OUTPUT = "G4WB_Bandung_Surgical_Fast.xlsx"
    # Target dimaksimalkan 179 halaman
    harvester = G4WBFastHarvester(FILE_OUTPUT, max_pages=179) 
    harvester.execute_harvest()