import json
import time
import random
import os
from playwright.sync_api import sync_playwright, TimeoutError
import pandas as pd
from datetime import datetime

class BPSInstagramScraper:
    """
    Advanced Scraper Engine with Diagnostic & Forensic Logging.
    Built for BPS data extraction standards.
    """
    def __init__(self, config_path="config/targets.json", auth_path="config/auth_state.json"):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        self.auth_path = auth_path
        self.raw_data_path = "data/raw/"
        self.log_path = "logs/"
        
        os.makedirs(self.raw_data_path, exist_ok=True)
        os.makedirs(self.log_path, exist_ok=True)

    def _human_delay(self):
        """Stochastic delays to mitigate rate-limiting."""
        time.sleep(random.uniform(4, 8))

    def scrape_profile(self, username):
        """Extract post URLs with iterative scrolling to bypass lazy-loading."""
        results = [] 
        
        with sync_playwright() as p:
            print(f"\n[INFO] Menginisiasi audit profil: @{username}")
            
            browser = p.chromium.launch(
                headless=False, 
                args=["--disable-blink-features=AutomationControlled"]
            ) 
            
            context = browser.new_context(
                storage_state=self.auth_path,
                viewport={'width': 1280, 'height': 720},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            
            try:
                page.goto(f"https://www.instagram.com/{username}/")
                print(f"[TRACE] Menunggu perenderan DOM awal untuk @{username}...")
                page.wait_for_selector("main", timeout=15000) 
                time.sleep(random.uniform(3, 5))
                
                # VARIABEL KENDALI SCROLL
                target_count = self.config['scraper_settings']['max_posts_per_account']
                unique_links = set()
                scroll_attempts = 0
                max_scroll_attempts = 15 # Jaring pengaman agar tidak infinite loop

                print(f"[TRACE] Memulai ekstraksi iteratif (Target: {target_count} post)...")

                while len(unique_links) < target_count and scroll_attempts < max_scroll_attempts:
                    # Ambil elemen yang ada di layar saat ini
                    posts = page.query_selector_all("a[href*='/p/'], a[href*='/reel/']")
                    
                    # Mencatat jumlah awal sebelum ditambahkan data baru
                    initial_count = len(unique_links)
                    
                    for post in posts:
                        link = post.get_attribute('href')
                        if link:
                            clean_link = link.split('?')[0] 
                            if clean_link.startswith('/'):
                                unique_links.add(f"https://www.instagram.com{clean_link}")
                            else:
                                unique_links.add(clean_link)
                                
                    print(f"[TRACE] Scroll Iterasi {scroll_attempts + 1}: Terkumpul {len(unique_links)}/{target_count} tautan unik.")
                    
                    if len(unique_links) >= target_count:
                        print("[SUCCESS] Target kuota tercapai.")
                        break
                        
                    # Deteksi anomali: Jika di-scroll tapi tidak ada link baru, mungkin mentok (End of Page)
                    if len(unique_links) == initial_count and scroll_attempts > 2:
                        print("[WARNING] Tidak mendeteksi post baru. Mencoba scroll lebih dalam...")
                        page.mouse.wheel(0, 2000)
                    else:
                        # Scroll natural untuk memicu muatan data baru
                        page.mouse.wheel(0, 1000)
                    
                    # Berikan jeda stokastik agar terlihat seperti manusia membaca
                    time.sleep(random.uniform(2.5, 5.0))
                    scroll_attempts += 1

                # Konversi hasil akhirnya
                final_links = list(unique_links)[:target_count]
                print(f"[SUCCESS] Total akhir: {len(final_links)} entri data diamankan dari @{username}.")
                
                for link in final_links:
                    results.append({
                        "institution": username,
                        "post_url": link,
                        "scraped_at": datetime.now().isoformat()
                    })

            except TimeoutError:
                print(f"[ERROR] Timeout. Halaman @{username} tidak merespons dalam batas waktu.")
            except Exception as e:
                print(f"[CRITICAL] Kegagalan sistemik pada @{username}: {e}")
            finally:
                browser.close()
        
        return results

    def run(self):
        """Orchestrate multi-target scraping operation."""
        all_data = []
        for target in self.config['targets']:
            data = self.scrape_profile(target['username'])
            
            # SAFETY NET: Pastikan data tidak None sebelum diekstensi
            if data: 
                all_data.extend(data)
                
            self._human_delay()

        if all_data:
            df = pd.DataFrame(all_data)
            filename = f"{self.raw_data_path}raw_collection_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            df.to_csv(filename, index=False)
            print(f"\n[FINAL] Ekstraksi selesai. Data mentah tersimpan di: {filename}")
        else:
            print("\n[FINAL] Misi dibatalkan. Tidak ada data yang berhasil diekstrak.")

if __name__ == "__main__":
    scraper = BPSInstagramScraper()
    scraper.run()