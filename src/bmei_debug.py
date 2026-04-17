import os
import asyncio
from pathlib import Path
from playwright.async_api import async_playwright

class BPS_HAR_Debugger:
    def __init__(self):
        self.edge_source_dir = f"C:\\Users\\{os.getlogin()}\\AppData\\Local\\Microsoft\\Edge\\User Data"
        self.workspace_dir = Path.cwd() / "data" / "edge_workspace"
        os.makedirs(self.workspace_dir, exist_ok=True)

    def prepare_workspace(self):
        print(" [>] Menyiapkan Ruang Isolasi Browser untuk Debugging...")
        source = Path(self.edge_source_dir) / "Default"
        target = self.workspace_dir / "Default"
        os.system(f'robocopy "{source}" "{target}" /E /XF SingletonLock lock /R:1 /W:1 >nul 2>&1')

    async def run(self):
        self.prepare_workspace()
        
        print("\n" + "="*60)
        print(" BPS NETWORK DIAGNOSTIC | HAR RECORDER")
        print("="*60)
        
        async with async_playwright() as p:
            context = await p.chromium.launch_persistent_context(
                user_data_dir=str(self.workspace_dir), 
                channel="msedge", 
                headless=False,
                args=["--disable-blink-features=AutomationControlled"],
                record_har_path="BPS_Debug_Network.har"
            )
            
            page = await context.new_page()
            
            print("\n[1] Mengakses narasi.tv secara langsung (Bypass RSS)...")
            try:
                await page.goto("https://narasi.tv", wait_until="domcontentloaded", timeout=30000)
                print("    Berhasil memuat narasi.tv! Menunggu 5 detik untuk merekam traffic...")
                await page.wait_for_timeout(5000)
            except Exception as e:
                print(f"    Gagal memuat narasi.tv: {e}")

            print("\n[2] Menguji Cloudflare pada kabarbandung.pikiran-rakyat.com...")
            test_url = "https://kabarbandung.pikiran-rakyat.com/kabar-bandung/pr-41110131803/viral-di-tiktok-rshs-klarifikasi-kasus-bayi-diserahkan-ke-orang-lain"
            try:
                await page.goto(test_url, wait_until="domcontentloaded", timeout=30000)
                print("\a    [!] Halaman terbuka. Silakan amati atau centang Cloudflare jika muncul.")
                print("    Browser akan tetap terbuka selama 45 detik untuk merekam hasil verifikasi...")
                await page.wait_for_timeout(45000)
            except Exception as e:
                print(f"    Gagal memuat Pikiran Rakyat: {e}")

            await context.close()
            print("\n[v] Selesai. File HAR telah berhasil disimpan sebagai 'BPS_Debug_Network.har' di folder Anda.")

if __name__ == "__main__":
    asyncio.run(BPS_HAR_Debugger().run())
