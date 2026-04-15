from playwright.sync_api import sync_playwright
import os

# Konfigurasi Path
AUTH_STATE_PATH = "config/auth_state.json"

def generate_auth_state():
    """
    Membuka browser untuk otentikasi manual pertama kali dan menyimpan state (cookies).
    Pendekatan ini jauh lebih aman daripada hardcode username/password di dalam skrip.
    """
    print("[INFO] Menginisiasi lingkungan otentikasi yang aman...")
    
    with sync_playwright() as p:
        # Kita gunakan headless=False agar bisa melihat layarnya untuk login
        browser = p.chromium.launch(headless=False) 
        context = browser.new_context()
        page = context.new_page()

        try:
            print("[ACTION] Membuka halaman login Instagram...")
            page.goto("https://www.instagram.com/accounts/login/")
            
            # Skrip akan berhenti sejenak
            # Memberi waktu untuk mengetik username dan password dummy di browser.
            input("\n[WAITING] Silakan login secara manual di browser yang terbuka. \nJika sudah berhasil masuk ke beranda (Home) Instagram, tekan ENTER di terminal ini...")

            # Menyimpan sesi setelah menekan ENTER
            context.storage_state(path=AUTH_STATE_PATH)
            print(f"[SUCCESS] Auth state berhasil disimpan secara lokal di: {AUTH_STATE_PATH}")
            print("[INFO] File ini berisi token sensitif. JANGAN pernah mengunggahnya ke GitHub/publik.")

        except Exception as e:
            print(f"[ERROR] Terjadi anomali saat menyimpan auth state: {e}")
            
        finally:
            browser.close()

if __name__ == "__main__":
    # Memastikan folder config/ ada sebelum menyimpan
    os.makedirs("config", exist_ok=True)
    generate_auth_state()