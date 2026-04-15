import os
import time
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Memuat variabel lingkungan (Environment Variables) secara senyap
load_dotenv()

class BPSComtradeMacroEngine:
    """
    V1.0 Comtrade Macro-Engine
    Fokus: Pakaian Jadi (HS 61 & 62), Data Bulanan, Staggered Fetching Protocol.
    """
    def __init__(self):
        # Mengambil kunci dari brankas .env
        self.api_key = os.getenv("COMTRADE_API_KEY")
        
        # Endpoint API V1 PBB: C (Commodities) / M (Monthly) / HS (Harmonized System)
        self.base_url = "https://comtradeapi.un.org/data/v1/get/C/M/HS"
        
        self.export_dir = Path.cwd() / "data" / "exports"
        os.makedirs(self.export_dir, exist_ok=True)
        
        # Parameter BPS (Fokus Pakaian Jadi)
        self.reporter_code = "360" # Indonesia
        self.partner_code = "0"    # World (Seluruh Dunia)
        self.flow_codes = "M,X"    # Import (M) & Export (X)
        self.hs_codes = "61,62"    # Pakaian Rajutan (61) & Non-Rajutan (62)
        
        self.macro_data = []

    def _generate_monthly_periods(self, year):
        """Menghasilkan format periode YYYYMM (Contoh: 202301, 202302)."""
        return ",".join([f"{year}{str(month).zfill(2)}" for month in range(1, 13)])

    def fetch_with_backoff(self, url, headers, max_retries=5):
        """
        Exponential Backoff Protocol: 
        Merespons galat peladen secara elegan tanpa memutus eksekusi.
        """
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers, timeout=30)
                
                # 200: Sukses | 429: Terlalu banyak permintaan
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    wait_time = (2 ** attempt) * 2
                    print(f"    [!] Rate Limit tercapai. Mesin melakukan hibernasi selama {wait_time} detik...")
                    time.sleep(wait_time)
                else:
                    print(f"    [ERROR] Penolakan Peladen (Status: {response.status_code}): {response.text[:100]}")
                    return None
            except requests.exceptions.RequestException as e:
                print(f"    [ERROR] Gangguan Koneksi Jaringan: {e}")
                time.sleep(5)
                
        print("    [FATAL] Gagal menembus peladen setelah percobaan maksimal.")
        return None

    def collect_historical_data(self, start_year, end_year):
        if not self.api_key or self.api_key == "masukkan_kunci_primary_anda_di_sini_tanpa_tanda_kutip":
            print("\n[FATAL] API Key tidak ditemukan. Silakan periksa file .env Anda.")
            return False

        headers = {
            "Ocp-Apim-Subscription-Key": self.api_key,
            "Cache-Control": "no-cache"
        }

        print("\n[MENGINISIASI KONEKSI KE PELADEN UN COMTRADE...]")
        
        # Staggered Fetching: Iterasi per tahun untuk mencegah Overload API
        for year in range(start_year, end_year + 1):
            periods = self._generate_monthly_periods(year)
            print(f"  [>] Mengekstrak Laporan Kepabeanan Tahun {year}...")
            
            query_url = (
                f"{self.base_url}?"
                f"reporterCode={self.reporter_code}&"
                f"partnerCode={self.partner_code}&"
                f"period={periods}&"
                f"cmdCode={self.hs_codes}&"
                f"flowCode={self.flow_codes}"
            )
            
            data = self.fetch_with_backoff(query_url, headers)
            
            if data and "data" in data:
                records = data["data"]
                if not records:
                    print(f"      [-] Tidak ada catatan perdagangan untuk tahun {year}.")
                    continue
                    
                print(f"      [+] Berhasil menyerap {len(records)} baris matriks.")
                
                for record in records:
                    self.macro_data.append({
                        "Periode (Tahun-Bulan)": record.get("period"),
                        "Arus Perdagangan": record.get("flowDesc"),
                        "Kode HS": record.get("cmdCode"),
                        "Deskripsi Komoditas": record.get("cmdDesc"),
                        "Berat Bersih (Kg)": record.get("netWgt"),
                        "Nilai Transaksi (USD)": record.get("primaryValue")
                    })
            else:
                print(f"      [!] Gagal memvalidasi muatan data untuk tahun {year}.")
                
            # Jeda sopan santun untuk menghormati API PBB
            time.sleep(2)
            
        return True

    def save_to_excel(self):
        if not self.macro_data:
            return print("\n[!] Resolusi Nol. Tidak ada data makro yang berhasil diekstrak.")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = self.export_dir / f"bps_macro_pakaian_jadi_{timestamp}.xlsx"
        
        df = pd.DataFrame(self.macro_data)
        
        # Konversi tipe data agar Excel bisa menjumlahkan angkanya
        df["Berat Bersih (Kg)"] = pd.to_numeric(df["Berat Bersih (Kg)"], errors='coerce')
        df["Nilai Transaksi (USD)"] = pd.to_numeric(df["Nilai Transaksi (USD)"], errors='coerce')
        
        # Penataan Rectangularization via XlsxWriter
        writer = pd.ExcelWriter(filename, engine='xlsxwriter')
        df.to_excel(writer, index=False, sheet_name='Data Makro UN')
        
        workbook  = writer.book
        worksheet = writer.sheets['Data Makro UN']
        
        header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
        num_format = workbook.add_format({'num_format': '#,##0'})
        usd_format = workbook.add_format({'num_format': '$#,##0.00'})
        
        # Menerapkan format visual
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
            worksheet.set_column(col_num, col_num, 20)
            
        worksheet.set_column('E:E', 20, num_format) # Kolom Berat
        worksheet.set_column('F:F', 20, usd_format) # Kolom USD
            
        writer.close()
        
        print(f"\n[!] DATA VAULT SECURED: Makro-Data Pakaian Jadi berhasil diarsipkan.")
        print("    Membuka Microsoft Excel secara mandiri...")
        
        try:
            if os.name == 'nt':
                abs_path = os.path.abspath(filename)
                os.system(f'start "" "{abs_path}"')
        except Exception as e:
            print(f"    [Peringatan] Gagal memicu Excel otomatis: {e}")

    def run(self):
        print("\n" + "="*75)
        print(" BPS MACRO-ENGINE V1 | UN COMTRADE INTEGRATION")
        print(" Target: Pakaian Jadi (HS 61 & 62) | Arus: Ekspor & Impor")
        print("="*75)
        
        # Kita tarik 5 tahun terakhir: 2019 hingga 2023 (2024 mungkin belum lengkap)
        success = self.collect_historical_data(2019, 2023)
        
        if success:
            self.save_to_excel()

if __name__ == "__main__":
    BPSComtradeMacroEngine().run()