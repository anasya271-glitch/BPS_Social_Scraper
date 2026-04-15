import pdfplumber
import pandas as pd
import logging
import os

# Konfigurasi Logging untuk Traceability
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("audit_log.log"), logging.StreamHandler()]
)

class InvestigativeDataExtractor:
    def __init__(self, input_folder, output_file):
        self.input_folder = input_folder
        self.output_file = output_file
        self.keywords = ["ekspor", "luar negeri", "geografis", "export", "foreign"]

    def process_reports(self):
        all_data = []
        pdf_files = [f for f in os.listdir(self.input_folder) if f.endswith('.pdf')]
        
        with pd.ExcelWriter(self.output_file, engine='openpyxl') as writer:
            for file_name in pdf_files:
                logging.info(f"Memulai ekstraksi: {file_name}")
                path = os.path.join(self.input_folder, file_name)
                
                with pdfplumber.open(path) as pdf:
                    found_any = False
                    for i, page in enumerate(pdf.pages):
                        content = page.extract_text()
                        if content and any(k in content.lower() for k in self.keywords):
                            tables = page.extract_tables()
                            for j, table in enumerate(tables):
                                df = pd.DataFrame(table)
                                # Bersihkan data kosong
                                df = df.dropna(how='all').dropna(axis=1, how='all')
                                sheet_name = f"{file_name[:10]}_P{i}_T{j}"
                                df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
                                found_any = True
                    
                    if not found_any:
                        logging.warning(f"Tidak ditemukan tabel relevan di {file_name}")

        logging.info(f"Proses selesai. Data disimpan di {self.output_file}")

# Eksekusi Mode Debugging (5 Emiten)
if __name__ == "__main__":
    # Pastikan folder 'laporan_tahunan' berisi 5 PDF emiten (SSTM, ULTJ, ZATA, BELL, ALDO)
    extractor = InvestigativeDataExtractor("laporan_tahunan", "Hasil_Ekstraksi_Bandung.xlsx")
    extractor.process_reports()