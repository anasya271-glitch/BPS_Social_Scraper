import pandas as pd
from rapidfuzz import process, fuzz
import os

class EntityResolutionEngine:
    """
    Standard: Audit-Ready & Traceable.
    Menggabungkan Direktori Internal dengan Data Ekstraksi Publik menggunakan Jembatan Leksikal.
    """
    def __init__(self, internal_db_path, bol_data_path):
        print("[SISTEM] Memuat basis data...")
        self.internal_df = pd.read_excel(internal_db_path)
        self.bol_df = pd.read_excel(bol_data_path)
        
        # JEMBATAN LEKSIKAL (The Lexical Crosswalk)
        # Memetakan kata kunci Bahasa Indonesia di database internal ke term BoL Bahasa Inggris
        self.lexical_bridge = {
            "pakaian": ["apparel", "garment", "shirt", "clothing", "knitted"],
            "tekstil": ["textile", "fabric", "yarn", "woven", "cotton"],
            "sepatu": ["footwear", "shoes", "sneakers", "boots"],
            "kopi": ["coffee", "arabica", "robusta", "beans"],
            "plastik": ["plastic", "polymer", "resin", "packaging"],
            "makanan": ["food", "snack", "noodle", "biscuit", "beverage"],
            "kertas": ["paper", "cardboard", "pulp", "carton"],
            "obat": ["pharmaceutical", "medicine", "medical", "drug"],
            "furnitur": ["furniture", "wood", "chair", "table", "cabinet"]
        }
        
    def harmonize_data(self):
        # Asumsi: kd_kabkot 73 adalah Kota Bandung. Ubah jika kode institusi Anda berbeda.
        self.internal_df = self.internal_df.rename(columns={
            'R101': 'nama_perusahaan',
            'R102': 'alamat_perusahaan',
            'R201_K1': 'produksi utama',
            'Kab': 'kd_kabkot'
        })
        
        # 2. Filter spesifik Kota Bandung (Kode 73)
        self.bandung_entities = self.internal_df[self.internal_df['kd_kabkot'].astype(str) == "73"].copy()
        
        # Menerjemahkan 'produksi utama' ke dalam 'english_keywords'
        def apply_bridge(produksi):
            if pd.isna(produksi):
                return []
            produksi_lower = str(produksi).lower()
            keywords = []
            for id_term, en_terms in self.lexical_bridge.items():
                if id_term in produksi_lower:
                    keywords.extend(en_terms)
            return keywords

        self.bandung_entities['english_keywords'] = self.bandung_entities['produksi utama'].apply(apply_bridge)
        print(f"[RADAR] Mengamankan {len(self.bandung_entities)} entitas industri target di Kota Bandung.")

    def perform_entity_triangulation(self, threshold=70):
        """Mendeteksi eksportir langsung dengan membandingkan Nama Perusahaan dan Komoditas"""
        print("[SISTEM] Memulai proses Triangulasi Identitas (Nama & Komoditas)...")
        matched_records = []
        
        # Menggunakan Nama Perusahaan sebagai basis pencocokan, bukan alamat
        valid_entities = self.bandung_entities.dropna(subset=['nama_perusahaan'])
        internal_names = valid_entities['nama_perusahaan'].tolist()
        
        for index, row in self.bol_df.iterrows():
            bol_shipper = str(row.get('Shipper', '')).upper()
            bol_desc = str(row.get('Description', '')).lower()
            
            # Memastikan kolom Shipper tidak kosong
            if len(bol_shipper) > 3:
                # 1. Triangulasi Identitas (Fuzzy Name Matching)
                # Menggunakan token_set_ratio agar urutan kata "PT" atau "CV" tidak menurunkan skor
                best_match = process.extractOne(bol_shipper, internal_names, scorer=fuzz.token_set_ratio)
                
                if best_match and best_match[1] >= threshold:
                    matched_name = best_match[0]
                    confidence_score = best_match[1]
                    
                    company_data = valid_entities[valid_entities['nama_perusahaan'] == matched_name].iloc[0]
                    
                    # 2. Triangulasi Semantik (Komoditas)
                    is_commodity_match = False
                    for kw in company_data['english_keywords']:
                        if kw in bol_desc:
                            is_commodity_match = True
                            break
                    
                    matched_records.append({
                        'Eksportir_BoL': bol_shipper,
                        'Entitas_BPS': matched_name,
                        'Alamat_BPS': company_data['alamat_perusahaan'],
                        'Produksi_Utama_BPS': company_data['produksi utama'],
                        'Deskripsi_Barang_BoL': row.get('Description', ''),
                        'Skor_Kecocokan': round(confidence_score, 2),
                        'Validasi_Komoditas': "SEJALAN" if is_commodity_match else "ANOMALI/BERBEDA"
                    })
                    
        result_df = pd.DataFrame(matched_records)
        return result_df

# ==========================================
# KUNCI KONTAK (EXECUTION BLOCK)
# ==========================================
if __name__ == "__main__":
    # 1. Tentukan nama file Anda di sini
    FILE_DATABASE_INTERNAL = "data_internal_bps_1.xlsx" # Ganti dengan nama file Excel Anda
    FILE_HASIL_IMPORTYETI = "Draft_Audit_Ekspor_Bandung.xlsx" # File hasil scraping sebelumnya
    FILE_OUTPUT = "Hasil_Triangulasi_NVOCC.xlsx"

    # Pastikan file tersedia sebelum menjalankan
    if not os.path.exists(FILE_DATABASE_INTERNAL) or not os.path.exists(FILE_HASIL_IMPORTYETI):
        print("[!] ERROR: Pastikan kedua file Excel sumber berada di folder yang sama dengan skrip ini.")
    else:
        # 2. Inisiasi Mesin
        engine = EntityResolutionEngine(FILE_DATABASE_INTERNAL, FILE_HASIL_IMPORTYETI)
        
        # 3. Harmonisasi dan Pasang Jembatan Leksikal
        engine.harmonize_data()
        
        # 4. Jalankan Pencocokan
        hasil_df = engine.perform_entity_triangulation(threshold=80)
        
        # 5. Ekspor ke Excel
        if not hasil_df.empty:
            # Mengurutkan berdasarkan skor tertinggi
            hasil_df = hasil_df.sort_values(by='Skor_Kecocokan_Alamat', ascending=False)
            hasil_df.to_excel(FILE_OUTPUT, index=False)
            print(f"\n[SUKSES] Menemukan {len(hasil_df)} indikasi relasi bisnis.")
            print(f"Data disimpan dalam bentuk Audit-Ready di: {FILE_OUTPUT}")
            os.startfile(os.path.abspath(FILE_OUTPUT)) # Otomatis membuka Excel
        else:
            print("\n[INFO] Tidak ditemukan kecocokan di atas ambang batas (Threshold 80%).")