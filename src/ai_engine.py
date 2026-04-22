import ollama
import json
import logging

logger = logging.getLogger("BPS_AI_Engine")

class BPS_AI_Engine:
    def __init__(self):
        self.models = {
            "naker": "bps-naker",
            "lnprt": "bps-lnprt",
            "bmei": "bmei-auditor"
        }

    def _execute_query(self, model_key, custom_prompt):
        """Transport layer untuk mengirim prompt ke Ollama."""
        try:
            response = ollama.chat(
                model=self.models[model_key],
                messages=[{'role': 'user', 'content': custom_prompt}],
                format='json',
                options={'temperature': 0.1} # Menjaga konsistensi sesuai SOP BPS
            )
            return json.loads(response['message']['content'])
        except Exception as e:
            logger.error(f"Koneksi Ollama Gagal ({model_key}): {e}")
            return None

    def classify_naker(self, article_text: str) -> dict:
        """
        Klasifikasi NAKER dengan teknik Few-Shot Prompting V66.
        Menerima teks mentah/terpotong dan mengembalikan JSON terstruktur.
        """
        v66_prompt = f"""
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
Output: {{"status_geografi": "Valid Kota Bandung", "ringkasan_berita": "Pabrik tekstil di Cicendo mem-PHK 500 karyawan akibat order ekspor turun.", "dampak_bekerja": "2 Turun", "dampak_pengangguran": "1 Naik", "kategori_kbli": "C. Industri Pengolahan", "confidence_score": 95}}

Contoh 2:
Teks: "Bupati Bandung meresmikan job fair di Soreang dengan 50 perusahaan peserta."
Output: {{"status_geografi": "Out of Jurisdiction", "ringkasan_berita": "Event terjadi di Kabupaten Bandung (Soreang), bukan Kota Bandung.", "dampak_bekerja": "3 Tetap", "dampak_pengangguran": "3 Tetap", "kategori_kbli": "N/A", "confidence_score": 0}}

Contoh 3:
Teks: "Disnaker Kota Bandung menggelar job fair di Gedebage, sedia 3000 lowongan kerja."
Output: {{"status_geografi": "Valid Kota Bandung", "ringkasan_berita": "Disnaker Kota Bandung menyelenggarakan job fair di Gedebage dengan 3000 lowongan.", "dampak_bekerja": "1 Naik", "dampak_pengangguran": "2 Turun", "kategori_kbli": "N. Aktivitas Penyewaan dan Sewa Guna Usaha Tanpa Hak Opsi, Ketenagakerjaan, Agen Perjalanan dan Penunjang Usaha Lainnya", "confidence_score": 88}}

Contoh 4:
Teks: "Ribuan buruh di Kota Bandung demo menuntut kenaikan UMK 2026 sebesar 10 persen di depan Gedung Sate."
Output: {{"status_geografi": "Valid Kota Bandung", "ringkasan_berita": "Buruh Kota Bandung demo tuntut kenaikan UMK 10% di Gedung Sate.", "dampak_bekerja": "3 Tetap", "dampak_pengangguran": "3 Tetap", "kategori_kbli": "N/A (Isu Normatif)", "confidence_score": 90}}

Contoh 5:
Teks: "Rasionalisasi karyawan dilakukan PT XYZ Bandung, 120 pekerja tidak diperpanjang kontraknya."
Output: {{"status_geografi": "Valid Kota Bandung", "ringkasan_berita": "PT XYZ Bandung melakukan rasionalisasi dengan tidak memperpanjang 120 kontrak pekerja.", "dampak_bekerja": "2 Turun", "dampak_pengangguran": "1 Naik", "kategori_kbli": "C. Industri Pengolahan", "confidence_score": 92}}

Contoh 6:
Teks: "Lowongan CPNS 2026 Pemkot Bandung dibuka dengan formasi 50 orang. Syarat S1."
Output: {{"status_geografi": "Irrelevant", "ringkasan_berita": "Informasi teknis pendaftaran CPNS, bukan fenomena ketenagakerjaan makro.", "dampak_bekerja": "3 Tetap", "dampak_pengangguran": "3 Tetap", "kategori_kbli": "N/A", "confidence_score": 0}}

SEKARANG ANALISIS TEKS BERIKUT:
{article_text}
"""
        return self._execute_query("bps-naker", v66_prompt)

    def interrogate_lnprt(self, text):
        return self._interrogate_model(self.models["lnprt"], text)

    def audit_bmei(self, prompt: str) -> dict:
        return self._execute_query("bps-bmei", prompt)