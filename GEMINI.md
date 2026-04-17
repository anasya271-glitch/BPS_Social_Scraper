# Project: Bandung Municipality's Phenomenon Scraper 📊

## Context
Ekosistem instrumen intelijen data terpadu untuk BPS Kota Bandung. Sistem ini dirancang untuk mendeteksi, mengekstraksi, dan melakukan audit otomatis terhadap fenomena ekonomi spesifik menggunakan pendekatan Asynchronous Web Scraping dan Local SLM (Small Language Model) guna menjaga kedaulatan data.

## Tech Stack & Infrastructure
- **Language:** Python 3.11.x
- **AI Engine:** Ollama Local (Model Base: `llama3`).
- **Scraping & Automation:** Playwright (Async & Sync), `newspaper3k`, `feedparser`.
- **OCR Engine:** `easyocr` & `pytesseract` dengan akselerasi `torch`.
- **Environment:** Windows PowerShell (Virtual Environment wajib aktif).

## Agentic Roles & Modelfiles (Ollama)
Proyek ini menggunakan 3 model AI lokal spesifik dengan *System Prompt* yang berbeda. Jangan pernah tertukar saat melakukan debugging:
1. **`modelfile_bmei`**: "BPS Master Auditor" - Fokus Critical Discourse Analysis pada Ekspor, Impor, dan Rantai Pasok (digunakan di `bmei_scraper.py`).
2. **`modelfile_lnprt`**: "Auditor Investigatif LNPRT" - Fokus pada ekstraksi fenomena pengeluaran Lembaga Non-Profit (digunakan di `lnprt_scraper.py`).
3. **`modelfile_naker`**: "Auditor Ketenagakerjaan BPS" - Klasifikasi dinamika tenaga kerja (Bekerja vs Pengangguran) ke kategori KBLI (digunakan di `naker_scraper.py`).

## Core Source Scripts (`src/`)
- `bmei_scraper.py` & `bmei_debug.py`: Sentinel untuk audit data Ekspor-Impor.
- `lnprt_scraper.py`: Sentinel data LNPRT.
- `naker_scraper.py`: Sentinel dinamika ketenagakerjaan.
- `instagram_scraper.py`: Multimodal scraper untuk Instagram (dengan OCR terintegrasi).
- `BoL_scraper.py` & `cek_data_bol.py`: Modul pelacakan Bill of Lading.
- `auth_setup.py`: Wajib dieksekusi untuk menyiapkan *state* autentikasi Playwright.

## Rules & Behavioral Memory (Hukum Absolut)
- **Geofencing Ketat:** Yurisdiksi HANYA mencakup wilayah Kota Bandung (Gedebage, Cibaduyut, Antapani, dll). Jika lokasi berada di Kabupaten Bandung atau Cimahi, statusnya adalah "Out of Jurisdiction".
- **Data Sovereignty:** Proses klasifikasi teks dan audit WAJIB menggunakan AI lokal (Ollama). Jangan membocorkan data teks ke API eksternal.
- **Isolasi Output:** Seluruh hasil akhir berorientasi pada direktori `data/audit_results/` dan menggunakan format tabular/Excel (`.xlsx` / pandas dataframe).
- **Session Auth:** Jika ada script yang gagal login/scraping, ingatkan saya untuk mengecek `auth_setup.py`.

## Memory Point (Current State)
1. **Refactoring Selesai:** Pemisahan *concern* dari script monolithic menjadi modul spesifik (`bmei`, `lnprt`, `naker`) sudah diimplementasikan.
2. **Problem Solved:** Masalah `EPERM` di Windows dan *Environment Variables* untuk token telah diperbaiki.