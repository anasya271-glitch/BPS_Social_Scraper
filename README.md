

-----

<p align="center">
  <img src="https://readme-typing-svg.demolab.com?font=Montserrat&weight=800&size=30&duration=2500&pause=500&color=FF8C00&center=true&vCenter=true&width=850&lines=Bandung+Municipality's+Phenomenon+Scraper;+Audit+Statistik+Otomatis;Real-time+Market+Monitoring" />
</p>

<link rel="stylesheet" href="https://cloudflare.com">

<a href="https://linkedin.com" target="_blank">
    <i class="fab fa-linkedin" style="font-size:30px; color:#0077B5;"></i>
</a>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11+-blue?style=flat&logo=python&logoColor=white" alt="Python Version">
  <a href="https://ollama.com/">
    <img src="https://img.shields.io/badge/AI-Ollama%20Local-orange?style=flat&logo=ollama&logoColor=white" alt="Ollama AI">
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/License-MIT-green?style=flat" alt="License MIT">
  </a>
  <br>
  <img src="https://img.shields.io/badge/Status-Active%20Investigation-success?style=flat" alt="Build Status">
  <img src="https://img.shields.io/badge/Source-Public%20Domain-blue?style=flat" alt="Data Source">
  <br>
  <a href="https://www.linkedin.com/in/anasyanf">
    <img src="https://img.shields.io/badge/LinkedIn-Connect-blue?style=flat&logo=LinkedIn&logoColor=white" alt="LinkedIn">
  </a>
  <a href="https://www.instagram.com/chiyeonas">
    <img src="https://img.shields.io/badge/Instagram-Follow-E4405F?label=%40chiyeonas&style=flat&logo=instagram" alt="My Instagram">
  </a>
  <a href="https://www.instagram.com/bps_kota_bandung">
    <img src="https://img.shields.io/badge/Instagram-Follow-E4405F?label=%40bps_kota_bandung&style=flat&logo=instagram" alt="BPS Instagram">
  </a>
</p>


**Bandung Municipality's Phenomenon Scraper** adalah ekosistem instrumen intelijen data terpadu yang dirancang untuk Badan Pusat Statistik (BPS) Kota Bandung. Alat ini bertujuan untuk mendeteksi, mengekstraksi, dan melakukan audit otomatis terhadap fenomena ekonomi; khususnya Ekspor, Impor, Logistik, dan Lembaga Non-Profit di wilayah Kota Bandung.

Sistem ini menggabungkan kekuatan **Asynchronous Web Scraping** dengan **Local Small Language Model (SLM)** untuk menjamin kedaulatan data dan akurasi analisis tanpa biaya API pihak ketiga.

<details>
<summary><b><i><u><span style="color:blue">Daftar Isi</span></u></i></b></summary>

- [Misi Proyek](#misi-proyek)
- [Alur Kerja Sistem](#alur-kerja-sistem)
  - [Ketajaman Metodologi](#ketajaman-metodologi)
- [Development Roadmap](#development-roadmap)
- [Katalog Instrumen Terpadu](#katalog-instrumen-terpadu)
- [Prasyarat Sistem](#prasyarat-sistem)
  - [Spesifikasi Rekomendasi Perangkat Keras (Hardware)](#1-spesifikasi-rekomendasi-perangkat-keras-hardware)
  - [Spesifikasi Perangkat Lunak atau (Software)](#2-spesifikasi-perangkat-lunak-software)
- [Panduan Instalasi](#panduan-instalasi)
  - [Isolasi Lingkungan (Virtual Environment)](#1-isolasi-lingkungan-virtual-environment)
  - [Instalasi Pustaka Inti](#2-instalasi-pustaka-inti)
  - [Konfigurasi Browser & AI](#3-konfigurasi-browser--ai)
- [Cara Penggunaan (Eksekusi)](#cara-penggunaan-eksekusi)
- [Output & Laporan](#output--laporan)
- [Catatan Keamanan & Etika](#catatan-keamanan-dan-etika)
- [Lisensi](#lisensi)
</details>

-----

## Misi Proyek

Membangun infrastruktur data yang mampu menjembatani celah antara rilis berita publik dengan realita administratif lapangan, guna meminimalisir asimetri informasi dalam pelaporan statistik kewilayahan. Dengan kata lain, proyek ini dibangun untuk mengatasi asimetri informasi dengan cara:
1. Mendeteksi anomali harga komoditas impor di tingkat pasar lokal.
2. Memvalidasi klaim publik dengan data manifest logistik (*Bill of Lading*).
3. Memantau aktivitas ekonomi lembaga non-pemerintah melalui parameter hibah dan donasi.
4. Menyediakan laporan audit yang memiliki bukti digital atau *audit trail* yang lengkap.

-----

## Struktur Proyek
```text
📁
.
├── 📂 config/             # Target & Konfigurasi (Disembunyikan .gitignore)
├── 📂 src/                # Inti mesin
│   ├── 📂 archive/        # Script BETA
│   └── *.py                # Scraping & AI Logic
├── 📂 data/               # Penyimpanan data lokal (Disembunyikan .gitignore)
├── .gitignore              # List file dan folder sensitif yang disembunyikan
├── 📜 LICENSE             # Lisensi MIT dalam 2 bahasa
└── 📜 README.md           # Dokumentasi serta deskripsi proyek
```

-----

## Alur Kerja Sistem

```mermaid
graph LR
  subgraph Sources [<b>Pengumpulan Data</b> 🌐 ]
    A[Situs Publik & Media Sosial]
  end

  subgraph Engine [<b>Ekstraksi Data</b> ⚙️ ]
    B{Mesin Playwright}
    B --> B1[Pengalih Agen Pengguna]
  end

  subgraph Processing [<b>Zona Aman Lokal</b> 🛡️ ]
    C[Deduplikasi]
    C --> D[Analisis Ollama AI]
  end

  subgraph Output [<b>Hasil Akhir</b> 📊 ]
    E[Excel Terstruktur]
    F[Catatan Audit]
  end

  Sources --> B
  B1 --> C
  D --> E
  D --> F

  classDef largeTitle font-size:24px,fill:none,stroke:none;
  class Title largeTitle;
  style A fill:#bbdefb,stroke:#1565c0,stroke-width:2px,color:#000
  style B fill:#ffe0b2,stroke:#ef6c00,stroke-width:2px,color:#000
  style B1 fill:#fff9c4,stroke:#fbc02d,stroke-width:2px,color:#000
  style C fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px,color:#000
  style D fill:#e1bee7,stroke:#6a1b9a,stroke-width:2px,color:#000
  style E fill:#b2ebf2,stroke:#00838f,stroke-width:2px,color:#000
  style F fill:#cfd8dc,stroke:#455a64,stroke-width:2px,color:#000

  style Sources fill:none,stroke:#1565c0,stroke-dasharray: 5 5
  style Engine fill:none,stroke:#ef6c00,stroke-dasharray: 5 5
  style Processing fill:none,stroke:#2e7d32,stroke-dasharray: 5 5
  style Output fill:none,stroke:#00838f,stroke-dasharray: 5 5
```

### Ketajaman Metodologi

Singkatnya, sistem ini tidak hanya mengambil data, tetapi melakukan:
1. Menghapus kebisingan informasi yang tidak relevan dengan inflasi daerah.
2. Menggunakan jejak profil Edge asli untuk meminimalisir deteksi bot.
3. Membandingkan lead investigasi dari Instagram dengan berita resmi.

-----

## Development Roadmap
Proyek ini akan terus berkembang:

- [x] **Fase 1**: Mesin Scraping Inti (Instagram & News).
- [x] **Fase 2**: Integrasi AI Lokal (Ollama).
- [ ] **Fase 3**: Sistem Deteksi Anomali Otomatis.
- [ ] **Fase 4**: Visualisasi Dashboard (Seaborn/Matplotlib integration).

----


##  Katalog Instrumen Terpadu

| Kategori | Nama Alat | Deskripsi Fungsi | Tujuan Utama |
| :--- | :--- | :--- | :--- |
| ***Investigation*** | `bmei_scraper.py` | Flagship tool dengan integrasi Ollama. | Audit fenomena berita Ekspor/Impor Bandung. |
| | `lnprt_scraper.py` | Versi adaptasi untuk sektor lembaga sosial. | Mendeteksi hibah, donasi internasional, dan aktivitas LNPRT. |
| | `naker_scraper.py` | Versi adaptasi untuk sektor ketenagakerjaan. | Mendeteksi PHK, upah, penyerapan tenaga kerja. |
| | `instagram_scraper.py` | Monitoring akun asosiasi & pelaku usaha. | Menangkap sentimen pasar dan tren logistik dari tangan pertama. |
| | `g4wb_scraper.py` | Ekstraktor data go4WorldBusiness. | Mencari profil eksportir dan importir aktif di wilayah Bandung. |
| | `BoL_scraper.py` | Pengambil data *Bill of Lading*. | Validasi arus barang fisik melalui dokumen pengapalan global. |
| | `main_comtrade.py` | Koneksi API UN Comtrade. | Penyediaan data pembanding (*baseline*) statistik internasional. |
| ***Utility & Auth*** | `auth_setup.py` | Pengelola login browser. | Mengamankan sesi login agar terhindar dari tantangan login berulang. |
| | `auth_setup.py` | Membuka browser untuk otentikasi manual pertama kali dan menyimpan state (cookies). | Menginisiasi lingkungan otentikasi yang aman daripada hardcode username/password di dalam skrip. |
| | `idx.py` | Pengelola entri data dan indeks. | Manajemen alur kerja dan pemetaan target sebelum ekstraksi. |
| | `cek_data_bol.py` | Pengecek integritas manifest. | Memastikan data *Bill of Lading* valid dan bebas duplikasi. |
| | `reverse_dork.py` | Mesin optimasi pencarian. | Menghasilkan parameter Google Dork yang presisi untuk meminimalkan *noise*. |
| ***Debugging*** | `bmei_debug.py` | Lab pengujian bmei_scraper.py. | Menguji instruksi *prompt* dan fungsi mesin tanpa *scraper* penuh. |

> [!TIP]
> Jalankan script `auth_setup.py` terlebih dahulu untuk memastikan *session* yang stabil dan berkelanjutan sebelum menjalankan *scraping tasks* berdurasi panjang.

---

##  Prasyarat Sistem

### 1\. Spesifikasi Rekomendasi Perangkat Keras (Hardware)

  * **GPU:** NVIDIA GTX 1630 (4GB VRAM) atau lebih tinggi (Penting untuk akselerasi Ollama).
  * **RAM:** Minimal 8GB.
  * **Penyimpanan:** SSD (disarankan untuk performa penulisan log asinkron).

### 2\. Spesifikasi Perangkat Lunak (Software)

  * **Python 3.11:** Versi ini wajib digunakan untuk stabilitas pustaka OCR dan deteksi *binary* browser.
  * **Ollama Server:** Terpasang dan sedang berjalan dengan model `bps-auditor`.
  * **Browser:** Microsoft Edge (untuk fitur *Organic Signature* melalui *profile copying*, artinya skrip akan gagal jika Edge tidak terinstal di lokasi standar).
  * **Tesseract OCR:** Wajib terinstal di Windows dan terdaftar di System Path (untuk scraping IG)

-----

##  Panduan Instalasi

### 1\. Isolasi Lingkungan (Virtual Environment)

Buat venv menggunakan Python 3.11
```bash
py -3.11 -m venv venv
```

Aktivasi venv
```bash
.\venv\Scripts\Activate
```

### 2\. Instalasi Pustaka Inti

Perbarui pip terlebih dahulu
```bash
python -m pip install --upgrade pip
```

Instalasi seluruh dependensi investigasi
```bash
pip install pandas playwright requests feedparser trafilatura openpyxl opencv-python numpy torch torchvision pytesseract Pillow opencv-python-headless pydantic newspaper3k xlsxwriter
```

### 3\. Konfigurasi Browser & AI

Instalasi binary browser Playwright
```bash
playwright install msedge
```

Pastikan model AI lokal sudah siap
```bash
ollama create bmei-auditor -f modelfile_bmei
ollama create bps-naker -f modelfile_naker
ollama create bps-lnprt -f modelfile_lnprt
```

-----

##  Cara Penggunaan (Eksekusi)

### A. Menjalankan Audit Berita Historis (bmei_scraper.py, lnprt_scraper.py, naker_scraper.py)

Gunakan parameter rentang waktu untuk menarik data fenomena di masa lalu, contoh:

```bash
python src/nama_scraper.py --mode history --start YYYY-MM-DD --end YYYY-MM-DD
```

ganti nama_scraper.py dengan: bmei_scraper.py / lnprt_scraper.py / naker_scraper.py
ganti juga YYYY-MM-DD dengan rentang waktu yang diinginkan. contoh:

```bash
python src/bmei_scraper.py --mode history --start 2026-01-01 --end 2026-01-30
```

### B. Monitoring Akun Instagram Spesifik

```bash
python src/instagram_scraper.py --target username_akun
```

-----

##  Output & Laporan

#### 📂 `data/exports/`

  * **Isi:** Berkas `.xlsx` yang sudah matang.
  * **Peran:** Ini adalah produk akhir dari `llama_scraper.py`. Berisi rangkuman anomali, skor relevansi, dan teks berita yang sudah dibersihkan dari elemen iklan oleh fungsi Ad-Killer. Adapun database `BPS_Social_Scraper/visited_urls.txt` berguna untuk mencegah pemrosesan ganda (Deduplikasi).

#### 📂 `data/raw/`

  * **Isi:** Berkas data raw dari berformat `.xlsx`.
  * **Peran:** Ini adalah produk akhir dari `instagram_scraper.py`. Berisikan username institusi yang didata, url postingan, path tangkapan layar postingan yang didata, caption postingan, serta teks hasil ocr postingan.

#### 📂 `data/logs/`

  * **Isi:** Tangkapan layar peramban di Instagram.
  * **Peran:** Digunakan untuk *troubleshooting* kode `IS_debug.py` yang merupakan versi debugging dari script `instagram_scraper.py`.

#### 📂 `data/media/`

  * **Isi:** Gambar dari Instagram, *screenshot* postingan, atau grafik yang diunduh.
  * **Peran:** Pendukung narasi audit. Untuk `instagram_scraper.py`, folder ini menyimpan bukti visual bahwa asosiasi dagang tertentu memang mengeluhkan harga logistik (penting karena postingan IG bisa dihapus oleh pemiliknya, tapi Anda sudah punya cadangannya).

#### 📂 `data/edge_workspace/`

  * **Isi:** Salinan sementara profil browser Edge.
  * **Peran:** Keamanan. Skrip bekerja di sini agar tidak merusak *history* atau *password* asli di browser utama.

-----

##  Catatan Keamanan dan Etika

> [!IMPORTANT]
>  * Seluruh pemrosesan Small Language Model (SLM) dan ekstraksi informasi dilakukan secara lokal di perangkat. Tidak ada data mentah maupun hasil analisis investigasi yang ditransmisikan ke server pihak ketiga di luar yurisdiksi nasional, menjamin kerahasiaan penuh sesuai standar keamanan informasi BPS.
>  * Skrip ini mengadopsi teknik User-Agent emulation dan sinkronisasi profil autentik untuk memitigasi risiko deteksi otomatis. Pendekatan ini memastikan interaksi dengan infrastruktur web target tetap berada dalam koridor perilaku manusia yang wajar (*human-like behavior*).
>  * Implementasi kontrol konkurensi asinkron dibatasi secara ketat (maksimal dua instansi aktif) untuk menghormati kapasitas server target. Hal ini merupakan bentuk kepatuhan terhadap etika pengumpulan data publik guna mencegah degradasi performa pada sistem penyedia data serta menghindari pemblokiran IP oleh server.
>  * Penggunaan perangkat lunak ini ditujukan eksklusif untuk mendukung transparansi dan akuntabilitas sektor publik. Skrip ini tidak dirancang untuk menembus sistem keamanan yang diproteksi, melainkan untuk mengoptimalkan pengawasan data yang tersedia secara publik bagi kepentingan audit negara.

-----

## Lisensi

*Bandung Municipality's Phenomenon Scraper* dirilis dibawah [MIT license](LICENSE).

-----

***"In God we trust, all others must bring data. (Kita boleh percaya tuhan secara absolut, yang lain harus membawa data)" — W. Edwards Deming.***

*Proyek ini dikembangkan untuk keperluan audit data Badan Pusat Statistik (BPS) Kota Bandung.*
