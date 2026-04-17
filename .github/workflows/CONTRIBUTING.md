# Panduan Kontribusi
> **Bandung Municipality's Phenomenon Scraper**

Terima kasih atas minat Anda untuk berkontribusi pada ekosistem audit data ini. Proyek ini dikembangkan dengan filosofi **Meritokrasi Teknis** dan **Audit Investigatif Berbasis Data**. Kami menyambut kontribusi yang mengedepankan transparansi, akuntabilitas, dan efisiensi sektor publik.

Sebelum Anda mengirimkan *Pull Request* (PR) atau melaporkan *Issue*, mohon pahami tata tertib arsitektur dan etika yang berlaku di bawah ini.

---

## 1. Pakta Kedaulatan Data

Ini adalah aturan paling absolut dalam proyek ini (*Zero-Tolerance Policy*):
1. **Tidak Ada Data Mentah.** Jangan pernah mengunggah data *Bill of Lading*, data sensus internal, kredensial login, atau informasi sensitif lainnya ke dalam *Pull Request* atau *Issue*.
2. ***Local-First Inferencing.*** Setiap penambahan fitur kecerdasan buatan (AI) **wajib** menggunakan arsitektur SLM (Small Language Model) lokal seperti Ollama. Proposal yang menyertakan panggilan API ke server luar (OpenAI, Anthropic, Google) akan langsung ditolak untuk menjaga *Fidelity* dan kerahasiaan data nasional.

## 2. Standar Kualitas Kode

Kami menerapkan standar *Clean & Scalable Code* secara ketat:
* **Prinsip** ***DRY (Don't Repeat Yourself);*** jika sebuah fungsi ekstraksi web digunakan lebih dari dua kali, jadikan itu sebagai fungsi modular di direktori `src/utils/`.
* ***Professional Docstrings;*** setiap fungsi wajib memiliki dokumentasi (dalam Bahasa Inggris) yang menjelaskan *Input*, *Output*, dan *Potential Exceptions*.
* ***Precision & Hedging;*** Dalam menulis prompt untuk Llama AI, gunakan teknik pernyataan bersyarat. Jangan memaksa AI untuk menebak. Jika data tidak ada, program harus menghasilkan output `NULL` atau `UNKNOWN`, bukan asumsi.

## 3. Protokol Pelaporan Bug

Saat menemukan anomali pada scraper atau kegagalan *parsing* data:
1. Periksa apakah struktur HTML dari situs target (misal: Instagram atau Portal Berita) telah berubah.
2. Gunakan tag **[BUG]** pada judul Issue.
3. Sertakan metadata dari `audit_log.log` (pastikan nama akun atau IP telah disensor).
4. Jelaskan ***Root Cause Analysis (RCA)*** singkat mengapa bug tersebut terjadi.

## 4. Alur Kerja *Pull Request*

1. Lakukan *Fork* pada repositori ini.
2. Buat *branch* baru dengan penamaan tematis: `feature/nama-fitur` atau `hotfix/perbaikan-bug`.
3. Jalankan `flake8 src/` di terminal lokal Anda untuk memastikan kode mematuhi standar PEP8 sebelum melakukan *commit*.
4. Kirimkan PR dan tunggu **Automated Labeler** GitHub Actions mengklasifikasikan usulan Anda. Tim kami akan melakukan tinjauan teknis (*Code Review*) maksimal dalam 3x24 jam.

---
*"Integritas sistem dimulai dari integritas data yang dimasukkan ke dalamnya."*
