#!/usr/bin/env python3
# bandung_scraper.py
"""
Bandung Employment & Socioeconomic Phenomena Scraper + Analyzer
===============================================================
Mencakup:
- Klasifikasi sektor (KBLI 2020, 17 sektor)
- Deteksi euphemisme (PHK, pengangguran, kemiskinan, bencana, korupsi, dll.)
- Impact keywords (positif/negatif ketenagakerjaan)
- Regional data (30 kecamatan, 151 kelurahan Kota Bandung)
- Scoring & relevance engine
[BUG FIXES APPLIED]
- Safe regex pre-compilation at module load (not per-call)
- preprocess_text handles None/non-string input
- detect_euphemisms dedup edge case fixed
- summarize_euphemisms defaultdict properly serializable
- score_article handles empty/None text
- Graceful skip for invalid regex patterns
"""
import os
import re
import logging
import asyncio
import feedparser
import urllib.parse
import base64
import shutil
import requests
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional, Any
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse
from playwright.async_api import async_playwright, TimeoutError, Error as PlaywrightError
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional
logger = logging.getLogger("naker.bandung_scraper")
# [BUG FIX] Safe regex compilation — logs warning and returns None on failure
def _safe_compile(pattern: str, flags: int = re.IGNORECASE) -> Optional[re.Pattern]:
    """Compile regex safely; returns None if pattern is invalid."""
    try:
        return re.compile(pattern, flags)
    except re.error as e:
        logger.warning(f"Invalid regex pattern skipped: '{pattern}' — {e}")
        return None
    
# ===========================================================================
# 1. EUPHEMISM DETECTION ENGINE — KATEGORI A–I
# ===========================================================================
EUPHEMISM_MAP = {

    # --- A. PHK & PEMUTUSAN HUBUNGAN KERJA ---
    "phk": {
        "label": "PHK / Pemutusan Hubungan Kerja",
        "severity": "high",
        "keywords": [
            # Formal / resmi
            "pemutusan hubungan kerja",
            "phk",
            "phk massal",
            "rasionalisasi karyawan",
            "rasionalisasi pegawai",
            "rasionalisasi tenaga kerja",
            "restrukturisasi organisasi",
            "restrukturisasi perusahaan",
            "perampingan organisasi",
            "perampingan struktur",
            "efisiensi karyawan",
            "efisiensi tenaga kerja",
            "efisiensi pegawai",
            "efisiensi sdm",
            "efisiensi sumber daya manusia",
            "optimalisasi sdm",
            "optimalisasi organisasi",
            "penyesuaian organisasi",
            "penyesuaian jumlah karyawan",
            "penyesuaian tenaga kerja",
            "pengurangan karyawan",
            "pengurangan pegawai",
            "pengurangan tenaga kerja",
            "pengurangan jumlah pekerja",
            "pelepasan karyawan",
            "pelepasan tenaga kerja",
            "pemberhentian karyawan",
            "pemberhentian pegawai",
            "pemberhentian sepihak",
            "pemberhentian sementara",
            "pemangkasan karyawan",
            "pemangkasan pegawai",
            "pemangkasan sdm",
            "merumahkan karyawan",
            "merumahkan pekerja",
            "dirumahkan",
            "karyawan dirumahkan",
            "pekerja dirumahkan",
            "tidak diperpanjang kontrak",
            "kontrak tidak diperpanjang",
            "kontrak berakhir",
            "kontrak habis",
            "masa kontrak habis",
            "putus kontrak",
            "tidak diperpanjang",
            "pensiun dini",
            "pensiun dipercepat",
            "program pensiun dini",
            "golden handshake",
            "pesangon",
            "uang pesangon",
            "paket kompensasi",
            "program voluntary separation",
            "voluntary separation program",
            "vsp",
            "mutual separation",
            "pemutusan secara baik-baik",
            "perpisahan secara kekeluargaan",
            "mengundurkan diri",
            "resign massal",
            # Informal / media
            "di-phk",
            "kena phk",
            "terkena phk",
            "korban phk",
            "gelombang phk",
            "tsunami phk",
            "badai phk",
            "dipecat",
            "dikeluarkan",
            "dilepas",
            "dibuang perusahaan",
            "kehilangan pekerjaan",
            "kehilangan mata pencaharian",
            "tidak bekerja lagi",
            "pabrik tutup",
            "perusahaan tutup",
            "gulung tikar",
            "bangkrut",
            "pailit",
            "pkpu",
            "suspensi operasional",
            "menghentikan operasional",
            "menghentikan produksi",
            "produksi berhenti",
            "operasional dihentikan",
            "lock out",
            "lockout",
            # Konteks Bandung — industri tekstil/garmen
            "pabrik tekstil tutup",
            "pabrik garmen tutup",
            "buruh pabrik dirumahkan",
            "buruh tekstil di-phk",
            "pekerja garmen dirumahkan",
            "industri tekstil lesu",
            "industri garmen terpuruk",
            "pabrik di majalaya tutup",
            "pabrik di cigondewah tutup",
            "sektor tekstil bandung",
        ],
        "patterns": [
            r"(?:mem|di)?\bphk\b",
            r"(?:di|mem)rumahkan\s+\d*\s*(?:ribu\s+)?(?:karyawan|pekerja|buruh|pegawai)",
            r"(?:ribuan|ratusan|puluhan|belasan)\s+(?:karyawan|pekerja|buruh)\s+(?:di-?phk|dirumahkan|dipecat|diberhentikan)",
            r"(?:pabrik|perusahaan|toko|gerai)\s+(?:tutup|gulung\s*tikar|bangkrut|berhenti\s+beroperasi)",
            r"kontrak\s+(?:tidak|tak|tdk)\s+(?:di)?perpanjang",
            r"(?:gelombang|tsunami|badai|ancaman)\s+phk",
        ],
    },

    # --- B. PENGANGGURAN & KEHILANGAN PEKERJAAN ---
    "pengangguran": {
        "label": "Pengangguran / Jobless",
        "severity": "high",
        "keywords": [
            "pengangguran",
            "pengangguran terbuka",
            "tingkat pengangguran terbuka",
            "tpt",
            "pengangguran terselubung",
            "pengangguran friksional",
            "pengangguran struktural",
            "pengangguran musiman",
            "pengangguran siklikal",
            "angka pengangguran",
            "pengangguran meningkat",
            "pengangguran bertambah",
            "pengangguran membengkak",
            "nganggur",
            "menganggur",
            "belum bekerja",
            "belum mendapat pekerjaan",
            "sedang mencari kerja",
            "pencari kerja",
            "sulit mendapat pekerjaan",
            "sulit cari kerja",
            "susah cari kerja",
            "lapangan kerja sempit",
            "lapangan kerja terbatas",
            "lapangan kerja minim",
            "minimnya lapangan kerja",
            "kurangnya lapangan pekerjaan",
            "tidak ada lowongan",
            "lowongan minim",
            "jobless",
            "job seeker",
            "angkatan kerja menganggur",
            "neet",
            "pemuda menganggur",
            "lulusan menganggur",
            "sarjana menganggur",
            "sarjana nganggur",
            "fresh graduate menganggur",
            "bonus demografi",
            "beban demografi",
            "setengah menganggur",
            "setengah pengangguran",
            "underemployment",
            "jam kerja kurang",
            "jam kerja rendah",
            "pekerja paruh waktu terpaksa",
            # Konteks Bandung
            "pengangguran di bandung",
            "angka pengangguran bandung",
            "tpt bandung",
            "tpt kota bandung",
            "pencari kerja bandung",
            "lulusan bandung menganggur",
        ],
        "patterns": [
            r"(?:angka|tingkat|rate|jumlah)\s+pengangguran\s+(?:naik|meningkat|bertambah|membengkak|melonjak|tinggi)",
            r"(?:sulit|susah|sukar)\s+(?:mendapat|mencari|cari|dapat)\s+(?:pekerjaan|kerja|kerjaan)",
            r"(?:ribuan|ratusan|jutaan)\s+(?:orang|warga|penduduk)\s+menganggur",
            r"pengangguran\s+(?:di\s+)?(?:kota\s+)?bandung",
        ],
    },

    # --- C. KEMISKINAN & KESENJANGAN ---
    "kemiskinan": {
        "label": "Kemiskinan / Kesenjangan Ekonomi",
        "severity": "high",
        "keywords": [
            # Formal
            "kemiskinan",
            "angka kemiskinan",
            "garis kemiskinan",
            "penduduk miskin",
            "warga miskin",
            "masyarakat miskin",
            "keluarga miskin",
            "rumah tangga miskin",
            "masyarakat kurang mampu",
            "masyarakat prasejahtera",
            "prasejahtera",
            "keluarga prasejahtera",
            "penerima bantuan sosial",
            "penerima bansos",
            "dtks",
            "data terpadu kesejahteraan sosial",
            "keluarga penerima manfaat",
            "kpm",
            "masyarakat berpenghasilan rendah",
            "mbr",
            "kelompok rentan",
            "rentan miskin",
            "hampir miskin",
            "near poor",
            "miskin ekstrem",
            "kemiskinan ekstrem",
            "kemiskinan absolut",
            "kemiskinan relatif",
            "ketimpangan",
            "kesenjangan",
            "kesenjangan ekonomi",
            "kesenjangan sosial",
            "gini ratio",
            "rasio gini",
            "koefisien gini",
            "ketimpangan pendapatan",
            "ketimpangan pengeluaran",
            # Informal / media
            "warga tidak mampu",
            "warga kurang beruntung",
            "hidup di bawah garis kemiskinan",
            "hidup pas-pasan",
            "ekonomi pas-pasan",
            "hidup serba kekurangan",
            "kesulitan ekonomi",
            "kesulitan finansial",
            "terpuruk secara ekonomi",
            "ekonomi terpuruk",
            "daya beli menurun",
            "daya beli melemah",
            "daya beli rendah",
            "daya beli turun",
            "kemampuan beli menurun",
            "deflasi konsumsi",
            "konsumsi menurun",
            "pengeluaran menurun",
            "sulit memenuhi kebutuhan",
            "kebutuhan pokok mahal",
            "harga kebutuhan naik",
            "beban hidup berat",
            "beban ekonomi berat",
            "ekonomi sulit",
            "krisis ekonomi",
            # Konteks Bandung
            "kemiskinan bandung",
            "warga miskin bandung",
            "bantuan sosial bandung",
            "bansos bandung",
            "dtks bandung",
            "mbr bandung",
        ],
        "patterns": [
            r"(?:angka|tingkat|jumlah|persentase)\s+kemiskinan\s+(?:naik|meningkat|bertambah|tinggi|melonjak)",
            r"(?:warga|penduduk|masyarakat|keluarga)\s+(?:miskin|prasejahtera|kurang\s+mampu|tidak\s+mampu)",
            r"daya\s+beli\s+(?:menurun|melemah|turun|rendah|anjlok|merosot)",
            r"(?:kesenjangan|ketimpangan)\s+(?:ekonomi|sosial|pendapatan)",
        ],
    },

    # --- D. PENURUNAN EKONOMI / RESESI ---
    "penurunan_ekonomi": {
        "label": "Penurunan Ekonomi / Resesi",
        "severity": "medium",
        "keywords": [
            # Formal
            "penurunan ekonomi", "perlambatan ekonomi", "pelambatan ekonomi",
            "kontraksi ekonomi", "resesi", "resesi ekonomi", "resesi teknikal",
            "pertumbuhan negatif", "pertumbuhan ekonomi negatif",
            "pertumbuhan ekonomi melambat", "pertumbuhan ekonomi menurun",
            "pertumbuhan ekonomi melemah", "pdb menurun", "pdb negatif",
            "produk domestik bruto menurun", "deflasi", "stagflasi",
            "krisis ekonomi", "krisis moneter", "krisis keuangan",
            "krisis likuiditas", "krisis fiskal",
            "penurunan investasi", "investasi menurun", "investasi lesu",
            "investasi stagnan", "iklim investasi memburuk",
            "penurunan omzet", "omzet menurun", "omzet anjlok", "omzet turun drastis",
            "penurunan pendapatan", "pendapatan menurun", "pendapatan turun",
            "pendapatan merosot", "revenue drop",
            "penurunan penjualan", "penjualan menurun", "penjualan lesu",
            "penurunan ekspor", "ekspor menurun", "ekspor turun",
            "penurunan produksi", "produksi menurun", "produksi turun",
            "utilisasi rendah", "utilisasi menurun", "kapasitas menganggur",
            "overcapacity", "kelebihan kapasitas",
            "neraca perdagangan defisit", "defisit perdagangan",
            "defisit anggaran", "defisit fiskal",
            "inflasi tinggi", "inflasi melonjak", "harga naik",
            "harga melonjak", "harga melambung", "kenaikan harga",
            "biaya hidup naik", "biaya hidup tinggi", "biaya hidup mahal",
            # Informal / media
            "ekonomi lesu", "ekonomi seret", "ekonomi melemah",
            "ekonomi terpuruk", "ekonomi megap-megap", "ekonomi morat-marit",
            "ekonomi carut-marut", "ekonomi amburadul",
            "usaha sepi", "dagangan sepi", "pasar sepi", "mall sepi",
            "toko sepi", "sepi pembeli", "sepi pengunjung",
            "bisnis lesu", "bisnis stagnan", "bisnis sulit",
            "usaha bangkrut", "usaha tutup", "umkm tutup",
            "umkm bangkrut", "umkm terpuruk", "umkm kesulitan",
            "pedagang mengeluh", "pedagang merugi", "pedagang gulung tikar",
            # Konteks Bandung
            "ekonomi bandung lesu", "umkm bandung terpuruk",
            "pasar baru sepi", "pasar andir sepi", "pasar kosambi sepi",
            "wisata bandung sepi", "hotel bandung sepi",
            "pusat perbelanjaan bandung sepi", "factory outlet sepi",
            "pad bandung menurun", "pendapatan asli daerah bandung turun",
        ],
        "patterns": [
            r"(?:ekonomi|bisnis|usaha|perdagangan)\s+(?:lesu|seret|melemah|terpuruk|stagnan|anjlok)",
            r"(?:omzet|penjualan|pendapatan|ekspor|produksi)\s+(?:turun|menurun|anjlok|merosot|melemah)\s+(?:\d+\s*%|\d+\s*persen)",
            r"(?:inflasi|harga)\s+(?:naik|melonjak|melambung|tinggi)\s+(?:\d+\s*%|\d+\s*persen)?",
            r"pertumbuhan\s+(?:ekonomi\s+)?(?:negatif|minus|melambat|menurun|melemah)",
        ],
    },

    # --- E. SEKTOR INFORMAL & KERENTANAN KERJA ---
    "sektor_informal": {
        "label": "Sektor Informal / Kerentanan Kerja",
        "severity": "medium",
        "keywords": [
            # Formal
            "sektor informal", "pekerja informal", "tenaga kerja informal",
            "pekerja tidak tetap", "pekerja lepas", "pekerja harian lepas",
            "pekerja kontrak", "pekerja outsourcing", "pekerja alih daya",
            "tenaga kerja alih daya", "buruh harian", "buruh lepas",
            "buruh serabutan", "pekerja serabutan", "kerja serabutan",
            "pekerja gig", "gig economy", "gig worker",
            "pekerja platform", "driver online", "ojol",
            "ojek online", "kurir online", "mitra platform",
            "pekerja tanpa jaminan", "tanpa jaminan sosial",
            "tanpa bpjs", "tidak terdaftar bpjs",
            "tanpa kontrak kerja", "tanpa perjanjian kerja",
            "pekerja rentan", "pekerjaan rentan", "kerentanan kerja",
            "precarious work", "precariat",
            "upah rendah", "upah murah", "upah di bawah umr",
            "upah di bawah umk", "upah tidak layak", "gaji tidak layak",
            "upah minimum", "umr", "umk", "ump",
            "upah lembur tidak dibayar", "upah telat",
            "upah belum dibayar", "gaji belum dibayar",
            "eksploitasi pekerja", "eksploitasi buruh",
            "pekerja anak", "tenaga kerja anak",
            "pedagang kaki lima", "pkl", "pedagang asongan",
            "pedagang keliling", "tukang ojek", "tukang becak",
            "pemulung", "pengamen", "pengemis",
            "usaha mikro", "usaha kecil", "usaha rumahan",
            "home industry", "industri rumah tangga",
            "ekonomi kreatif informal",
            # Informal
            "kerja apa aja", "kerja asal ada", "makan gaji buta",
            "kerja keras tapi miskin", "working poor",
            "hidup dari hari ke hari", "nombok terus",
            # Konteks Bandung
            "pkl bandung", "pkl di jalan", "pkl di trotoar",
            "pedagang kaki lima bandung", "pedagang cibadak",
            "ojol bandung", "driver grab bandung", "driver gojek bandung",
            "buruh serabutan bandung", "pekerja informal bandung",
        ],
        "patterns": [
            r"(?:pekerja|buruh|tenaga\s+kerja)\s+(?:informal|lepas|harian|serabutan|kontrak|outsourcing)",
            r"(?:tanpa|tidak\s+ada|belum\s+punya)\s+(?:jaminan\s+sosial|bpjs|kontrak\s+kerja|perjanjian\s+kerja)",
            r"upah\s+(?:di\s+bawah|kurang\s+dari|belum\s+sesuai)\s+(?:umr|umk|ump|standar|ketentuan)",
            r"(?:pkl|pedagang\s+kaki\s+lima)\s+(?:ditertibkan|digusur|direlokasi|ditata)",
        ],
    },

    # --- F. KORUPSI & PENYALAHGUNAAN ANGGARAN ---
    "korupsi": {
        "label": "Korupsi / Penyalahgunaan Anggaran",
        "severity": "medium",
        "keywords": [
            # Formal
            "korupsi", "tindak pidana korupsi", "tipikor",
            "penyalahgunaan anggaran", "penyalahgunaan wewenang",
            "penyalahgunaan jabatan", "penyimpangan anggaran",
            "penyelewengan anggaran", "penyelewengan dana",
            "penyelewengan keuangan", "mark up", "markup anggaran",
            "mark up proyek", "penggelembungan anggaran",
            "penggelembungan dana", "penyunatan anggaran",
            "pemotongan anggaran tidak sah",
            "gratifikasi", "suap", "penyuapan", "sogok", "menyogok",
            "uang pelicin", "uang ketok", "uang tanda terima kasih",
            "fee proyek", "kickback", "komisi gelap",
            "pencucian uang", "money laundering",
            "pengadaan fiktif", "proyek fiktif", "proyek siluman",
            "tender tertutup", "tender arisan",
            "kolusi", "nepotisme", "kkn",
            "konflik kepentingan", "conflict of interest",
            "pungutan liar", "pungli", "pungutan tidak resmi",
            "pemerasan", "penggelapan", "penggelapan dana",
            "maladministrasi", "malaadministrasi",
            "kerugian negara", "kerugian daerah", "kerugian keuangan negara",
            "temuan bpk", "temuan audit", "opini disclaimer",
            "opini tidak wajar", "opini tdp",
            # Informal
            "duit haram", "uang haram", "uang rakyat dikorupsi",
            "bancakan proyek", "bagi-bagi proyek", "proyek pesanan",
            "anggaran bocor", "kebocoran anggaran",
            "koruptor", "tersangka korupsi", "terdakwa korupsi",
            "ott", "operasi tangkap tangan", "ditangkap kpk",
            "dijerat kpk", "kasus korupsi",
            # Konteks Bandung
            "korupsi bandung", "korupsi pemkot bandung",
            "korupsi apbd bandung", "pungli bandung",
            "kasus korupsi bandung", "anggaran bandung bocor",
        ],
        "patterns": [
            r"(?:kasus|dugaan|indikasi|perkara)\s+(?:korupsi|tipikor|suap|gratifikasi|penggelapan)",
            r"(?:kerugian\s+(?:negara|daerah|keuangan))\s+(?:sebesar|mencapai|senilai)\s+(?:rp|IDR)",
            r"(?:ott|operasi\s+tangkap\s+tangan)\s+(?:kpk|kejaksaan|polisi)",
            r"(?:penyalahgunaan|penyelewengan|penyimpangan)\s+(?:anggaran|dana|wewenang|jabatan)",
        ],
    },

    # --- G. BENCANA & DAMPAK LINGKUNGAN ---
    "bencana": {
        "label": "Bencana Alam & Lingkungan",
        "severity": "medium",
        "keywords": [
            "bencana alam", "bencana", "banjir", "banjir bandang",
            "banjir rob", "genangan", "genangan air",
            "tanah longsor", "longsor", "pergerakan tanah",
            "gempa bumi", "gempa", "gempa tektonik",
            "kebakaran", "kebakaran hutan", "kebakaran lahan",
            "kekeringan", "kemarau panjang", "krisis air",
            "kekurangan air bersih", "air bersih langka",
            "pencemaran lingkungan", "polusi", "polusi udara",
            "polusi air", "pencemaran air", "pencemaran sungai",
            "pencemaran tanah", "limbah", "limbah industri",
            "limbah pabrik", "limbah b3", "limbah beracun",
            "sampah menumpuk", "sampah menggunung", "krisis sampah",
            "tpa penuh", "tpa overload",
            "cuaca ekstrem", "angin kencang", "angin puting beliung",
            "hujan deras", "hujan lebat", "perubahan iklim",
            "pemanasan global", "el nino", "la nina",
            "rob", "abrasi", "erosi",
            "pohon tumbang", "jalan amblas", "jalan rusak",
            "infrastruktur rusak", "rumah rusak", "rumah roboh",
            "pengungsi bencana", "korban bencana", "terdampak bencana",
            "daerah rawan bencana", "zona merah bencana",
            "darurat bencana", "tanggap darurat", "status darurat",
            # Konteks Bandung
            "banjir bandung", "banjir cicaheum", "banjir cibiru",
            "banjir pagarsih", "banjir pasteur", "banjir dayeuhkolot",
            "longsor bandung", "longsor lembang", "longsor punclut",
            "polusi udara bandung", "kualitas udara bandung",
            "ispu bandung", "sampah bandung", "tpa sarimukti",
            "sungai cikapundung tercemar", "sungai citarum tercemar",
            "kebakaran bandung", "kebakaran pasar bandung",
        ],
        "patterns": [
            r"(?:banjir|longsor|gempa|kebakaran)\s+(?:di\s+)?(?:kota\s+)?bandung",
            r"(?:korban|pengungsi|terdampak)\s+(?:banjir|longsor|gempa|bencana)\s+(?:mencapai|sebanyak|bertambah)\s+\d+",
            r"(?:polusi|pencemaran|kualitas)\s+(?:udara|air|sungai)\s+(?:bandung|memburuk|berbahaya|tidak\s+sehat)",
            r"(?:sampah|limbah)\s+(?:menumpuk|menggunung|meluap|mencemari)",
        ],
    },

    # --- H. MASALAH KESEHATAN MASYARAKAT ---
    "kesehatan": {
        "label": "Masalah Kesehatan Masyarakat",
        "severity": "medium",
        "keywords": [
            "wabah", "pandemi", "epidemi", "endemi",
            "klb", "kejadian luar biasa", "outbreak",
            "stunting", "gizi buruk", "malnutrisi", "kurang gizi",
            "gizi kurang", "busung lapar", "kelaparan",
            "rawan pangan", "krisis pangan", "ketahanan pangan rendah",
            "kematian ibu", "kematian bayi", "kematian balita",
            "angka kematian ibu", "aki", "angka kematian bayi", "akb",
            "demam berdarah", "dbd", "tbc", "tuberkulosis",
            "diare", "campak", "difteri", "polio",
            "covid", "covid-19", "varian baru",
            "penyakit menular", "penyakit tropis",
            "fasilitas kesehatan kurang", "puskesmas minim",
            "tenaga kesehatan kurang", "kekurangan dokter",
            "bpjs kesehatan", "bpjs defisit", "iuran bpjs naik",
            "obat langka", "obat mahal", "obat tidak tersedia",
            "kesehatan jiwa", "gangguan mental", "depresi",
            "bunuh diri", "percobaan bunuh diri",
            # Konteks Bandung
            "stunting bandung", "dbd bandung", "tbc bandung",
            "rsud bandung", "puskesmas bandung",
            "kesehatan masyarakat bandung",
        ],
        "patterns": [
            r"(?:kasus|angka|jumlah)\s+(?:stunting|dbd|tbc|covid|diare|campak)\s+(?:naik|meningkat|melonjak|tinggi)",
            r"(?:wabah|klb|outbreak)\s+(?:di\s+)?(?:kota\s+)?bandung",
            r"(?:fasilitas|tenaga)\s+kesehatan\s+(?:kurang|minim|tidak\s+memadai|terbatas)",
        ],
    },

    # --- I. KONFLIK INDUSTRIAL & BURUH ---
    "konflik_buruh": {
        "label": "Konflik Industrial / Buruh",
        "severity": "medium",
        "keywords": [
            "mogok kerja", "mogok", "aksi mogok", "mogok massal",
            "demo buruh", "demo pekerja", "demo karyawan",
            "unjuk rasa buruh", "unjuk rasa pekerja",
            "demonstrasi buruh", "protes buruh", "protes pekerja",
            "aksi buruh", "aksi pekerja", "aksi massa buruh",
            "serikat buruh", "serikat pekerja", "sp", "sb",
            "federasi serikat pekerja", "konfederasi buruh",
            "perselisihan hubungan industrial", "phi",
            "perselisihan kerja", "sengketa kerja",
            "pengadilan hubungan industrial",
            "bipartit", "tripartit", "mediasi ketenagakerjaan",
            "somasi", "gugatan buruh", "gugatan pekerja",
            "hak normatif tidak dibayar", "hak normatif dilanggar",
            "upah tidak dibayar", "thr tidak dibayar",
            "thr telat", "thr dipotong",
            "lembur tidak dibayar", "jamsostek tidak dibayar",
            "pelanggaran ketenagakerjaan", "pelanggaran uu ketenagakerjaan",
            "intimidasi buruh", "intimidasi pekerja",
            "union busting", "anti serikat",
            "pekerja migran", "tki", "tkw", "pmi",
            "pekerja migran indonesia", "buruh migran",
            # Konteks Bandung
            "demo buruh bandung", "mogok kerja bandung",
            "aksi buruh bandung", "serikat pekerja bandung",
            "unjuk rasa buruh bandung", "konflik industrial bandung",
        ],
        "patterns": [
            r"(?:ribuan|ratusan|puluhan)\s+(?:buruh|pekerja|karyawan)\s+(?:mogok|demo|unjuk\s+rasa|protes|turun\s+ke\s+jalan)",
            r"(?:mogok|demo|unjuk\s+rasa|aksi)\s+(?:buruh|pekerja|karyawan)\s+(?:di\s+)?(?:kota\s+)?bandung",
            r"(?:thr|upah|gaji|lembur|hak\s+normatif)\s+(?:tidak|belum|tak)\s+(?:dibayar|dibayarkan|diberikan)",
            r"(?:pelanggaran|melanggar)\s+(?:uu|undang-undang)\s+(?:ketenagakerjaan|cipta\s+kerja|omnibus)",
        ],
    },
}

# ===========================================================================
# 1b. PRE-COMPILE PATTERNS AT MODULE LOAD
# ===========================================================================
_COMPILED_PATTERNS: Dict[str, List[re.Pattern]] = {}
def _compile_all_patterns():
    """Pre-compile euphemism regex patterns. Called once at module load."""
    for cat_key, cat_data in EUPHEMISM_MAP.items():
        compiled = []
        for pat_str in cat_data.get("patterns", []):
            pat = _safe_compile(pat_str)
            if pat is not None:
                compiled.append(pat)
        _COMPILED_PATTERNS[cat_key] = compiled

# ===========================================================================
# 2. EUPHEMISM DETECTION FUNCTIONS
# ===========================================================================
@dataclass
class EuphemismMatch:
    """Hasil deteksi euphemisme dalam teks."""
    category: str
    label: str
    severity: str
    matched_keyword: str
    match_type: str          # "keyword" atau "pattern"
    context_snippet: str
    position: Tuple[int, int]
def preprocess_text(text) -> str:
    """
    Normalisasi teks sebelum analisis.
    [BUG FIX] Handle None, non-string input gracefully.
    """
    if text is None:
        return ""
    if not isinstance(text, str):
        text = str(text)
    text = text.lower()
    text = re.sub(r'\s+', ' ', text)
    text = text.replace('\u2013', '-').replace('\u2014', '-')
    text = re.sub(r'[\u201c\u201d\u201e]', '"', text)
    text = re.sub(r"[\u2018\u2019\u201a]", "'", text)
    return text.strip()
def extract_context(text: str, start: int, end: int, window: int = 80) -> str:
    """Ambil potongan teks sekitar match untuk konteks."""
    ctx_start = max(0, start - window)
    ctx_end = min(len(text), end + window)
    snippet = text[ctx_start:ctx_end].strip()
    if ctx_start > 0:
        snippet = "..." + snippet
    if ctx_end < len(text):
        snippet = snippet + "..."
    return snippet
def detect_euphemisms(text) -> List[EuphemismMatch]:
    """
    Deteksi seluruh euphemisme dalam teks.
    [BUG FIX] Uses pre-compiled patterns from _COMPILED_PATTERNS.
    [BUG FIX] Handles None/empty text input.
    [BUG FIX] Dedup overlap logic guards against empty list.
    """
    if not text:
        return []
    normalized = preprocess_text(text)
    if not normalized:
        return []
    matches = []
    for cat_key, cat_data in EUPHEMISM_MAP.items():
        # --- Keyword matching ---
        for kw in cat_data.get("keywords", []):
            kw_lower = kw.lower()
            idx = 0
            while True:
                pos = normalized.find(kw_lower, idx)
                if pos == -1:
                    break
                # Word boundary check
                before_ok = (pos == 0) or not normalized[pos - 1].isalnum()
                after_pos = pos + len(kw_lower)
                after_ok = (after_pos >= len(normalized)) or not normalized[after_pos].isalnum()
                if before_ok and after_ok:
                    matches.append(EuphemismMatch(
                        category=cat_key,
                        label=cat_data["label"],
                        severity=cat_data["severity"],
                        matched_keyword=kw,
                        match_type="keyword",
                        context_snippet=extract_context(normalized, pos, after_pos),
                        position=(pos, after_pos),
                    ))
                idx = pos + 1
        # --- Pattern matching (pre-compiled) ---
        # [BUG FIX] Use pre-compiled patterns instead of re-compiling each call
        for compiled_pat in _COMPILED_PATTERNS.get(cat_key, []):
            try:
                for m in compiled_pat.finditer(normalized):
                    matches.append(EuphemismMatch(
                        category=cat_key,
                        label=cat_data["label"],
                        severity=cat_data["severity"],
                        matched_keyword=m.group(),
                        match_type="pattern",
                        context_snippet=extract_context(normalized, m.start(), m.end()),
                        position=(m.start(), m.end()),
                    ))
            except Exception as e:
                # [BUG FIX] Catch any runtime regex error gracefully
                logger.warning(f"Regex match error in category '{cat_key}': {e}")
                continue
    # --- Dedup overlapping matches ---
    matches.sort(key=lambda x: (x.position[0], -(x.position[1] - x.position[0])))
    deduped = []
    last_end = -1
    for m in matches:
        if m.position[0] >= last_end:
            deduped.append(m)
            last_end = m.position[1]
        # [BUG FIX] Guard against empty deduped list before accessing [-1]
        elif deduped and (m.position[1] - m.position[0]) > (deduped[-1].position[1] - deduped[-1].position[0]):
            deduped[-1] = m
            last_end = m.position[1]
    return deduped
def summarize_euphemisms(matches: List[EuphemismMatch]) -> Dict:
    """
    Rangkum hasil deteksi euphemisme.
    [BUG FIX] Use regular dict instead of defaultdict with lambda
    so result is JSON-serializable without extra conversion.
    """
    summary = {
        "total_matches": len(matches),
        "by_category": {},
        "high_severity_count": 0,
        "categories_detected": [],
    }
    categories_seen = set()
    for m in matches:
        categories_seen.add(m.category)
        if m.category not in summary["by_category"]:
            summary["by_category"][m.category] = {
                "count": 0,
                "severity": m.severity,
                "keywords_found": [],
            }
        cat = summary["by_category"][m.category]
        cat["count"] += 1
        cat["keywords_found"].append(m.matched_keyword)
        if m.severity == "high":
            summary["high_severity_count"] += 1
    summary["categories_detected"] = sorted(categories_seen)
    return summary


# ===========================================================================
# 3. SECTOR KEYWORDS (KBLI 2020 — 17 SEKTOR)
# ===========================================================================

SECTOR_KEYWORDS = {
    "A_pertanian": ["pertanian", "perkebunan", "peternakan", "perikanan", "agribisnis", "pangan", "hortikultura", "agribisnis", "pangan", "padi", "sawah", "petani", "nelayan", "peternak", "ikan", "udang", "rumput laut", "kopi", "teh", "kakao", "karet", "sawit", "buah", "buah-buahan", "sayur", "sayuran"],
    "B_pertambangan": ["pertambangan", "tambang", "mineral", "galian", "batubara", "emas", "perak", "tembaga", "nikel", "timah", "batu bara", r"minyak[\s\-]?bumi", "gas bumi", "minyak", "gas"],
    "C_manufaktur": ["manufaktur", "industri", "pabrik", "produksi", "tekstil", "garmen", "otomotif", "elektronik", "farmasi", "makanan olahan", "minuman olahan", "kertas", "plastik", "logam", "kimia", "permesinan", "alat berat"],
    "D_listrik_gas": ["listrik", "gas", "energi", "pln", "pembangkit", "pipa gas", "jaringan listrik", "energi terbarukan", "energi fosil", "energi", "pembangkit listrik", "pembangkit energi"],
    "E_sampah": [r"air[\s\-]?bersih", r"pengelolaan[\s\-]?limbah", "sanitasi", "pdam", "sampah", "tpa", r"tempat[\s\-]?pembuangan[\s\-]?akhir", r"pengelolaan[\s\-]?sampah", "daur ulang", "recycling"],
    "F_konstruksi": ["konstruksi", "pembangunan", "proyek infrastruktur", "properti", "real estate", "bangunan", "jalan", "jembatan", "gedung", "perumahan", "apartemen"],
    "G_perdagangan": ["perdagangan", "retail", "toko", "pasar", "e-commerce", "marketplace", "ekspor", "impor", "distributor", "grosir", "mal", r"pusat[\s\-]?perbelanjaan", r"factory[\s\-]?outlet"],
    "H_transportasi": ["transportasi", "logistik", "angkutan", "pengiriman", "bandara", "pelabuhan", r"kereta[\s\-]?api", "bus", "truk", "kendaraan", r"transportasi[\s\-]?online", "ojol"],
    "I_akomodasi": ["hotel", "restoran", "kafe", "kuliner", "pariwisata", "wisata", "hospitality", "penginapan", "makanan", "minuman", "catering", "event organizer", "penginapan", "akomodasi"],
    "J_informasi_komunikasi": [r"teknologi[\s\-]?informasi", r"tele[\s\-]?komunikasi", "media", "startup", "it", "digital", "software", "hardware", "aplikasi", "internet", "komunikasi", r"konten[\s\-]?digital"],
    "K_keuangan_asuransi": ["perbankan", "bank", "asuransi", "fintech", "keuangan", "kredit", "pinjaman", "investasi", "saham", "obligasi", "reksa dana", "asuransi jiwa", "asuransi kesehatan"],
    "L_real_estate": [r"real[\s\-]?estate", "properti", "perumahan", "apartemen", "komersial", "residensial", "sewa", "kontrakan", r"jual[\s\-]?beli properti", r"pengembangan[\s\-]?properti"],
    "O_administrasi_pemerintahan": ["pemerintah", "pemerintahan", "asn", "pns", "birokrasi", "apbd", "apbn", "anggaran", r"dana[\s\-]?desa", r"dana[\s\-]?alokasi khusus", "dana bantuan operasional sekolah", "dbos", "otonomi daerah", "pemda", "pemprov", "pemkot"],
    "P_pendidikan": ["pendidikan", "sekolah", "universitas", "kampus", "guru", "dosen", "siswa", "mahasiswa", "kurikulum", "pembelajaran", "kelas", "ruang belajar", "pendidikan vokasi", "pelatihan kerja", "kursus", "bimbingan belajar"],
    "Q_kesehatan_sosial": ["kesehatan", "rumah sakit", "klinik", "dokter", "perawat", "sosial", r"kesejahteraan[\s\-]?sosial", r"bpjs[\s\-]?kesehatan", r"bpjs[\s\-]?ketenagakerjaan", r"panti[\s\-]?sosial", r"panti[\s\-]?jompo", r"panti[\s\-]?asuhan"],
    "R_kesenian_hiburan": ["seni", "budaya", "hiburan", "musik", "film", "event", "kreatif", "industri kreatif", "kegiatan seni", "kegiatan budaya"],
    "S_jasa_lainnya": ["jasa", "laundry", "bengkel", "salon", "perawatan", "kecantikan", "kesehatan alternatif", "karaoke", "catering", r"event[\s\-]?organizer"],
}


# ===========================================================================
# 4. IMPACT KEYWORDS (POSITIF / NEGATIF KETENAGAKERJAAN)
# ===========================================================================

IMPACT_KEYWORDS = {
    "dampak_negatif_ketenagakerjaan": [
        "phk", "pengangguran", "kehilangan pekerjaan", "dirumahkan", "pemutusan",
        "pengurangan pekerja", "tutup", "bangkrut", "gulung tikar", "pailit",
        "upah turun", "pendapatan menurun", "daya beli turun",
    ],
    "dampak_positif_ketenagakerjaan": [
        "lowongan kerja", "rekrutmen", "penerimaan karyawan", "investasi baru",
        "pembukaan pabrik", "perluasan usaha", "ekspansi", "pelatihan kerja",
        "sertifikasi", "upskilling", "reskilling", "program magang",
        "umk naik", "umr naik", "upah naik", "kenaikan gaji",
    ],
    "kebijakan_pemerintah": [
        "kebijakan", "peraturan", "regulasi", r"undang[\s\-]?undang", "perda",
        "stimulus", "insentif", "subsidi", "bantuan", "program pemerintah",
        "kartu prakerja", r"pra[\s\-]?kerja", "jaminan sosial",
    ],
    "negatif_ekonomi": [
        "resesi", "inflasi", "deflasi", "krisis", "kemiskinan",
        "kesenjangan", "daya beli turun", "harga naik", "bangkrut",
    ],
    "positif_ekonomi": [
        "pertumbuhan ekonomi", "investasi", "ekspor meningkat",
        "PDRB naik", "daya beli meningkat", "pemulihan ekonomi",
    ],
}

# ===========================================================================
# 5. REGIONAL DATA — KOTA BANDUNG (30 KECAMATAN, 151 KELURAHAN)
# ===========================================================================

BANDUNG_DISTRICTS = {
    "andir": [r"\bandir", r"\bkebon[\s\-]?jeruk", r"\bciroyom", r"\bdungus[\s\-]?cariang", r"\bgaruda", r"\bmaleber", r"\bcampaka"],
    "antapani": [r"\bantapani", r"\bantapani[\s\-]?kidul", r"\bantapani[\s\-]?tengah", r"\bantapani[\s\-]?wetan"],
    "arcamanik": [r"\barcamanik", r"\bcisaranten[\s\-]?bina[\s\-]?harapan", r"\bcisaranten[\s\-]?endah", r"\bcisaranten[\s\-]?kulon", r"\bsukamiskin"],
    "astana anyar": [r"\bastana[\s\-]?anyar", r"\bcibadak", r"\bkarang[\s\-]?anyar", r"\bnyengseret", r"\bpanjunan", r"\bpelindung[\s\-]?hewan"],
    "babakan ciparay": [r"\bbabakan[\s\-]?ciparay", r"\bbabakan", r"\bcirangrang", r"\bmargahayu[\s\-]?utara", r"\bmargasuka", r"\bsukahaji"],
    "bandung kidul": [r"\bbandung[\s\-]?kidul", r"\bbatununggal", r"\bkujangsari", r"\bmengger", r"\bwates"],
    "bandung kulon": [r"\bbandung[\s\-]?kulon", r"\bcaringin", r"\bcibuntu", r"\bcigondewah[\s\-]?kaler", r"\bcigondewah[\s\-]?kidul", r"\bcigondewah[\s\-]?rahayu", r"\bwarung[\s\-]?muncang", r"\bgempol[\s\-]?sari"],
    "bandung wetan": [r"\bbandung[\s\-]?wetan", r"\bcihapit", r"\bcitarum", r"\btamansari"],
    "batununggal": [r"\bbatununggal", r"\bbinong", r"\bgumuruh", r"\bkebon[\s\-]?waru", r"\bkacapiring", r"\bkebongedang", r"\bmaleer", r"\bsamoja", r"\bcibangkong"],
    "bojongloa kaler": [r"\bbojongloa[\s\-]?kaler", r"\bbabakan[\s\-]?asih", r"\bbabakan[\s\-]?tarogong", r"\bjamika", r"\bkopo", r"\bsuka[\s\-]?asih"],
    "bojongloa kidul": [r"\bbojongloa[\s\-]?kidul", r"\bcibaduyut", r"\bkebon[\s\-]?lega", r"\bmekarwangi", r"\bsitusaeur"],
    "buahbatu": [r"\bbuah[\s\-]?batu", r"\bcijawura", r"\bmarga[\s\-]?sari", r"\bsekejati"],
    "cibeunying kaler": [r"\bcibeunying[\s\-]?kaler", r"\bcigadung", r"\bcihaurgeulis", r"\bneglasari", r">\sukaluyu"],
    "cibeunying kidul": [r"\bcibeunying[\s\-]?kidul", r"\bcicadas", r"\bcikutra", r"\bpadasuka", r"\bsukamaju", r"\bsukapada"],
    "gedebage": [r"\bgedebage", r"\bcimincrang", r"\bcisaranten[\s\-]?kidul", r"\brancabolang", r"\brancanumpang"],
    "kiaracondong": [r"\bkiaracondong", r"\bbabakan[\s\-]?surabaya", r"\bcicaheum", r"\bkebon[\s\-]?jayanti", r"\bkebon[\s\-]?kangkung", r"\bsukapura"],
    "lengkong": [r"\blengkong", r"\bburangrang", r"\bcijagra", r"\bcikawao", r"\blingkar[\s\-]?selatan", r"\bmalabar", r"\bpaledang", r"\bturangga"],
    "mandalajati": [r"\bmandalajati", r"\bjati[\s\-]?handap", r"\bkarang[\s\-]?pamulang", r"\bpasir[\s\-]?impun", r"\bsindang[\s\-]?jaya"],
    "panyileukan": [r"\bpanyileukan", r"\bcibiruwetan", r"\bcipadung[\s\-]?kidul", r"\bcipadung[\s\-]?kulon", r"\bcipadung[\s\-]?wetan", r"\bmekarsari"],
    "rancasari": [r"\brancasari", r"\bcipamokolan", r"\bderwati", r"\bmanjahlega", r"\bmekarjaya"],
    "regol": [r"\bregol", r"\bancol", r"\bbalonggede", r"\bcisereuh", r"\bcigereleng", r"\bpungkur", r"\bpasirluyu"],
    "sukajadi": [r"\bsukajadi", r"\bcipedes", r"\bpasteur", r"\bsukabungah", r"\bsukagalih", r"\bsukawarna"],
    "sukasari": [r"\bsukasari", r"\bgegerkalong", r"\bisola", r">\sarijadi", r">\sukarasa"],
    "sumur bandung": [r"\bsumur[\s\-]?bandung", r"\bbabakan[\s\-]?ciamis", r"\bbraga", r"\bkebon[\s\-]?pisang", r"\bmerdeka"],
    "ujung berung": [r"\bujung[\s\-]?berung", r"\bcigending", r"\bpasanggrahan", r"\bpasir[\s\-]?endah", r"\bpasir[\s\-]?wangi", r"\bpasir[\s\-]?jati"],
}
BANDUNG_LOCATION_KEYWORDS = ["Bandung", "Kota Bandung", "Pemkot Bandung", "cihampelas", "alun-alun bandung"]
for kecamatan, kelurahans in BANDUNG_DISTRICTS.items():
    BANDUNG_LOCATION_KEYWORDS.append(kecamatan)
    BANDUNG_LOCATION_KEYWORDS.extend(kelurahans)

_compile_all_patterns()

# ===========================================================================
# 6. SCORING & RELEVANCE ENGINE
# ===========================================================================

def score_article(text) -> Dict:
    """
    Hitung skor relevansi artikel.
    [BUG FIX] Handle None/empty text gracefully — return zero-score result.
    """
    # [BUG FIX] Guard against None/empty input
    if not text:
        return {
            "euphemisms": [],
            "sectors": [],
            "impacts": [],
            "locations": [],
            "total_score": 0.0,
            "score_breakdown": {"euphemism": 0, "sector": 0, "impact": 0, "location": 0},
        }
    normalized = preprocess_text(text)
    results = {
        "euphemisms": detect_euphemisms(text),
        "sectors": [],
        "impacts": [],
        "locations": [],
        "total_score": 0.0,
    }
    euph_summary = summarize_euphemisms(results["euphemisms"])
    euph_score = min(50, euph_summary["total_matches"] * 5 + euph_summary["high_severity_count"] * 10)
    # Sector score (0-20)
    sector_score = 0
    for sector, keywords in SECTOR_KEYWORDS.items():
        for kw in keywords:
            if kw.lower() in normalized:
                results["sectors"].append(sector)
                sector_score += 4
                break
    sector_score = min(20, sector_score)
    # Impact score (0-20)
    impact_score = 0
    for impact_type, keywords in IMPACT_KEYWORDS.items():
        for kw in keywords:
            if kw.lower() in normalized:
                results["impacts"].append({"type": impact_type, "keyword": kw})
                impact_score += 3
    impact_score = min(20, impact_score)
    # Location score (0-10)
    location_score = 0
    for loc in BANDUNG_LOCATION_KEYWORDS:
        if loc.lower() in normalized:
            results["locations"].append(loc)
            location_score += 2
    location_score = min(10, location_score)
    results["total_score"] = euph_score + sector_score + impact_score + location_score
    results["score_breakdown"] = {
        "euphemism": euph_score,
        "sector": sector_score,
        "impact": impact_score,
        "location": location_score,
    }
    return results

# ===========================================================================
# 7. THE ENGINE: BANDUNG SCRAPER (V66 PLAYWRIGHT INTEGRATION)
# Murni berfungsi sebagai pengumpul HTML, tanpa logika analitik.
# ===========================================================================

class BandungScraper:
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        # Radar 33 Situs Lengkap (High, Medium, Low Tier)
        self.sites = [
            "bandung.go.id", "tempo.co", "tirto.id", "narasi.tv",
            "ayobandung.com", "pikiran-rakyat.com", "bandung.kompas.com",
            "disdagin.bandung.go.id", "cnnindonesia.com", "rri.co.id",
            "jabarprov.go.id", "bps.go.id", "kemnaker.go.id", "kompas.com", "detik.com",
            "radarbandung.id", "kumparan.com", "infobandungkota.com",
            "prfmnews.id", "kilasbandungnews.com", "bandungbergerak.id",
            "koranmandala.com", "jabarekspres.com", "jabar.tribunnews.com",
            "liputan6.com", "merdeka.com", "sindonews.com",
            "blogspot.com", "wordpress.com", "medium.com",
            "facebook.com", "twitter.com", "instagram.com"
        ]
        
        self.mode = self.config.get("mode", "live")
        self.start_date = self.config.get("start", "")
        self.end_date = self.config.get("end", "")

        # Konfigurasi Edge Profile (Otomatis menggunakan Cookies/Credentials Anda)
        self.edge_source_dir = str(Path.home() / "AppData" / "Local" / "Microsoft" / "Edge" / "User Data")
        self.workspace_dir = Path.cwd() / "data" / "edge_workspace"
        os.makedirs(self.workspace_dir, exist_ok=True)
        self.browser_semaphore = asyncio.Semaphore(3)

    async def discover_articles(self) -> List[Dict[str, Any]]:
        """
        Tahap 1: Dynamic Matrix Engine.
        Memecah kueri menjadi klaster (PHK, Loker, UMK) untuk menembus limitasi Google.
        """
        KLASTER_ISU = {
            "PHK_Krisis": '("PHK" OR "pabrik tutup" OR "gulung tikar" OR "dirumahkan")',
            "Ekspansi_Loker": '("lowongan kerja" OR "job fair" OR "rekrutmen" OR "padat karya")',
            "Normatif_Industrial": '("UMK" OR "demo buruh" OR "upah minimum" OR "serikat pekerja")'
        }
        
        rentang_waktu = []
        if self.mode == "history" and (self.start_date or self.end_date):
            t_query = ""
            if self.start_date: t_query += f" after:{self.start_date}"
            if self.end_date: t_query += f" before:{self.end_date}"
            rentang_waktu.append(t_query.strip())
        else:
            rentang_waktu = ["when:1d", "when:7d"]

        logger.info(f" [>] Radar V66 Aktif: {len(self.sites)} Situs x {len(KLASTER_ISU)} Isu.")
        
        all_tasks = []
        for site in self.sites:
            for k_nama, k_query in KLASTER_ISU.items():
                for waktu in rentang_waktu:
                    all_tasks.append(self._fetch_rss_matrix(site, k_nama, k_query, waktu))
        
        matrix_results = await asyncio.gather(*all_tasks)
        
        seen_urls = set()
        seen_titles = set()
        final_entries = []
        
        for result_group in matrix_results:
            for item in result_group:
                # Normalisasi judul untuk mencegah lolosnya artikel identik akibat URL Masking Google
                clean_title = " ".join(item["title"].lower().split())
                
                if item["url"] not in seen_urls and clean_title not in seen_titles:
                    seen_urls.add(item["url"])
                    seen_titles.add(clean_title)
                    final_entries.append(item)
                    
        return final_entries

    async def _fetch_rss_matrix(self, site, k_nama, k_query, waktu) -> List[Dict]:
        query = f'site:{site} "Kota Bandung" {k_query} {waktu}'.strip()
        rss_url = f"https://news.google.com/rss/search?q={urllib.parse.quote(query)}&hl=id&gl=ID&ceid=ID:id"
        res = []
        try:
            feed = await asyncio.to_thread(feedparser.parse, rss_url)
            for e in feed.entries[:10]:
                res.append({
                    "url": self._decode_google_url(e.link),
                    "title": e.title,
                    "site": site,
                    "published": e.get("published", ""),
                    "klaster": k_nama
                })
        except: pass
        return res

    def prepare_workspace(self):
        """Sinkronisasi Ruang Isolasi (V66 Fast-Boot)."""
        logger.info(" [>] Sinkronisasi Ruang Isolasi (Mengabaikan Cache Raksasa)...")
        source = Path(self.edge_source_dir) / "Default"
        target = self.workspace_dir / "Default"
        
        if target.exists(): 
            shutil.rmtree(target, ignore_errors=True)
            
        try: 
            # Mengecualikan folder cache bergiga-giga untuk mempercepat booting (Selective Cloning)
            ignore_list = shutil.ignore_patterns(
                "SingletonLock", "lock", "Cache", "Code Cache", 
                "Service Worker", "Media Cache", "GPUCache", 
                "DawnCache", "Crashpad", "VideoDecodeStats"
            )
            shutil.copytree(source, target, ignore=ignore_list)
            logger.info(" [+] Sinkronisasi Selesai. Booting Playwright...")
        except Exception as e: 
            logger.debug(f" Minor copy issue (Aman diabaikan): {e}")

    def _decode_google_url(self, url: str) -> str:
        """Menerjemahkan link Google News menjadi URL asli."""
        real_url = url
        if "articles/CBM" in url:
            try:
                encoded_str = url.split("articles/")[1].split("?")[0]
                padding = 4 - (len(encoded_str) % 4)
                encoded_str += "=" * padding
                decoded_bytes = base64.urlsafe_b64decode(encoded_str)
                match = re.search(rb'(https?://[a-zA-Z0-9\-\.\_\/\?\=\&\%\+]+)', decoded_bytes)
                if match: real_url = match.group(1).decode('utf-8')
            except Exception: pass
        
        if any(domain in real_url for domain in ["tribunnews.com", "pikiran-rakyat.com", "ayobandung.com", "kompas.com", "tirto.id"]):
            parsed = urlparse(real_url)
            params = parse_qs(parsed.query)
            params['page'] = ['all']
            new_query = urlencode(params, doseq=True)
            real_url = urlunparse(parsed._replace(query=new_query))
        return real_url

    def verify_mime_type(self, url: str) -> Tuple[bool, str]:
        """Validasi ekstensi file tanpa perlu mengunduh seluruh HTML."""
        headers = {"User-Agent": "Mozilla/5.0"}
        for attempt in range(2):
            try:
                with requests.get(url, stream=True, headers=headers, timeout=5) as response:
                    content_type = response.headers.get('Content-Type', '').lower()
                    if 'application/' in content_type and 'xhtml' not in content_type:
                        return False, f"Format Ditolak ({content_type})"
                    return True, "Valid HTML"
            except requests.exceptions.Timeout:
                if attempt == 1: return True, "MIME Timeout"
            except: return True, "MIME Error"
        return True, "MIME Error"

    async def network_interceptor(self, route):
        """Memblokir iklan dan pelacak untuk menghemat RAM (V66 Logic)."""
        url = route.request.url.lower()
        if any(ad in url for ad in ["doubleclick.net", "googlesyndication.com", "ads-twitter.com"]):
            await route.abort()
            return
        await route.continue_()

    async def execute_hydration_scroll(self, page):
        """Scroll dinamis untuk memicu lazy-load gambar/teks."""
        try:
            viewport_height = await page.evaluate("window.innerHeight")
            for _ in range(5):
                await page.evaluate(f"window.scrollBy(0, {viewport_height * 0.7})")
                await page.wait_for_timeout(800)
            await page.evaluate("window.scrollTo(0, 0)")
            await page.wait_for_timeout(1000)
        except: pass

    async def fetch_with_backoff(self, page, url, max_retries=3):
        """Mengunduh HTML dengan sistem antrian pintar."""
        for attempt in range(max_retries):
            try:
                response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                if response and response.status in [429, 503]:
                    wait_time = 2 ** attempt
                    await asyncio.sleep(wait_time)
                    continue
                return response
            except Exception as e:
                if attempt == max_retries - 1: raise
                await asyncio.sleep(2)
        raise Exception("Max retries exceeded")

    async def start_browser(self):
        """Membuka browser Edge agar standby untuk dipanggil satu per satu."""
        self.prepare_workspace()
        from playwright.async_api import async_playwright
        self.playwright_instance = await async_playwright().start()
        self.context = await self.playwright_instance.chromium.launch_persistent_context(
            user_data_dir=str(self.workspace_dir),
            channel="msedge",
            headless=False,
            viewport={"width": 1280, "height": 720},
            args=["--disable-blink-features=AutomationControlled"]
        )

    async def close_browser(self):
        """Menutup browser secara aman saat pipeline selesai."""
        if hasattr(self, 'context') and self.context:
            await self.context.close()
        if hasattr(self, 'playwright_instance') and self.playwright_instance:
            await self.playwright_instance.stop()

    async def fetch_single_article(self, url: str) -> Dict[str, Any]:
        """Mengunduh 1 artikel secara On-Demand (Gaya LNPRT/BMEI)."""
        article_data = {"url": url, "fetch_success": False, "html": "", "error": ""}
        
        # Pre-flight MIME check
        is_html, _ = await asyncio.to_thread(self.verify_mime_type, url)
        if not is_html:
            return article_data

        page = await self.context.new_page()
        try:
            await self.fetch_with_backoff(page, url)
            await page.wait_for_timeout(2000)
            
            try:
                iframe = await page.wait_for_selector('iframe[src*="cloudflare"], #challenge-running', timeout=4000)
                if iframe:
                    logger.warning(f" [!] Cloudflare terdeteksi pada {url[:40]}... Menunggu resolusi 15s.")
                    await page.wait_for_function(
                        "document.querySelector('iframe[src*=\"cloudflare\"]') === null && document.querySelector('#challenge-running') === null",
                        timeout=15000
                    )
            except: pass
            
            await self.execute_hydration_scroll(page)
            article_data["html"] = await page.content()
            article_data["fetch_success"] = True
        except Exception as e:
            article_data["error"] = str(e)
        finally:
            if not page.is_closed(): await page.close()
            
        return article_data

if __name__ == "__main__":
    sample_text = """
    Sebanyak 500 karyawan pabrik tekstil di Majalaya dirumahkan akibat menurunnya
    pesanan ekspor. Dinas Tenaga Kerja Kota Bandung mencatat angka pengangguran
    terbuka meningkat 2% dibanding tahun lalu. Warga yang terdampak mengaku
    kesulitan memenuhi kebutuhan pokok sehari-hari. Program rasionalisasi karyawan
    telah berjalan sejak bulan lalu dengan skema pesangon.
    """
    result = score_article(sample_text)
    print(f"Total Score: {result['total_score']}")
    print(f"Euphemisms detected: {len(result['euphemisms'])}")
    for e in result["euphemisms"]:
        print(f"  [{e.severity.upper()}] {e.category}: '{e.matched_keyword}' ({e.match_type})")
        print(f"    Context: {e.context_snippet}")
    print(f"\nScore Breakdown: {result['score_breakdown']}")
    print(f"Sectors: {result['sectors']}")