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
- Scoring & relevance engine untuk memprioritaskan artikel dengan dampak ketenagakerjaan signifikan di Bandung.
- Integrasi dengan BPS AI Engine untuk analisis lanjutan dan klasifikasi otomatis.
- Desain modular untuk kemudahan pemeliharaan, debugging, dan pengembangan fitur di masa depan.
- Logging terperinci untuk setiap tahap proses, termasuk scraping, parsing, scoring, dan analisis AI.
- Penanganan error yang robust dengan fallback dan retry mechanism untuk scraping dan API calls.
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
# 1a. EUPHEMISM DETECTION ENGINE — KATEGORI A–I
# ===========================================================================
EUPHEMISM_MAP = {

    "phk": {
        "label": "PHK / Pemutusan Hubungan Kerja",
        "severity": "high",
        "keywords": [
            r"\bpemutusan hubungan kerja",
            r"\bphk",
            r"\bphk massal",
            r"\brasionalisasi karyawan",
            r"\brasionalisasi pegawai",
            r"\brasionalisasi tenaga kerja",
            r"\brestrukturisasi organisasi",
            r"\brestrukturisasi perusahaan",
            r"\bperampingan organisasi",
            r"\bperampingan struktur",
            r"\befisiensi karyawan",
            r"\befisiensi tenaga kerja",
            r"\befisiensi pegawai",
            r"\befisiensi sdm",
            r"\befisiensi sumber daya manusia",
            r"\boptimalisasi sdm",
            r"\boptimalisasi organisasi",
            r"\bpenyesuaian organisasi",
            r"\bpenyesuaian jumlah karyawan",
            r"\bpenyesuaian tenaga kerja",
            r"\bpengurangan karyawan",
            r"\bpengurangan pegawai",
            r"\bpengurangan tenaga kerja",
            r"\bpengurangan jumlah pekerja",
            r"\bpelepasan karyawan",
            r"\bpelepasan tenaga kerja",
            r"\bpemberhentian karyawan",
            r"\bpemberhentian pegawai",
            r"\bpemberhentian sepihak",
            r"\bpemberhentian sementara",
            r"\bpemangkasan karyawan",
            r"\bpemangkasan pegawai",
            r"\bpemangkasan sdm",
            r"\bmerumahkan karyawan",
            r"\bmerumahkan pekerja",
            r"\bdirumahkan",
            r"\bkaryawan dirumahkan",
            r"\bpekerja dirumahkan",
            r"\btidak diperpanjang kontrak",
            r"\bkontrak tidak diperpanjang",
            r"\bkontrak berakhir",
            r"\bkontrak habis",
            r"\bmasa kontrak habis",
            r"\bputus kontrak",
            r"\btidak diperpanjang",
            r"\bpensiun dini",
            r"\bpensiun dipercepat",
            r"\bprogram pensiun dini",
            r"\bgolden handshake",
            r"\bpesangon",
            r"\buang pesangon",
            r"\bpaket kompensasi",
            r"\bprogram voluntary separation",
            r"\bvoluntary separation program",
            r"\bvsp",
            r"\bmutual separation",
            r"\bpemutusan secara baik-baik",
            r"\bperpisahan secara kekeluargaan",
            r"\bmengundurkan diri",
            r"\bresign massal",
            r"\bdi-phk",
            r"\bkena phk",
            r"\bterkena phk",
            r"\bkorban phk",
            r"\bgelombang phk",
            r"\btsunami phk",
            r"\bbadai phk",
            r"\bdipecat",
            r"\bdikeluarkan",
            r"\bdilepas",
            r"\bdibuang perusahaan",
            r"\bkehilangan pekerjaan",
            r"\bkehilangan mata pencaharian",
            r"\btidak bekerja lagi",
            r"\bpabrik tutup",
            r"\bperusahaan tutup",
            r"\bgulung tikar",
            r"\bbangkrut",
            r"\bpailit",
            r"\bpkpu",
            r"\bsuspensi operasional",
            r"\bmenghentikan operasional",
            r"\bmenghentikan produksi",
            r"\bproduksi berhenti",
            r"\boperasional dihentikan",
            r"\block out",
            r"\blockout",
            r"\bpabrik tekstil tutup",
            r"\bpabrik garmen tutup",
            r"\bburuh pabrik dirumahkan",
            r"\bburuh tekstil di-phk",
            r"\bpekerja garmen dirumahkan",
            r"\bindustri tekstil lesu",
            r"\bindustri garmen terpuruk",
            r"\bpabrik di majalaya tutup",
            r"\bpabrik di cigondewah tutup",
            r"\bsektor tekstil bandung",
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

    "pengangguran": {
        "label": "Pengangguran / Jobless",
        "severity": "high",
        "keywords": [
            r"\bpengangguran",
            r"\bpengangguran terbuka",
            r"\btingkat pengangguran terbuka",
            r"\btpt",
            r"\bpengangguran terselubung",
            r"\bpengangguran friksional",
            r"\bpengangguran struktural",
            r"\bpengangguran musiman",
            r"\bpengangguran siklikal",
            r"\b angka pengangguran",
            r"\bpengangguran meningkat",
            r"\bpengangguran bertambah",
            r"\bpengangguran membengkak",
            r"\bnganggur",
            r"\bmenganggur",
            r"\b belum bekerja",
            r"\b belum mendapat pekerjaan",
            r"\b sedang mencari kerja",
            r"\b pencari kerja",
            r"\b sulit mendapat pekerjaan",
            r"\b sulit cari kerja",
            r"\b susah cari kerja",
            r"\b lapangan kerja sempit",
            r"\b lapangan kerja terbatas",
            r"\b lapangan kerja minim",
            r"\b minimnya lapangan kerja",
            r"\b kurangnya lapangan pekerjaan",
            r"\b tidak ada lowongan",
            r"\b lowongan minim",
            r"\b jobless",
            r"\b job seeker",
            r"\b angkatan kerja menganggur",
            r"\b neet",
            r"\b pemuda menganggur",
            r"\b lulusan menganggur",
            r"\bsarjana menganggur",
            r"\bsarjana nganggur",
            r"\bfresh graduate menganggur",
            r"\bbonus demografi",
            r"\bbeban demografi",
            r"\bsetengah menganggur",
            r"\bsetengah pengangguran",
            r"\bunderemployment",
            r"\bjam kerja kurang",
            r"\bjam kerja rendah",
            r"\bpekerja paruh waktu terpaksa",
            r"\bpengangguran di bandung",
            r"\bangka pengangguran bandung",
            r"\btpt bandung",
            r"\btpt kota bandung",
            r"\bpencari kerja bandung",
            r"\blulusan bandung menganggur",
        ],
        "patterns": [
            r"(?:angka|tingkat|rate|jumlah)\s+pengangguran\s+(?:naik|meningkat|bertambah|membengkak|melonjak|tinggi)",
            r"(?:sulit|susah|sukar)\s+(?:mendapat|mencari|cari|dapat)\s+(?:pekerjaan|kerja|kerjaan)",
            r"(?:ribuan|ratusan|jutaan)\s+(?:orang|warga|penduduk)\s+menganggur",
            r"pengangguran\s+(?:di\s+)?(?:kota\s+)?bandung",
        ],
    },

    "kemiskinan": {
        "label": "Kemiskinan / Kesenjangan Ekonomi",
        "severity": "high",
        "keywords": [
            r"\bkemiskinan",
            r"\b angka kemiskinan",
            r"\bgaris kemiskinan",
            r"\bpenduduk miskin",
            r"\bwarga miskin",
            r"\bmasyarakat miskin",
            r"\bkeluarga miskin",
            r"\brumah tangga miskin",
            r"\bmasyarakat kurang mampu",
            r"\bmasyarakat prasejahtera",
            r"\bprasejahtera",
            r"\bkeluarga prasejahtera",
            r"\bpenerima bantuan sosial",
            r"\bpenerima bansos",
            r"\bdtks",
            r"\bdata terpadu kesejahteraan sosial",
            r"\bkeluarga penerima manfaat",
            r"\bkpm",
            r"\bmasyarakat berpenghasilan rendah",
            r"\bmbr",
            r"\bkelompok rentan",
            r"\brentan miskin",
            r"\bhampir miskin",
            r"\bnear poor",
            r"\bmiskin ekstrem",
            r"\bkemiskinan ekstrem",
            r"\bkemiskinan absolut",
            r"\bkemiskinan relatif",
            r"\bketimpangan",
            r"\bkesenjangan",
            r"\bkesenjangan ekonomi",
            r"\bkesenjangan sosial",
            r"\bgini ratio",
            r"\brasio gini",
            r"\bkoefisien gini",
            r"\bketimpangan pendapatan",
            r"\bketimpangan pengeluaran",
            r"\bwarga tidak mampu",
            r"\bwarga kurang beruntung",
            r"\bhidup di bawah garis kemiskinan",
            r"\bhidup pas-pasan",
            r"\bekonomi pas-pasan",
            r"\bhidup serba kekurangan",
            r"\bkesulitan ekonomi",
            r"\bkesulitan finansial",
            r"\bterpuruk secara ekonomi",
            r"\bekonomi terpuruk",
            r"\bdaya beli menurun",
            r"\bdaya beli melemah",
            r"\bdaya beli rendah",
            r"\bdaya beli turun",
            r"\bkemampuan beli menurun",
            r"\bdeflasi konsumsi",
            r"\bkonsumsi menurun",
            r"\bpengeluaran menurun",
            r"\bsulit memenuhi kebutuhan",
            r"\bkebutuhan pokok mahal",
            r"\bharga kebutuhan naik",
            r"\bbeban hidup berat",
            r"\bbeban ekonomi berat",
            r"\bekonomi sulit",
            r"\bkrisis ekonomi",
            r"\bkemiskinan bandung",
            r"\bwarga miskin bandung",
            r"\bbantuan sosial bandung",
            r"\bbansos bandung",
            r"\bdtks bandung",
            r"\bmbr bandung",
        ],
        "patterns": [
            r"(?:angka|tingkat|jumlah|persentase)\s+kemiskinan\s+(?:naik|meningkat|bertambah|tinggi|melonjak)",
            r"(?:warga|penduduk|masyarakat|keluarga)\s+(?:miskin|prasejahtera|kurang\s+mampu|tidak\s+mampu)",
            r"daya\s+beli\s+(?:menurun|melemah|turun|rendah|anjlok|merosot)",
            r"(?:kesenjangan|ketimpangan)\s+(?:ekonomi|sosial|pendapatan)",
        ],
    },

    "penurunan_ekonomi": {
        "label": "Penurunan Ekonomi / Resesi",
        "severity": "medium",
        "keywords": [
            r"\bpenurunan ekonomi", r"\bperlambatan ekonomi", r"\bpelambatan ekonomi",
            r"\bkontraksi ekonomi", r"\bresesi", r"\bresesi ekonomi", r"\bresesi teknikal",
            r"\bpertumbuhan negatif", r"\bpertumbuhan ekonomi negatif",
            r"\bpertumbuhan ekonomi melambat", r"\bpertumbuhan ekonomi menurun",
            r"\bpertumbuhan ekonomi melemah", r"\bpdb menurun", r"\bpdb negatif",
            r"\bproduk domestik bruto menurun", r"\bdeflasi", r"\bstagflasi",
            r"\bkrisis ekonomi", r"\bkrisis moneter", r"\bkrisis keuangan",
            r"\bkrisis likuiditas", r"\bkrisis fiskal",
            r"\bpenurunan investasi", r"\binvestasi menurun", r"\binvestasi lesu",
            r"\binvestasi stagnan", r"\biklim investasi memburuk",
            r"\bpenurunan omzet", r"\bomzet menurun", r"\bomzet anjlok", r"\bomzet turun drastis",
            r"\bpenurunan pendapatan", r"\bpendapatan menurun", r"\bpendapatan turun",
            r"\bpendapatan merosot", r"\brevenue drop",
            r"\bpenurunan penjualan", r"\bpenjualan menurun", r"\bpenjualan lesu",
            r"\bpenurunan ekspor", r"\bekspor menurun", r"\bekspor turun",
            r"\bpenurunan produksi", r"\bproduksi menurun", r"\bproduksi turun",
            r"\butilisasi rendah", r"\butilisasi menurun", r"\bkapasitas menganggur",
            r"\bovercapacity", r"\bkelebihan kapasitas",
            r"\bneraca perdagangan defisit", r"\bdefisit perdagangan",
            r"\bdefisit anggaran", r"\bdefisit fiskal",
            r"\binflasi tinggi", r"\binflasi melonjak", r"\bharga naik",
            r"\bharga melonjak", r"\bharga melambung", r"\bkenaikan harga",
            r"\bbiaya hidup naik", r"\bbiaya hidup tinggi", r"\bbiaya hidup mahal",
            r"\bekonomi lesu", r"\bekonomi seret", r"\bekonomi melemah",
            r"\bekonomi terpuruk", r"\bekonomi megap-megap", r"\bekonomi morat-marit",
            r"\bekonomi carut-marut", r"\bekonomi amburadul",
            r"\busaha sepi", r"\bdagangan sepi", r"\bpasar sepi", r"\bmall sepi",
            r"\btoko sepi", r"\bsepi pembeli", r"\bsepi pengunjung",
            r"\bbisnis lesu", r"\bbisnis stagnan", r"\bbisnis sulit",
            r"\busaha bangkrut", r"\busaha tutup", r"\bumkm tutup",
            r"\bumkm bangkrut", r"\bumkm terpuruk", r"\bumkm kesulitan",
            r"\bpedagang mengeluh", r"\bpedagang merugi", r"\bpedagang gulung tikar",
            r"\bekonomi bandung lesu", r"\bumkm bandung terpuruk",
            r"\bpasar baru sepi", r"\bpasar andir sepi", r"\bpasar kosambi sepi",
            r"\bwisata bandung sepi", r"\bhotel bandung sepi",
            r"\bpusat perbelanjaan bandung sepi", r"\bfactory outlet sepi",
            r"\bpad bandung menurun", r"\bpendapatan asli daerah bandung turun",
        ],
        "patterns": [
            r"(?:ekonomi|bisnis|usaha|perdagangan)\s+(?:lesu|seret|melemah|terpuruk|stagnan|anjlok)",
            r"(?:omzet|penjualan|pendapatan|ekspor|produksi)\s+(?:turun|menurun|anjlok|merosot|melemah)\s+(?:\d+\s*%|\d+\s*persen)",
            r"(?:inflasi|harga)\s+(?:naik|melonjak|melambung|tinggi)\s+(?:\d+\s*%|\d+\s*persen)?",
            r"pertumbuhan\s+(?:ekonomi\s+)?(?:negatif|minus|melambat|menurun|melemah)",
        ],
    },

    "sektor_informal": {
        "label": "Sektor Informal / Kerentanan Kerja",
        "severity": "medium",
        "keywords": [
            r"\bsektor informal", r"\bpekerja informal", r"\btenaga kerja informal",
            r"\bpekerja tidak tetap", r"\bpekerja lepas", r"\bpekerja harian lepas",
            r"\bpekerja kontrak", r"\bpekerja outsourcing", r"\bpekerja alih daya",
            r"\btenaga kerja alih daya", r"\bburuh harian", r"\bburuh lepas",
            r"\bburuh serabutan", r"\bpekerja serabutan", r"\bkerja serabutan",
            r"\bpekerja gig", r"\bgig economy", r"\bgig worker",
            r"\bpekerja platform", r"\bdriver online", r"\bojol",
            r"\bojek online", r"\bkurir online", r"\bmitra platform",
            r"\bpekerja tanpa jaminan", r"\btanpa jaminan sosial",
            r"\btanpa bpjs", r"\btidak terdaftar bpjs",
            r"\btanpa kontrak kerja", r"\btanpa perjanjian kerja",
            r"\bpekerja rentan", r"\bpekerjaan rentan", r"\bkerentanan kerja",
            r"\bprecarious work", r"\bprecariat",
            r"\bupah rendah", r"\bupah murah", r"\bupah di bawah umr",
            r"\bupah di bawah umk", r"\bupah tidak layak", r"\bgaji tidak layak",
            r"\bupah minimum", r"\bumr", r"\bumk", r"\bump",
            r"\bupah lembur tidak dibayar", r"\bupah telat",
            r"\bupah belum dibayar", r"\bgaji belum dibayar",
            r"\beksploitasi pekerja", r"\beksploitasi buruh",
            r"\bpekerja anak", r"\btenaga kerja anak",
            r"\bpedagang kaki lima", r"\bpkl", r"\bpedagang asongan",
            r"\bpedagang keliling", r"\btukang ojek", r"\btukang becak",
            r"\bpemulung", r"\bpengamen", r"\bpengemis",
            r"\busaha mikro", r"\busaha kecil", r"\busaha rumahan",
            r"\bhome industry", r"\bindustri rumah tangga",
            r"\bekonomi kreatif informal",
            r"\bkerja apa aja", r"\bkerja asal ada", r"\bmakan gaji buta",
            r"\bkerja keras tapi miskin", r"\bworking poor",
            r"\bhidup dari hari ke hari", r"\bnombok terus",
            r"\bpkl bandung", r"\bpkl di jalan", r"\bpkl di trotoar",
            r"\bpedagang kaki lima bandung", r"\bpedagang cibadak",
            r"\bojol bandung", r"\bdriver grab bandung", r"\bdriver gojek bandung",
            r"\bburuh serabutan bandung", r"\bpekerja informal bandung",
        ],
        "patterns": [
            r"(?:pekerja|buruh|tenaga\s+kerja)\s+(?:informal|lepas|harian|serabutan|kontrak|outsourcing)",
            r"(?:tanpa|tidak\s+ada|belum\s+punya)\s+(?:jaminan\s+sosial|bpjs|kontrak\s+kerja|perjanjian\s+kerja)",
            r"upah\s+(?:di\s+bawah|kurang\s+dari|belum\s+sesuai)\s+(?:umr|umk|ump|standar|ketentuan)",
            r"(?:pkl|pedagang\s+kaki\s+lima)\s+(?:ditertibkan|digusur|direlokasi|ditata)",
        ],
    },

    "korupsi": {
        "label": "Korupsi / Penyalahgunaan Anggaran",
        "severity": "medium",
        "keywords": [
            r"\bkorupsi", r"\btindak pidana korupsi", r"\btipikor",
            r"\bpenyalahgunaan anggaran", r"\bpenyalahgunaan wewenang",
            r"\bpenyalahgunaan jabatan", r"\bpenyimpangan anggaran",
            r"\bpenyelewengan anggaran", r"\bpenyelewengan dana",
            r"\bpenyelewengan keuangan", r"\bmark up", r"\bmarkup anggaran",
            r"\bmark up proyek", r"\bpenggelembungan anggaran",
            r"\bpenggelembungan dana", r"\bpenyunatan anggaran",
            r"\bpemotongan anggaran tidak sah",
            r"\bgratifikasi", r"\bsuap", r"\bpenyuapan", r"\bsogok", r"\bmenyogok",
            r"\buang pelicin", r"\buang ketok", r"\buang tanda terima kasih",
            r"\bfee proyek", r"\bkickback", r"\bkomisi gelap",
            r"\bpencucian uang", r"\bmoney laundering",
            r"\bpengadaan fiktif", r"\bproyek fiktif", r"\bproyek siluman",
            r"\btender tertutup", r"\btender arisan",
            r"\bkolusi", r"\bnepotisme", r"\bkkn",
            r"\bkonflik kepentingan", r"\bconflict of interest",
            r"\bpungutan liar", r"\bpungli", r"\bpungutan tidak resmi",
            r"\bpemerasan", r"\bpenggelapan", r"\bpenggelapan dana",
            r"\bmaladministrasi", r"\bmalaadministrasi",
            r"\bkerugian negara", r"\bkerugian daerah", r"\bkerugian keuangan negara",
            r"\btemuan bpk", r"\btemuan audit", r"\bopini disclaimer",
            r"\bopini tidak wajar", r"\bopini tdp",
            r"\bduit haram", r"\buang haram", r"\buang rakyat dikorupsi",
            r"\bbancakan proyek", r"\bbagi-bagi proyek", r"\bproyek pesanan",
            r"\banggaran bocor", r"\bkebocoran anggaran",
            r"\bkoruptor", r"\btersangka korupsi", r"\bterdakwa korupsi",
            r"\bott", r"\boperasi tangkap tangan", r"\bditangkap kpk",
            r"\bdijerat kpk", r"\bkasus korupsi",
            r"\bkorupsi bandung", r"\bkorupsi pemkot bandung",
            r"\bkorupsi apbd bandung", r"\bpungli bandung",
            r"\bkasus korupsi bandung", r"\banggaran bandung bocor",
        ],
        "patterns": [
            r"(?:kasus|dugaan|indikasi|perkara)\s+(?:korupsi|tipikor|suap|gratifikasi|penggelapan)",
            r"(?:kerugian\s+(?:negara|daerah|keuangan))\s+(?:sebesar|mencapai|senilai)\s+(?:rp|IDR)",
            r"(?:ott|operasi\s+tangkap\s+tangan)\s+(?:kpk|kejaksaan|polisi)",
            r"(?:penyalahgunaan|penyelewengan|penyimpangan)\s+(?:anggaran|dana|wewenang|jabatan)",
        ],
    },

    "bencana": {
        "label": "Bencana Alam & Lingkungan",
        "severity": "medium",
        "keywords": [
            r"\bbencana alam", r"\bbencana", r"\bbanjir", r"\bbanjir bandang",
            r"\bbanjir rob", r"\bgenangan", r"\bgenangan air",
            r"\btanah longsor", r"\blongsor", r"\bpergerakan tanah",
            r"\bgempa bumi", r"\bgempa", r"\bgempa tektonik",
            r"\bkebakaran", r"\bkebakaran hutan", r"\bkebakaran lahan",
            r"\bkekeringan", r"\bkemarau panjang", r"\bkrisis air",
            r"\bkekurangan air bersih", r"\bair bersih langka",
            r"\bpencemaran lingkungan", r"\bpolusi", r"\bpolusi udara",
            r"\bpolusi air", r"\bpencemaran air", r"\bpencemaran sungai",
            r"\bpencemaran tanah", r"\blimbah", r"\blimbah industri",
            r"\blimbah pabrik", r"\blimbah b3", r"\blimbah beracun",
            r"\bsampah menumpuk", r"\bsampah menggunung", r"\bkrisis sampah",
            r"\btpa penuh", r"\btpa overload",
            r"\bcuaca ekstrem", r"\bangin kencang", r"\bangin puting beliung",
            r"\bhujan deras", r"\bhujan lebat", r"\bperubahan iklim",
            r"\bpemanasan global", r"\bel nino", r"\bla nina",
            r"\brob", r"\babrasi", r"\berosi",
            r"\bpohon tumbang", r"\bjalan amblas", r"\bjalan rusak",
            r"\binfrastruktur rusak", r"\brumah rusak", r"\brumah roboh",
            r"\bpengungsi bencana", r"\bkorban bencana", r"\bterdampak bencana",
            r"\bdaerah rawan bencana", r"\bzona merah bencana",
            r"\bdarurat bencana", r"\btanggap darurat", r"\bstatus darurat",
            r"\bbanjir bandung", r"\bbanjir cicaheum", r"\bbanjir cibiru",
            r"\bbanjir pagarsih", r"\bbanjir pasteur", r"\bbanjir dayeuhkolot",
            r"\blongsor bandung", r"\blongsor lembang", r"\blongsor punclut",
            r"\bpolusi udara bandung", r"\bkualitas udara bandung",
            r"\bispu bandung", r"\bsampah bandung", r"\btpa sarimukti",
            r"\bsungai cikapundung tercemar", r"\bsungai citarum tercemar",
            r"\bkebakaran bandung", r"\bkebakaran pasar bandung",
        ],
        "patterns": [
            r"(?:banjir|longsor|gempa|kebakaran)\s+(?:di\s+)?(?:kota\s+)?bandung",
            r"(?:korban|pengungsi|terdampak)\s+(?:banjir|longsor|gempa|bencana)\s+(?:mencapai|sebanyak|bertambah)\s+\d+",
            r"(?:polusi|pencemaran|kualitas)\s+(?:udara|air|sungai)\s+(?:bandung|memburuk|berbahaya|tidak\s+sehat)",
            r"(?:sampah|limbah)\s+(?:menumpuk|menggunung|meluap|mencemari)",
        ],
    },

    "kesehatan": {
        "label": "Masalah Kesehatan Masyarakat",
        "severity": "medium",
        "keywords": [
            r"\bwabah", r"\bpandemi", r"\bepidemi", r"\bendemi",
            r"\bklb", r"\bkejadian luar biasa", r"\boutbreak",
            r"\bstunting", r"\bgizi buruk", r"\bmalnutrisi", r"\bkurang gizi",
            r"\bgizi kurang", r"\bbusung lapar", r"\bkelaparan",
            r"\brain pangan", r"\bkrisis pangan", r"\bketahanan pangan rendah",
            r"\bkematian ibu", r"\bkematian bayi", r"\bkematian balita",
            r"\bangka kematian ibu", r"\baki", r"\bangka kematian bayi", r"\bakb",
            r"\bdemam berdarah", r"\bdbd", r"\btbc", r"\btuberkulosis",
            r"\bdiare", r"\bcampak", r"\bdifteri", r"\bpolio",
            r"\bcovid", r"\bcovid-19", r"\bvarian baru",
            r"\bpenyakit menular", r"\bpenyakit tropis",
            r"\bfasilitas kesehatan kurang", r"\bpuskesmas minim",
            r"\btenaga kesehatan kurang", r"\bkekurangan dokter",
            r"\bbpjs kesehatan", r"\bbpjs defisit", r"\biuran bpjs naik",
            r"\bobat langka", r"\bobat mahal", r"\bobat tidak tersedia",
            r"\bkesehatan jiwa", r"\bgangguan mental", r"\bdepresi",
            r"\bbunuh diri", r"\bpercobaan bunuh diri",
            r"\bstunting bandung", r"\bdbd bandung", r"\btbc bandung",
            r"\brsud bandung", r"\bpuskesmas bandung",
            r"\bkesehatan masyarakat bandung",
        ],
        "patterns": [
            r"(?:kasus|angka|jumlah)\s+(?:stunting|dbd|tbc|covid|diare|campak)\s+(?:naik|meningkat|melonjak|tinggi)",
            r"(?:wabah|klb|outbreak)\s+(?:di\s+)?(?:kota\s+)?bandung",
            r"(?:fasilitas|tenaga)\s+kesehatan\s+(?:kurang|minim|tidak\s+memadai|terbatas)",
        ],
    },

    "konflik_buruh": {
        "label": "Konflik Industrial / Buruh",
        "severity": "medium",
        "keywords": [
            r"\bmogok kerja", r"\bmogok", r"\baksi mogok", r"\bmogok massal",
            r"\bdemo buruh", r"\bdemo pekerja", r"\bdemo karyawan",
            r"\bunjuk rasa buruh", r"\bunjuk rasa pekerja",
            r"\bdemonstrasi buruh", r"\bprotes buruh", r"\bprotes pekerja",
            r"\baksi buruh", r"\baksi pekerja", r"\baksi massa buruh",
            r"\bserikat buruh", r"\bserikat pekerja", r"\bsp", r"\bsb",
            r"\bfederasi serikat pekerja", r"\bkonfederasi buruh",
            r"\bperselisihan hubungan industrial", r"\bphi",
            r"\bperselisihan kerja", r"\bsengketa kerja",
            r"\bpengadilan hubungan industrial",
            r"\bbipartit", r"\btripartit", r"\bmediasi ketenagakerjaan",
            r"\bsomasi", r"\bgugatan buruh", r"\bgugatan pekerja",
            r"\bhak normatif tidak dibayar", r"\bhak normatif dilanggar",
            r"\bupah tidak dibayar", r"\bthr tidak dibayar",
            r"\bthr telat", r"\bthr dipotong",
            r"\blembur tidak dibayar", r"\bjamsostek tidak dibayar",
            r"\bpelanggaran ketenagakerjaan", r"\bpelanggaran uu ketenagakerjaan",
            r"\bintimidasi buruh", r"\bintimidasi pekerja",
            r"\bunion busting", r"\banti serikat",
            r"\bpekerja migran", r"\btki", r"\btkw", r"\bpmi",
            r"\bpekerja migran indonesia", r"\bburuh migran",
            r"\bdemo buruh bandung", r"\bmogok kerja bandung",
            r"\baksi buruh bandung", r"\bserikat pekerja bandung",
            r"\bunjuk rasa buruh bandung", r"\bkonflik industrial bandung",
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
    match_type: str          
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
        for kw in cat_data.get("keywords", []):
            kw_lower = kw.lower()
            idx = 0
            while True:
                pos = normalized.find(kw_lower, idx)
                if pos == -1:
                    break
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
                logger.warning(f"Regex match error in category '{cat_key}': {e}")
                continue
    matches.sort(key=lambda x: (x.position[0], -(x.position[1] - x.position[0])))
    deduped = []
    last_end = -1
    for m in matches:
        if m.position[0] >= last_end:
            deduped.append(m)
            last_end = m.position[1]
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
    "A_pertanian": [r"\bpertanian", r"\bperkebunan", r"\bpeternakan", r"\bperikanan", r"\bagribisnis", r"\bpangan", r"\bhortikultura", r"\bagribisnis", r"\bpangan", r"\bpadi", r"\bsawah", r"\bpetani", r"\bnelayan", r"\bpeternak", r"\bikan", r"\budang", r"\brumput laut", r"\bkopi", r"\bteh", r"\bkakao", r"\bkaret", r"\bsawit", r"\bbuah", r"\bbuah-buahan", r"\bsayur", r"\bsayuran"],
    "B_pertambangan": [r"\bpertambangan", r"\btambang", r"\bmineral", r"\bgalian", r"\bbatubara", r"\bemas", r"\bperak", r"\btembaga", r"\bnikel", r"\btimah", r"\bbatu[\s\-]?bara", r"minyak[\s\-]?bumi", r"\bgas[\s\-]?bumi", r"\bminyak", r"\bgas"],
    "C_manufaktur": [r"\bmanufaktur", r"\bindustri", r"\bpabrik", r"\bproduksi", r"\btekstil", r"\bgarmen", r"\botomotif", r"\belektronik", r"\bfarmasi", r"\bmakanan olahan", r"\bminuman olahan", r"\bkertas", r"\bplastik", r"\blogam", r"\bkimia", r"\bpermesinan", r"\balat berat"],
    "D_listrik_gas": [r"\blistrik", r"\bgas", r"\benergi", r"\bpln", r"\bpembangkit", r"\bpipa[\s\-]?gas", r"\bjaringan[\s\-]?listrik", r"\benergi[\s\-]?terbarukan", r"\benergi[\s\-]?fosil", r"\benergi", r"\bpembangkit[\s\-]?listrik", r"\bpembangkit[\s\-]?energi"],
    "E_sampah": [r"air[\s\-]?bersih", r"\bpengelolaan[\s\-]?limbah", r"\bsanitasi", "pdam", "sampah", "tpa", r"\btempat[\s\-]?pembuangan[\s\-]?akhir", r"\bpengelolaan[\s\-]?sampah", r"\bdaur[\s\-]?ulang", r"\brecycling"],
    "F_konstruksi": [r"\bkonstruksi", r"\bpembangunan", r"\bproyek[\s\-]?infrastruktur", r"\bproperti", r"\breal[\s\-]?estate", r"\bbangunan", r"\bjalan", r"\bjembatan", r"\bgedung", r"\bperumahan", r"\bapartemen"],
    "G_perdagangan": [r"\bperdagangan", r"\bretail", r"\btoko", r"\bpasar", r"\be-commerce", r"\bmarketplace", r"\bekspor", r"\bimpor", r"\bdistributor", r"\bgrosir", r"\bmal", r"\bmall", r"\bpusat[\s\-]?perbelanjaan", r"factory[\s\-]?outlet"],
    "H_transportasi": [r"\btransportasi", r"\blogistik", r"\bangkutan", r"\bpengiriman", r"\bbandara", r"\bpelabuhan", r"kereta[\s\-]?api", r"\bbus", r"\btruk", r"\bkendaraan", r"\btransportasi[\s\-]?online", r"\bojol"],
    "I_akomodasi": [r"\bhotel", r"\brestoran", r"\bkafe", r"\bkuliner", r"\bpariwisata", r"\bwisata", r"\bhospitality", r"\bpenginapan", r"\bmakanan", r"\bminuman", r"\bcatering", r"\bevent organizer", r"\bpenginapan", r"\bakomodasi"],
    "J_informasi_komunikasi": [r"teknologi[\s\-]?informasi", r"tele[\s\-]?komunikasi", "media", "startup", "it", "digital", "software", "hardware", "aplikasi", "internet", "komunikasi", r"konten[\s\-]?digital"],
    "K_keuangan_asuransi": [r"\bperbankan", r"\bbank", r"\basuransi", r"\bfintech", r"\bkeuangan", r"\bkredit", r"\bpinjaman", r"\binvestasi", r"\bsaham", r"\bobligasi", r"\breksa[\s\-]?dana", r"\basuransi[\s\-]?jiwa", r"\basuransi[\s\-]?kesehatan"],
    "L_real_estate": [r"\breal[\s\-]?estate", r"\bproperti", r"\bperumahan", r"\bapartemen", r"\bkomersial", r"\bresidensial", r"\bsewa", r"\bkontrakan", r"\bjual[\s\-]?beli properti", r"\bpengembangan[\s\-]?properti"],
    "O_administrasi_pemerintahan": [r"\bpemerintah", r"\bpemerintahan", r"\basn", r"\bpns", r"\bbirokrasi", r"\bapbd", r"\bapbn", r"\banggaran", r"\bdana[\s\-]?desa", r"\bdana[\s\-]?alokasi khusus", r"\bdana bantuan operasional sekolah", r"\bdbos", r"\botonomi[\s\-]?daerah", r"\bpemkot", r"\bbps", r"\bbps[\s\-]?kota[\s\-]?bandung", r"\bbadan[\s\-]?pusat[\s\-]?statistik", r"\bbadan[\s\-]?pusat[\s\-]?statistik[\s\-]?bandung", r"\bbadan[\s\-]?pusat[\s\-]?statistik[\s\-]?kota[\s\-]?bandung", r"\bkementrian", r"\bpemprov", r"\bpemerintah[\s\-]?provinsi", r"\bpemerintah[\s\-]?daerah", r"\bpemerintah[\s\-]?kabupaten", r"\bpemerintah[\s\-]?kota", r"\bpemerintah", r"\bpemda", r"\bBMKG", r"\bkantor", r"\bdinas", r"\bsekretariat", r"\bBPJS", r"\bpengadilan", r"\bkejaksaan", r"\bpolisi", r"\bkeamanan[\s\-]?publik", r"\bketertiban[\s\-]?umum", r"\bbadan", r"\bkepolisian", r"\bojk", r"\botoritas[\s\-]?jasa[\s\-]?keuangan"],
    "P_pendidikan": [r"\bpendidikan", r"\bsekolah", r"\buniversitas", r"\bkampus", r"\buniv", r"\bguru", r"\bdosen", r"\bsiswa", r"\bmahasiswa", r"\bkurikulum", r"\bpembelajaran", r"\bkelas", r"\bruang[\s\-]?belajar", r"\bpendidikan[\s\-]?vokasi", r"\bpelatihan[\s\-]?kerja", r"\bkursus", r"\bbimbingan[\s\-]?belajar"],
    "Q_kesehatan_sosial": [r"\bkesehatan", r"\brumah[\s\-]?sakit", r"\bklinik", r"\bdokter", r"\bperawat", r"\bsosial", r"\bkesejahteraan[\s\-]?sosial", r"\bbpjs[\s\-]?kesehatan", r"\bbpjs[\s\-]?ketenagakerjaan", r"\bpanti[\s\-]?sosial", r"\bpanti[\s\-]?jompo", r"\bpanti[\s\-]?asuhan"],
    "R_kesenian_hiburan": [r"\bseni", r"\bbudaya", r"\bhiburan", r"\bmusik", r"\bfilm", r"\bevent", r"\bkreatif", r"\bindustri[\s\-]?kreatif", r"\bkegiatan[\s\-]?seni", r"\bkegiatan[\s\-]?budaya"],
    "S_jasa_lainnya": [r"\bjasa", r"\blaundry", r"\bbengkel", r"\bsalon", r"\bperawatan", r"\bkecantikan", r"\bkesehatan[\s\-]?alternatif", r"\bkaraoke", r"\bcatering", r"\bevent[\s\-]?organizer"],
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
    "cibeunying kaler": [r"\bcibeunying[\s\-]?kaler", r"\bcigadung", r"\bcihaurgeulis", r"\bneglasari", r"\bsukaluyu"],
    "cibeunying kidul": [r"\bcibeunying[\s\-]?kidul", r"\bcicadas", r"\bcikutra", r"\bpadasuka", r"\bsukamaju", r"\bsukapada"],
    "gedebage": [r"\bgedebage", r"\bcimincrang", r"\bcisaranten[\s\-]?kidul", r"\brancabolang", r"\brancanumpang"],
    "kiaracondong": [r"\bkiaracondong", r"\bbabakan[\s\-]?surabaya", r"\bcicaheum", r"\bkebon[\s\-]?jayanti", r"\bkebon[\s\-]?kangkung", r"\bsukapura"],
    "lengkong": [r"\blengkong", r"\bburangrang", r"\bcijagra", r"\bcikawao", r"\blingkar[\s\-]?selatan", r"\bmalabar", r"\bpaledang", r"\bturangga"],
    "mandalajati": [r"\bmandalajati", r"\bjati[\s\-]?handap", r"\bkarang[\s\-]?pamulang", r"\bpasir[\s\-]?impun", r"\bsindang[\s\-]?jaya"],
    "panyileukan": [r"\bpanyileukan", r"\bcibiruwetan", r"\bcipadung[\s\-]?kidul", r"\bcipadung[\s\-]?kulon", r"\bcipadung[\s\-]?wetan", r"\bmekarsari"],
    "rancasari": [r"\brancasari", r"\bcipamokolan", r"\bderwati", r"\bmanjahlega", r"\bmekarjaya"],
    "regol": [r"\bregol", r"\bancol", r"\bbalonggede", r"\bcisereuh", r"\bcigereleng", r"\bpungkur", r"\bpasirluyu"],
    "sukajadi": [r"\bsukajadi", r"\bcipedes", r"\bpasteur", r"\bsukabungah", r"\bsukagalih", r"\bsukawarna"],
    "sukasari": [r"\bsukasari", r"\bgegerkalong", r"\bisola", r"\bsarijadi", r"\bsukarasa"],
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