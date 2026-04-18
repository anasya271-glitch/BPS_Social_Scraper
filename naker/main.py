# main.py
#!/usr/bin/env python3
"""
main.py — Bandung Socioeconomic Content Analyzer
=================================================
Menjalankan pipeline:
1. Async search berita Bandung (via Google News RSS / custom source)
2. Deteksi euphemisme & klasifikasi
3. Scoring & ranking artikel
4. Output laporan

Usage:
    python main.py
    python main.py --query "PHK pabrik tekstil Bandung"
    python main.py --file input.txt
"""

import asyncio
import argparse
import json
import sys
from datetime import datetime
from typing import List, Dict, Optional

# Import dari modul scraper
from bandung_scraper import (
    detect_euphemisms,
    summarize_euphemisms,
    score_article,
    preprocess_text,
    EUPHEMISM_MAP,
    SECTOR_KEYWORDS,
    IMPACT_KEYWORDS,
    BANDUNG_DISTRICTS,
    BANDUNG_LOCATION_KEYWORDS,
)


# ===========================================================================
# ASYNC CONTENT FETCHER (stub — ganti dengan API/scraper asli)
# ===========================================================================

async def fetch_content(query: str, max_results: int = 10) -> List[Dict]:
    """
    Fetch konten berita/artikel terkait query.
    
    NOTE: Ini adalah stub. Ganti dengan implementasi asli:
    - Google News RSS parser
    - NewsAPI / GNews API
    - Custom web scraper (aiohttp + BeautifulSoup)
    - Database query
    """
    print(f"[FETCH] Searching: '{query}' (max {max_results} results)")
    
    # Contoh data dummy untuk testing
    sample_articles = [
        {
            "title": "500 Karyawan Pabrik Tekstil di Majalaya Dirumahkan",
            "source": "Pikiran Rakyat",
            "date": "2026-04-15",
            "url": "https://example.com/artikel-1",
            "content": """
                Sebanyak 500 karyawan pabrik tekstil PT Maju Jaya di Majalaya
                dirumahkan akibat menurunnya pesanan ekspor. Dinas Tenaga Kerja
                Kota Bandung mencatat angka pengangguran terbuka meningkat 2%
                dibanding tahun lalu. Perusahaan melakukan rasionalisasi karyawan
                dengan skema pesangon. Warga terdampak mengaku kesulitan memenuhi
                kebutuhan pokok. Program bantuan sosial dari pemkot Bandung
                diharapkan dapat meringankan beban para pekerja yang di-PHK.
            """,
        },
        {
            "title": "Demo Buruh Tolak PHK Massal di Kawasan Industri Bandung",
            "source": "Detik Jabar",
            "date": "2026-04-14",
            "url": "https://example.com/artikel-2",
            "content": """
                Ratusan buruh dari serikat pekerja melakukan unjuk rasa di depan
                kantor Disnaker Kota Bandung, menuntut penghentian gelombang PHK
                massal di sektor manufaktur. Para demonstran menolak kebijakan
                efisiensi karyawan yang dianggap sepihak. Mereka juga menuntut
                pembayaran gaji tertunggak dan THR yang belum cair.
                Polisi mengamankan jalannya aksi yang berlangsung damai.
            """,
        },
        {
            "title": "UMKM Bandung Terpuruk, Daya Beli Masyarakat Menurun",
            "source": "Kompas Jabar",
            "date": "2026-04-13",
            "url": "https://example.com/artikel-3",
            "content": """
                Pelaku UMKM di Bandung mengeluhkan penurunan omzet hingga 40%
                sejak awal tahun. Pasar tradisional di Kiaracondong dan Cicaheum
                sepi pembeli. Daya beli masyarakat menurun akibat inflasi tinggi.
                Banyak pedagang kaki lima terpaksa menutup usahanya. Pemkot Bandung
                menjanjikan stimulus berupa pelatihan kerja dan akses kredit UMKM.
            """,
        },
    ]
    
    # Simulasi async delay
    await asyncio.sleep(0.1)
    return sample_articles[:max_results]


# ===========================================================================
# ANALYSIS PIPELINE
# ===========================================================================

async def analyze_content(articles: List[Dict]) -> List[Dict]:
    """Analisis setiap artikel: deteksi euphemisme + scoring."""
    results = []
    
    for i, article in enumerate(articles, 1):
        print(f"[ANALYZE] ({i}/{len(articles)}) {article['title'][:60]}...")
        
        text = article.get("content", "")
        score_result = score_article(text)
        euph_summary = summarize_euphemisms(score_result["euphemisms"])
        
        results.append({
            "title": article["title"],
            "source": article.get("source", "Unknown"),
            "date": article.get("date", "N/A"),
            "url": article.get("url", ""),
            "total_score": score_result["total_score"],
            "score_breakdown": score_result["score_breakdown"],
            "euphemism_summary": euph_summary,
            "sectors": score_result["sectors"],
            "impacts": score_result["impacts"],
            "locations": score_result["locations"],
            "euphemism_details": [
                {
                    "category": e.category,
                    "label": e.label,
                    "severity": e.severity,
                    "keyword": e.matched_keyword,
                    "type": e.match_type,
                    "context": e.context_snippet,
                }
                for e in score_result["euphemisms"]
            ],
        })
    
    # Urutkan berdasarkan skor (tertinggi dulu)
    results.sort(key=lambda x: x["total_score"], reverse=True)
    return results


def print_report(results: List[Dict], query: str) -> None:
    """Cetak laporan hasil analisis ke terminal."""
    sep = "=" * 70
    print(f"\n{sep}")
    print(f"  LAPORAN ANALISIS SOSIO-EKONOMI BANDUNG")
    print(f"  Query: {query}")
    print(f"  Waktu: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Artikel dianalisis: {len(results)}")
    print(sep)
    
    for i, r in enumerate(results, 1):
        print(f"\n--- [{i}] Skor: {r['total_score']:.1f} ---")
        print(f"  Judul   : {r['title']}")
        print(f"  Sumber  : {r['source']} | {r['date']}")
        print(f"  URL     : {r['url']}")
        print(f"  Skor    : E={r['score_breakdown']['euphemism']} "
              f"S={r['score_breakdown']['sector']} "
              f"I={r['score_breakdown']['impact']} "
              f"L={r['score_breakdown']['location']}")
        print(f"  Sektor  : {', '.join(r['sectors']) if r['sectors'] else '-'}")
        print(f"  Lokasi  : {', '.join(r['locations']) if r['locations'] else '-'}")
        
        euph = r["euphemism_summary"]
        print(f"  Euphemisms: {euph['total_matches']} total, "
              f"{euph['high_severity_count']} high-severity")
        if euph["categories_detected"]:
            print(f"  Kategori: {', '.join(euph['categories_detected'])}")
        
        if r["euphemism_details"]:
            print(f"  Detail deteksi:")
            for d in r["euphemism_details"][:5]:
                sev_icon = "🔴" if d["severity"] == "high" else "🟡"
                print(f"    {sev_icon} [{d['category']}] '{d['keyword']}' ({d['type']})")
    
    print(f"\n{sep}")
    print(f"  Selesai. {len(results)} artikel diproses.")
    print(sep)


def save_json(results: List[Dict], filename: str = "output_analysis.json") -> None:
    """Simpan hasil analisis ke file JSON."""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    print(f"[SAVE] Hasil disimpan ke {filename}")


# ===========================================================================
# MAIN
# ===========================================================================

async def main():
    parser = argparse.ArgumentParser(description="Bandung Socioeconomic Analyzer")
    parser.add_argument("--query", "-q", type=str,
                        default="fenomena sosial ekonomi ketenagakerjaan Bandung",
                        help="Query pencarian berita")
    parser.add_argument("--max-results", "-n", type=int, default=10,
                        help="Jumlah maksimal artikel")
    parser.add_argument("--file", "-f", type=str, default=None,
                        help="Analisis teks dari file (bukan search)")
    parser.add_argument("--output", "-o", type=str, default="output_analysis.json",
                        help="File output JSON")
    parser.add_argument("--json-only", action="store_true",
                        help="Output JSON saja, tanpa print report")
    args = parser.parse_args()

    print(f"{'=' * 70}")
    print(f"  Bandung Socioeconomic & Employment Phenomena Analyzer")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 70}")

    if args.file:
        # Mode: analisis file lokal
        print(f"[MODE] Analisis file: {args.file}")
        with open(args.file, "r", encoding="utf-8") as f:
            text = f.read()
        articles = [{"title": args.file, "source": "local", "date": "N/A", "content": text}]
    else:
        # Mode: search & fetch
        print(f"[MODE] Search: '{args.query}'")
        articles = await fetch_content(args.query, args.max_results)

    if not articles:
        print("[WARN] Tidak ada artikel ditemukan.")
        sys.exit(0)

    results = await analyze_content(articles)

    if not args.json_only:
        print_report(results, args.query if not args.file else args.file)

    save_json(results, args.output)


if __name__ == "__main__":
    asyncio.run(main())