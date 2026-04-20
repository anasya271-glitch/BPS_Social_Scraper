import ollama
import sys

def main():
    print("Membuka jalur komunikasi LANGSUNG ke Ollama (Direct Path)...")
    
    # Daftar model kustom Anda
    models = ["bps-naker", "bps-lnprt", "bmei-auditor"]
    
    # Contoh teks untuk diuji
    test_data = {
        "bps-naker": "Dibutuhkan tenaga kerja bagian administrasi gudang, pendidikan minimal SMA.",
        "bps-lnprt": "Kegiatan donor darah di Masjid Al-Barkah hari Minggu besok.",
        "bmei-auditor": "Data ekspor tekstil bulan Maret dari pelabuhan Cirebon."
    }

    for model_id in models:
        print(f"\n>>> Menguji Model: {model_id}...")
        try:
            # Memanggil Ollama secara langsung (Bypass Llama Stack)
            response = ollama.chat(model=model_id, messages=[
                {'role': 'user', 'content': f"Analisis teks ini secara singkat: {test_data[model_id]}"},
            ])
            print(f"✅ Berhasil! Respon dari {model_id}:")
            print(response['message']['content'])
        except Exception as e:
            print(f"❌ Gagal memanggil {model_id}. Error: {e}")
            print(f"💡 Tips: Pastikan sudah menjalankan 'ollama create {model_id}' sebelumnya.")

if __name__ == "__main__":
    main()