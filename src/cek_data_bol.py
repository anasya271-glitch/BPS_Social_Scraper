import pandas as pd
import os

bol_file = "Draft_Audit_Ekspor_Bandung.xlsx"

if not os.path.exists(bol_file):
    print(f"[!] File {bol_file} tidak ditemukan.")
else:
    bol_df = pd.read_excel(bol_file)
    print("="*50)
    print(" DIAGNOSTIK DATA BILL OF LADING (BoL)")
    print("="*50)
    print(f"Total Baris Data BoL : {len(bol_df)}")
    print("\n[DAFTAR NAMA PENGIRIM / SHIPPER DI BoL]:")
    
    shippers = bol_df['Shipper'].dropna().unique()
    for i, shipper in enumerate(shippers, 1):
        print(f" {i}. {shipper}")
    
    print("\n[DAFTAR DESKRIPSI BARANG]:")
    desc = bol_df['Description'].dropna().unique()
    for i, d in enumerate(desc[:5], 1):
        print(f" {i}. {d}")