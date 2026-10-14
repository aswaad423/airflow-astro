"""
Tes lokal (di luar Airflow): ekstrak -> cek kualitas -> preprocessing data
titik panas (hotspot) NASA FIRMS untuk wilayah Sulawesi Tenggara.
Jalankan:  python tes_firms_lokal.py
Butuh:     pip install requests
"""
import os
from datetime import date
from io import StringIO
import csv

import requests

MAP_KEY = os.environ.get("FIRMS_MAP_KEY") or input("Masukkan MAP_KEY NASA FIRMS: ").strip()
SOURCE = "VIIRS_SNPP_NRT"
# Kotak koordinat (barat, selatan, timur, utara) mencakup Sulawesi Tenggara
AREA = "120.5,-6.2,124.0,-3.0"
DAY_RANGE = 5  # maksimum yang diizinkan API

URL = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{MAP_KEY}/{SOURCE}/{AREA}/{DAY_RANGE}"

# ---------------------------------------------------------------- TAHAP 1
print("\n=== TAHAP 1: EKSTRAK (AMBIL DATA) ===")
r = requests.get(URL, timeout=30)
r.raise_for_status()
teks = r.text.strip()

if teks.startswith("Invalid") or teks.startswith("Rentang"):
    raise SystemExit(f"API mengembalikan error: {teks}")

pembaca = csv.DictReader(StringIO(teks))
baris_mentah = list(pembaca)
print("Jumlah baris mentah:", len(baris_mentah))
if baris_mentah:
    print("Contoh 1 baris mentah:")
    print(baris_mentah[0])

# ---------------------------------------------------------------- TAHAP 2
print("\n=== TAHAP 2: CEK KUALITAS DATA MENTAH ===")
tingkat_keyakinan = {}
siang_malam = {}
for b in baris_mentah:
    k = b.get("confidence", "?")
    tingkat_keyakinan[k] = tingkat_keyakinan.get(k, 0) + 1
    dn = b.get("daynight", "?")
    siang_malam[dn] = siang_malam.get(dn, 0) + 1

print("Distribusi tingkat keyakinan (confidence):", tingkat_keyakinan)
print("Distribusi siang/malam (daynight):", siang_malam)

# ---------------------------------------------------------------- TAHAP 3
print("\n=== TAHAP 3: PREPROCESSING ===")
tanggal_ambil = date.today().isoformat()
bersih = []
for b in baris_mentah:
    keyakinan = b.get("confidence", "n")
    # buang titik dengan keyakinan rendah, sering berupa gangguan (noise)
    if keyakinan == "l":
        continue
    try:
        lat = float(b["latitude"])
        lon = float(b["longitude"])
        frp = float(b.get("frp") or 0)
    except (ValueError, KeyError):
        continue

    bersih.append({
        "tanggal_ambil": tanggal_ambil,
        "tanggal_deteksi": b.get("acq_date"),
        "waktu_deteksi": b.get("acq_time"),
        "latitude": lat,
        "longitude": lon,
        "keyakinan": keyakinan,
        "frp": round(frp, 2),
        "siang_malam": b.get("daynight"),
    })

print("Jumlah data mentah   :", len(baris_mentah))
print("Dibuang (keyakinan l):", len(baris_mentah) - len(bersih))
print("Jumlah data bersih   :", len(bersih))
print("\n5 baris pertama setelah preprocessing:")
for x in bersih[:5]:
    print(x)
