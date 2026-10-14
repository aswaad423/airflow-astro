"""
Backfill data historis titik panas FIRMS (60 hari terakhir) + feature engineering
untuk persiapan machine learning.

Jalankan SEKALI SAJA di dalam container Airflow:
  astro dev bash
  python include/backfill_dan_fitur.py
"""
import csv
import os
import time
from datetime import date, timedelta
from io import StringIO

import MySQLdb
import requests

MAP_KEY = os.environ.get("FIRMS_MAP_KEY") or input("Masukkan MAP_KEY NASA FIRMS: ").strip()
SOURCE = "VIIRS_SNPP_NRT"
AREA = "120.5,-6.2,124.0,-3.0"
DAY_RANGE = 5          # maksimum per request
TOTAL_HARI_MUNDUR = 60 # coba tarik 60 hari ke belakang

DB = dict(host="mysql-praktikum", user="root", passwd="root", db="airflow_db")


def ambil_chunk(tanggal_mulai: date) -> str:
    url = (
        f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
        f"{MAP_KEY}/{SOURCE}/{AREA}/{DAY_RANGE}/{tanggal_mulai.isoformat()}"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.text.strip()


def simpan_baris(cur, teks_csv: str) -> int:
    pembaca = csv.DictReader(StringIO(teks_csv))
    jumlah = 0
    for b in pembaca:
        keyakinan = b.get("confidence", "n")
        if keyakinan == "l":
            continue
        try:
            lat = round(float(b["latitude"]), 5)
            lon = round(float(b["longitude"]), 5)
            frp = round(float(b.get("frp") or 0), 2)
        except (ValueError, KeyError):
            continue
        tanggal_deteksi = b.get("acq_date")
        waktu_deteksi = str(b.get("acq_time") or "0").zfill(4)
        cur.execute(
            """
            REPLACE INTO hotspot_harian
                (latitude, longitude, tanggal_deteksi, waktu_deteksi,
                 wilayah_id, keyakinan, frp, siang_malam, tanggal_ambil)
            VALUES (%s, %s, %s, %s,
                    (SELECT wilayah_id FROM wilayah WHERE nama_wilayah='Sulawesi Tenggara'),
                    %s, %s, %s, CURDATE())
            """,
            (lat, lon, tanggal_deteksi, waktu_deteksi, keyakinan, frp, b.get("daynight")),
        )
        jumlah += 1
    return jumlah


def main():
    conn = MySQLdb.connect(**DB)
    cur = conn.cursor()

    total_disimpan = 0
    hari_ini = date.today()
    titik_mulai = hari_ini
    langkah = 0

    while langkah * DAY_RANGE < TOTAL_HARI_MUNDUR:
        tgl_awal_chunk = titik_mulai - timedelta(days=(langkah + 1) * DAY_RANGE - 1)
        print(f"Menarik data mulai {tgl_awal_chunk} ({DAY_RANGE} hari) ...")
        try:
            teks = ambil_chunk(tgl_awal_chunk)
            if teks.startswith(("Invalid", "Rentang", "Kesalahan")):
                print(f"  -> dilewati, pesan API: {teks[:100]}")
            else:
                n = simpan_baris(cur, teks)
                conn.commit()
                total_disimpan += n
                print(f"  -> {n} baris disimpan")
        except Exception as e:
            print(f"  -> gagal, dilewati: {e}")

        langkah += 1
        time.sleep(1)  # jaga-jaga supaya tidak kena limit request

    print(f"\nTotal baris disimpan (setelah filter keyakinan rendah): {total_disimpan}")

    # Hitung ulang rekap_harian untuk SEMUA tanggal yang ada
    print("\nMenghitung ulang rekap_harian ...")
    cur.execute("""
        REPLACE INTO rekap_harian
            (tanggal_deteksi, jumlah_titik, jumlah_keyakinan_tinggi,
             rata_frp, maks_frp, jumlah_siang, jumlah_malam)
        SELECT
            tanggal_deteksi,
            COUNT(*),
            SUM(CASE WHEN keyakinan='h' THEN 1 ELSE 0 END),
            ROUND(AVG(frp),2),
            MAX(frp),
            SUM(CASE WHEN siang_malam='D' THEN 1 ELSE 0 END),
            SUM(CASE WHEN siang_malam='N' THEN 1 ELSE 0 END)
        FROM hotspot_harian
        GROUP BY tanggal_deteksi
    """)
    conn.commit()

    # ---------------- Feature engineering ----------------
    print("Menambah kolom fitur untuk ML ...")
    for kolom, tipe in [
        ("proporsi_keyakinan_tinggi", "DECIMAL(5,2)"),
        ("proporsi_siang", "DECIMAL(5,2)"),
        ("hari_dalam_minggu", "TINYINT"),
        ("tingkat_risiko", "VARCHAR(20)"),
    ]:
        try:
            cur.execute(f"ALTER TABLE rekap_harian ADD COLUMN {kolom} {tipe}")
        except MySQLdb.OperationalError:
            pass  # kolom sudah ada, lewati
    conn.commit()

    cur.execute("""
        UPDATE rekap_harian
        SET proporsi_keyakinan_tinggi = ROUND(jumlah_keyakinan_tinggi / jumlah_titik, 2),
            proporsi_siang = ROUND(jumlah_siang / jumlah_titik, 2),
            hari_dalam_minggu = DAYOFWEEK(tanggal_deteksi),
            tingkat_risiko = CASE
                WHEN jumlah_titik >= 70 THEN 'Tinggi'
                WHEN jumlah_titik >= 30 THEN 'Sedang'
                ELSE 'Rendah'
            END
    """)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM rekap_harian")
    total_hari = cur.fetchone()[0]
    print(f"\nSelesai. Total hari (baris) di rekap_harian sekarang: {total_hari}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
