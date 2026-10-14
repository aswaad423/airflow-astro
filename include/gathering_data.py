"""
TAHAP 1: DATA GATHERING
Proyek: ETL Titik Panas Karhutla, Cuaca, dan Kualitas Udara - Sulawesi Tenggara

Sumber data:
  1. NASA FIRMS         -> titik panas (hotspot) kebakaran hutan dan lahan
  2. Open-Meteo Archive  -> cuaca harian (suhu, hujan, angin)
  3. Open-Meteo Air Qlty -> kualitas udara harian (PM2.5, PM10, CO, AQI)

Lokasi: kotak koordinat Sulawesi Tenggara (untuk FIRMS) dan titik Kendari
        -3.97, 122.51 (untuk cuaca & kualitas udara).

Cara menjalankan (di dalam container Airflow):
  astro dev bash
  python include/01_gathering_data.py
"""
import csv
from collections import defaultdict
from datetime import date, timedelta
from io import StringIO
import os
import time

import MySQLdb
import requests

# ------------------------------------------------------------------
# KONFIGURASI
# ------------------------------------------------------------------
MAP_KEY = os.environ.get("FIRMS_MAP_KEY") or input("Masukkan MAP_KEY NASA FIRMS: ").strip()

DB = dict(host="mysql-praktikum", user="root", passwd="root", db="airflow_db")

FIRMS_SOURCE = "VIIRS_SNPP_NRT"
FIRMS_AREA = "120.5,-6.2,124.0,-3.0"   # barat, selatan, timur, utara (Sulawesi Tenggara)
FIRMS_DAY_RANGE = 5                   # maksimum per request dari API
FIRMS_TOTAL_HARI_MUNDUR = 365         # DIPERBARUI: Tarik 365 hari (1 tahun) ke belakang agar data ML akurat

LAT_CUACA, LON_CUACA = -3.97, 122.51    # titik Kendari
TGL_MULAI = (date.today() - timedelta(days=365)).isoformat() # DIPERBARUI: Menyesuaikan 1 tahun ke belakang
TGL_AKHIR = date.today().isoformat()
TZ = "Asia/Makassar"


# ------------------------------------------------------------------
# BAGIAN A: BUAT SEMUA TABEL
# ------------------------------------------------------------------
def buat_tabel(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wilayah (
            wilayah_id   INT PRIMARY KEY AUTO_INCREMENT,
            nama_wilayah VARCHAR(100) UNIQUE
        ) CHARACTER SET utf8mb4
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hotspot_harian (
            latitude        DECIMAL(9,5) NOT NULL,
            longitude       DECIMAL(9,5) NOT NULL,
            tanggal_deteksi DATE NOT NULL,
            waktu_deteksi   VARCHAR(4) NOT NULL,
            wilayah_id      INT,
            keyakinan       VARCHAR(1),
            frp             DECIMAL(8,2),
            siang_malam     VARCHAR(1),
            tanggal_ambil   DATE,
            PRIMARY KEY (latitude, longitude, tanggal_deteksi, waktu_deteksi),
            FOREIGN KEY (wilayah_id) REFERENCES wilayah(wilayah_id)
        ) CHARACTER SET utf8mb4
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rekap_harian (
            tanggal_deteksi         DATE PRIMARY KEY,
            jumlah_titik            INT,
            jumlah_keyakinan_tinggi INT,
            rata_frp                DECIMAL(8,2),
            maks_frp                DECIMAL(8,2),
            jumlah_siang            INT,
            jumlah_malam            INT
        ) CHARACTER SET utf8mb4
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cuaca_harian (
            tanggal         DATE PRIMARY KEY,
            suhu_maks       DECIMAL(5,2),
            suhu_min        DECIMAL(5,2),
            suhu_rata       DECIMAL(5,2),
            curah_hujan     DECIMAL(6,2),
            jam_hujan       DECIMAL(4,1),
            angin_maks      DECIMAL(5,2),
            hembusan_maks   DECIMAL(5,2)
        ) CHARACTER SET utf8mb4
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS kualitas_udara_harian (
            tanggal      DATE PRIMARY KEY,
            pm2_5_rata   DECIMAL(6,2),
            pm2_5_maks   DECIMAL(6,2),
            pm10_rata    DECIMAL(6,2),
            pm10_maks    DECIMAL(6,2),
            co_rata      DECIMAL(8,2),
            aqi_maks     INT
        ) CHARACTER SET utf8mb4
    """)
    cur.execute("INSERT IGNORE INTO wilayah (nama_wilayah) VALUES ('Sulawesi Tenggara')")
    print("Semua tabel siap.")


# ------------------------------------------------------------------
# BAGIAN B: TARIK DATA TITIK PANAS (FIRMS)
# ------------------------------------------------------------------
def ambil_chunk_firms(tanggal_mulai: date) -> str:
    url = (
        f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
        f"{MAP_KEY}/{FIRMS_SOURCE}/{FIRMS_AREA}/{FIRMS_DAY_RANGE}/{tanggal_mulai.isoformat()}"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.text.strip()


def simpan_hotspot(cur, teks_csv: str) -> int:
    pembaca = csv.DictReader(StringIO(teks_csv))
    jumlah = 0
    for b in pembaca:
        keyakinan = b.get("confidence", "n")
        if keyakinan == "l":  # buang keyakinan rendah (noise)
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


def tarik_firms(cur):
    print("\n--- Menarik data titik panas (NASA FIRMS) ---")
    total = 0
    hari_ini = date.today()
    langkah = 0
    while langkah * FIRMS_DAY_RANGE < FIRMS_TOTAL_HARI_MUNDUR:
        tgl_awal = hari_ini - timedelta(days=(langkah + 1) * FIRMS_DAY_RANGE - 1)
        print(f"  Mulai {tgl_awal} ({FIRMS_DAY_RANGE} hari) ...", end=" ")
        try:
            teks = ambil_chunk_firms(tgl_awal)
            if teks.startswith(("Invalid", "Rentang", "Kesalahan")):
                print(f"dilewati -> {teks[:80]}")
            else:
                n = simpan_hotspot(cur, teks)
                total += n
                print(f"{n} baris")
        except Exception as e:
            print(f"gagal -> {e}")
        langkah += 1
        time.sleep(1)

    print(f"  Total titik panas disimpan: {total}")

    # hitung ulang rekap harian dari seluruh hotspot_harian
    cur.execute("""
        REPLACE INTO rekap_harian
            (tanggal_deteksi, jumlah_titik, jumlah_keyakinan_tinggi,
             rata_frp, maks_frp, jumlah_siang, jumlah_malam)
        SELECT
            tanggal_deteksi, COUNT(*),
            SUM(CASE WHEN keyakinan='h' THEN 1 ELSE 0 END),
            ROUND(AVG(frp),2), MAX(frp),
            SUM(CASE WHEN siang_malam='D' THEN 1 ELSE 0 END),
            SUM(CASE WHEN siang_malam='N' THEN 1 ELSE 0 END)
        FROM hotspot_harian
        GROUP BY tanggal_deteksi
    """)
    print("  rekap_harian selesai dihitung ulang.")


# ------------------------------------------------------------------
# BAGIAN C: TARIK DATA CUACA (Open-Meteo Archive)
# ------------------------------------------------------------------
def tarik_cuaca(cur) -> int:
    print("\n--- Menarik data cuaca (Open-Meteo Archive) ---")
    r = requests.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude": LAT_CUACA, "longitude": LON_CUACA,
            "start_date": TGL_MULAI, "end_date": TGL_AKHIR,
            "daily": "temperature_2m_max,temperature_2m_min,temperature_2m_mean,"
                     "precipitation_sum,precipitation_hours,"
                     "wind_speed_10m_max,wind_gusts_10m_max",
            "timezone": TZ,
        },
        timeout=60,
    )
    r.raise_for_status()
    d = r.json()["daily"]
    n = 0
    for i, tgl in enumerate(d["time"]):
        cur.execute(
            "REPLACE INTO cuaca_harian VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (tgl, d["temperature_2m_max"][i], d["temperature_2m_min"][i],
             d["temperature_2m_mean"][i], d["precipitation_sum"][i],
             d["precipitation_hours"][i], d["wind_speed_10m_max"][i],
             d["wind_gusts_10m_max"][i]),
        )
        n += 1
    print(f"  {n} hari cuaca disimpan.")
    return n


# ------------------------------------------------------------------
# BAGIAN D: TARIK DATA KUALITAS UDARA (Open-Meteo Air Quality)
# ------------------------------------------------------------------
def rata2(nilai):
    v = [x for x in nilai if x is not None]
    return round(sum(v) / len(v), 2) if v else None


def maks(nilai):
    v = [x for x in nilai if x is not None]
    return max(v) if v else None


def tarik_udara(cur) -> int:
    print("\n--- Menarik data kualitas udara (Open-Meteo Air Quality) ---")
    r = requests.get(
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        params={
            "latitude": LAT_CUACA, "longitude": LON_CUACA,
            "start_date": TGL_MULAI, "end_date": TGL_AKHIR,
            "hourly": "pm10,pm2_5,carbon_monoxide,us_aqi",
            "timezone": TZ,
        },
        timeout=60,
    )
    r.raise_for_status()
    h = r.json()["hourly"]
    per_hari = defaultdict(lambda: defaultdict(list))
    for i, waktu in enumerate(h["time"]):
        tgl = waktu[:10]
        per_hari[tgl]["pm25"].append(h["pm2_5"][i])
        per_hari[tgl]["pm10"].append(h["pm10"][i])
        per_hari[tgl]["co"].append(h["carbon_monoxide"][i])
        per_hari[tgl]["aqi"].append(h["us_aqi"][i])

    n = 0
    for tgl, v in sorted(per_hari.items()):
        aqi = maks(v["aqi"])
        cur.execute(
            "REPLACE INTO kualitas_udara_harian VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (tgl, rata2(v["pm25"]), maks(v["pm25"]), rata2(v["pm10"]),
             maks(v["pm10"]), rata2(v["co"]), int(aqi) if aqi is not None else None),
        )
        n += 1
    print(f"  {n} hari kualitas udara disimpan.")
    return n


# ------------------------------------------------------------------
# PROGRAM UTAMA
# ------------------------------------------------------------------
def main():
    conn = MySQLdb.connect(**DB)
    cur = conn.cursor()

    buat_tabel(cur)
    conn.commit()

    tarik_firms(cur)
    conn.commit()

    try:
        tarik_cuaca(cur)
        conn.commit()
    except Exception as e:
        print(f"  GAGAL tarik cuaca -> {e}")

    try:
        tarik_udara(cur)
        conn.commit()
    except Exception as e:
        print(f"  GAGAL tarik kualitas udara -> {e}")

    print("\n=== RINGKASAN JUMLAH BARIS ===")
    for tabel in ["wilayah", "hotspot_harian", "rekap_harian",
                  "cuaca_harian", "kualitas_udara_harian"]:
        cur.execute(f"SELECT COUNT(*) FROM {tabel}")
        print(f"  {tabel:<24}: {cur.fetchone()[0]} baris")

    cur.close()
    conn.close()
    print("\nSelesai. Data gathering tuntas.")


if __name__ == "__main__":
    main()