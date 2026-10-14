"""
Membuat dan mengisi 2 tabel tambahan untuk analisis karhutla Sultra:
  - cuaca_harian          (Open-Meteo Archive API)
  - kualitas_udara_harian (Open-Meteo Air Quality API, hourly -> diagregasi harian)

Titik lokasi: Kendari (-3.97, 122.51). Satu titik tidak mewakili seluruh Sultra,
catat sebagai keterbatasan di laporan.

Jalankan di dalam container Airflow:
  astro dev bash
  python include/backfill_cuaca_udara.py
"""
from collections import defaultdict

import MySQLdb
import requests

DB = dict(host="mysql-praktikum", user="root", passwd="root", db="airflow_db")
LAT, LON = -3.97, 122.51
TGL_MULAI, TGL_AKHIR = "2026-07-30", "2026-09-27"
TZ = "Asia/Makassar"

CREATE_SQL = [
    """
    CREATE TABLE IF NOT EXISTS cuaca_harian (
        tanggal        DATE PRIMARY KEY,
        suhu_maks      DECIMAL(5,2),
        suhu_min       DECIMAL(5,2),
        suhu_rata      DECIMAL(5,2),
        curah_hujan    DECIMAL(6,2),
        jam_hujan      DECIMAL(4,1),
        angin_maks     DECIMAL(5,2),
        hembusan_maks  DECIMAL(5,2)
    ) CHARACTER SET utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS kualitas_udara_harian (
        tanggal      DATE PRIMARY KEY,
        pm2_5_rata   DECIMAL(6,2),
        pm2_5_maks   DECIMAL(6,2),
        pm10_rata    DECIMAL(6,2),
        pm10_maks    DECIMAL(6,2),
        co_rata      DECIMAL(8,2),
        aqi_maks     INT
    ) CHARACTER SET utf8mb4
    """,
]


def rata(nilai):
    v = [x for x in nilai if x is not None]
    return round(sum(v) / len(v), 2) if v else None


def maks(nilai):
    v = [x for x in nilai if x is not None]
    return max(v) if v else None


def tarik_cuaca(cur):
    r = requests.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude": LAT, "longitude": LON,
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
    return n


def tarik_udara(cur):
    r = requests.get(
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        params={
            "latitude": LAT, "longitude": LON,
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
            (tgl, rata(v["pm25"]), maks(v["pm25"]), rata(v["pm10"]),
             maks(v["pm10"]), rata(v["co"]), int(aqi) if aqi is not None else None),
        )
        n += 1
    return n


def main():
    conn = MySQLdb.connect(**DB)
    cur = conn.cursor()
    for sql in CREATE_SQL:
        cur.execute(sql)
    conn.commit()

    for nama, fungsi in [("cuaca_harian", tarik_cuaca), ("kualitas_udara_harian", tarik_udara)]:
        try:
            n = fungsi(cur)
            conn.commit()
            print(f"{nama}: {n} hari disimpan")
        except Exception as e:
            print(f"{nama}: GAGAL -> {e}")

    cur.execute("SELECT COUNT(*) FROM cuaca_harian WHERE curah_hujan IS NULL")
    print("Hari cuaca tanpa data hujan (NULL):", cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM kualitas_udara_harian WHERE pm2_5_rata IS NULL")
    print("Hari udara tanpa data PM2.5 (NULL):", cur.fetchone()[0])
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
