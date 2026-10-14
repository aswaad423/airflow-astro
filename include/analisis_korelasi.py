"""
Analisis korelasi 3 tabel: rekap_harian (titik panas), cuaca_harian, kualitas_udara_harian.
Tanpa library tambahan (pakai statistics bawaan Python).

Jalankan di dalam container Airflow:
  astro dev bash
  python include/analisis_korelasi.py
"""
from datetime import timedelta
from statistics import correlation

import MySQLdb

DB = dict(host="mysql-praktikum", user="root", passwd="root", db="airflow_db")

conn = MySQLdb.connect(**DB)
cur = conn.cursor()
cur.execute("""
    SELECT c.tanggal,
           r.jumlah_titik, r.rata_frp, r.maks_frp,
           c.suhu_maks, c.curah_hujan, c.angin_maks,
           u.pm2_5_rata, u.pm10_rata, u.aqi_maks
    FROM cuaca_harian c
    LEFT JOIN rekap_harian r ON r.tanggal_deteksi = c.tanggal
    LEFT JOIN kualitas_udara_harian u ON u.tanggal = c.tanggal
    ORDER BY c.tanggal
""")
kolom = ["tanggal", "titik", "rata_frp", "maks_frp", "suhu_maks",
         "hujan", "angin", "pm25", "pm10", "aqi"]
data = {}
for baris in cur.fetchall():
    d = dict(zip(kolom, baris))
    for k in kolom[1:]:
        d[k] = float(d[k]) if d[k] is not None else None
    data[d["tanggal"]] = d
cur.close()
conn.close()

print(f"Jumlah hari di cuaca_harian: {len(data)}")
print(f"Hari tanpa data titik panas (tidak ada di rekap_harian): "
      f"{[str(t) for t, d in data.items() if d['titik'] is None]}")


def pasangan(var_x, var_y, lag_x=0):
    """Pasangan (x pada hari t-lag_x, y pada hari t), buang yang kosong."""
    xs, ys = [], []
    for t, d in data.items():
        t_x = t - timedelta(days=lag_x)
        if t_x in data and data[t_x][var_x] is not None and d[var_y] is not None:
            xs.append(data[t_x][var_x])
            ys.append(d[var_y])
    return xs, ys


def korelasi(var_x, var_y, lag_x=0):
    xs, ys = pasangan(var_x, var_y, lag_x)
    if len(xs) < 5:
        return None, None, len(xs)
    return (round(correlation(xs, ys), 3),
            round(correlation(xs, ys, method="ranked"), 3),
            len(xs))


def cetak(judul, daftar):
    print(f"\n=== {judul} ===")
    print(f"{'hubungan':<34}{'Pearson':>9}{'Spearman':>10}{'n':>5}")
    for label, vx, vy, lag in daftar:
        p, s, n = korelasi(vx, vy, lag)
        print(f"{label:<34}{str(p):>9}{str(s):>10}{n:>5}")


cetak("PERTANYAAN 1: cuaca (faktor) vs jumlah titik panas", [
    ("hujan hari yang sama", "hujan", "titik", 0),
    ("hujan 1 hari sebelumnya", "hujan", "titik", 1),
    ("hujan 2 hari sebelumnya", "hujan", "titik", 2),
    ("hujan 3 hari sebelumnya", "hujan", "titik", 3),
    ("suhu maks hari yang sama", "suhu_maks", "titik", 0),
    ("angin maks hari yang sama", "angin", "titik", 0),
])

cetak("PERTANYAAN 2: titik panas vs PM2.5 (dampak)", [
    ("titik panas -> PM2.5 hari sama", "titik", "pm25", 0),
    ("titik panas -> PM2.5 +1 hari", "titik", "pm25", 1),
    ("titik panas -> PM2.5 +2 hari", "titik", "pm25", 2),
    ("FRP rata-rata -> PM2.5 hari sama", "rata_frp", "pm25", 0),
])

print("\n=== PERTANYAAN 3: 5 hari dengan PM2.5 tertinggi ===")
urut = sorted((d for d in data.values() if d["pm25"] is not None),
              key=lambda d: d["pm25"], reverse=True)[:5]
print(f"{'tanggal':<12}{'PM2.5':>8}{'titik panas':>13}{'hujan(mm)':>11}")
for d in urut:
    titik = "-" if d["titik"] is None else int(d["titik"])
    print(f"{str(d['tanggal']):<12}{d['pm25']:>8}{str(titik):>13}{d['hujan']:>11}")

print("\nPanduan membaca: nilai mendekati +1 = naik bersama, -1 = berlawanan, "
      "0 = tidak ada hubungan linear. Pearson sensitif terhadap outlier; "
      "Spearman lebih tahan.")
