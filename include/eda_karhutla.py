"""
EDA (Exploratory Data Analysis) untuk 3 tabel:
  rekap_harian, cuaca_harian, kualitas_udara_harian
Tanpa library tambahan (statistics bawaan Python).

Jalankan di dalam container Airflow:
  astro dev bash
  python include/eda_karhutla.py
"""
from collections import defaultdict
from datetime import timedelta
from statistics import correlation, mean, median, pstdev, quantiles, stdev

import MySQLdb

DB = dict(host="host.docker.internal", user="root", passwd="root", db="airflow_db")

TABEL = {
    "rekap_harian (titik panas)": ("rekap_harian", "tanggal_deteksi",
        ["jumlah_titik", "jumlah_keyakinan_tinggi", "rata_frp", "maks_frp",
         "jumlah_siang", "jumlah_malam"]),
    "cuaca_harian (cuaca)": ("cuaca_harian", "tanggal",
        ["suhu_maks", "suhu_min", "suhu_rata", "curah_hujan", "jam_hujan",
         "angin_maks", "hembusan_maks"]),
    "kualitas_udara_harian (udara)": ("kualitas_udara_harian", "tanggal",
        ["pm2_5_rata", "pm2_5_maks", "pm10_rata", "pm10_maks", "co_rata", "aqi_maks"]),
}

conn = MySQLdb.connect(**DB)
cur = conn.cursor()
seri = {}      # nama_kolom -> {tanggal: nilai}
tanggal_per_tabel = {}


def f(x):
    return float(x) if x is not None else None


def skew(v):
    m, s = mean(v), pstdev(v)
    return 0.0 if s == 0 else sum((x - m) ** 3 for x in v) / len(v) / s ** 3


print("=" * 78)
print("BAGIAN 1: STATISTIK DESKRIPTIF PER VARIABEL")
print("=" * 78)
for judul, (tabel, kol_tgl, kolom) in TABEL.items():
    cur.execute(f"SELECT {kol_tgl}, {', '.join(kolom)} FROM {tabel} ORDER BY {kol_tgl}")
    baris = cur.fetchall()
    tanggal = [b[0] for b in baris]
    tanggal_per_tabel[tabel] = set(tanggal)
    print(f"\n--- {judul}: {len(baris)} baris, {tanggal[0]} s.d. {tanggal[-1]} ---")
    print(f"{'variabel':<24}{'kosong':>7}{'min':>9}{'Q1':>9}{'median':>9}{'Q3':>9}"
          f"{'maks':>10}{'rata2':>9}{'simp.baku':>10}{'skew':>7}{'outlier':>8}")
    for i, k in enumerate(kolom, start=1):
        pasang = {b[0]: f(b[i]) for b in baris}
        seri[k] = pasang
        v = [x for x in pasang.values() if x is not None]
        kosong = len(pasang) - len(v)
        if len(v) < 4:
            print(f"{k:<24}{kosong:>7}  (data terlalu sedikit)")
            continue
        q1, _, q3 = quantiles(v, n=4, method="inclusive")
        iqr = q3 - q1
        outlier = sum(1 for x in v if x < q1 - 1.5 * iqr or x > q3 + 1.5 * iqr)
        print(f"{k:<24}{kosong:>7}{min(v):>9.2f}{q1:>9.2f}{median(v):>9.2f}{q3:>9.2f}"
              f"{max(v):>10.2f}{mean(v):>9.2f}{stdev(v):>10.2f}{skew(v):>7.2f}{outlier:>8}")

print("\n" + "=" * 78)
print("BAGIAN 2: KELENGKAPAN TANGGAL (hari yang hilang di tiap tabel)")
print("=" * 78)
semua = set().union(*tanggal_per_tabel.values())
awal, akhir = min(semua), max(semua)
rentang = {awal + timedelta(days=i) for i in range((akhir - awal).days + 1)}
for tabel, tgl in tanggal_per_tabel.items():
    hilang = sorted(rentang - tgl)
    print(f"{tabel:<24} hari hilang: {[str(t) for t in hilang] if hilang else 'tidak ada'}")

print("\n" + "=" * 78)
print("BAGIAN 3: HITUNGAN KATEGORI")
print("=" * 78)
hujan = [x for x in seri["curah_hujan"].values() if x is not None]
print(f"Hari hujan (>=1 mm)            : {sum(1 for x in hujan if x >= 1)} dari {len(hujan)}")
print(f"Hari kering (<1 mm)            : {sum(1 for x in hujan if x < 1)} dari {len(hujan)}")
print(f"Hari tanpa hujan sama sekali   : {sum(1 for x in hujan if x == 0)} dari {len(hujan)}")
pm = [x for x in seri["pm2_5_rata"].values() if x is not None]
print(f"Hari PM2.5 > 15 ug/m3 (WHO 24j): {sum(1 for x in pm if x > 15)} dari {len(pm)}")
print(f"Hari PM2.5 <= 15 ug/m3         : {sum(1 for x in pm if x <= 15)} dari {len(pm)}")
tp = [x for x in seri["jumlah_titik"].values() if x is not None]
print(f"Hari titik panas >= 100        : {sum(1 for x in tp if x >= 100)} dari {len(tp)}")

print("\n" + "=" * 78)
print("BAGIAN 4: RATA-RATA MINGGUAN (untuk melihat tren)")
print("=" * 78)
minggu = defaultdict(list)
for t in sorted(semua):
    minggu[t - timedelta(days=t.weekday())].append(t)
print(f"{'minggu mulai':<14}{'hari':>5}{'titik(rata)':>13}{'suhu maks':>11}"
      f"{'hujan(total)':>14}{'PM2.5':>8}")
for awal_mg, hari in sorted(minggu.items()):
    def ambil(k):
        return [seri[k][t] for t in hari if t in seri[k] and seri[k][t] is not None]
    titik, suhu, hj, p25 = ambil("jumlah_titik"), ambil("suhu_maks"), ambil("curah_hujan"), ambil("pm2_5_rata")
    print(f"{str(awal_mg):<14}{len(hari):>5}"
          f"{(mean(titik) if titik else float('nan')):>13.1f}"
          f"{(mean(suhu) if suhu else float('nan')):>11.1f}"
          f"{(sum(hj) if hj else float('nan')):>14.1f}"
          f"{(mean(p25) if p25 else float('nan')):>8.1f}")

print("\n" + "=" * 78)
print("BAGIAN 5: MATRIKS KORELASI SPEARMAN (variabel utama)")
print("=" * 78)
utama = ["jumlah_titik", "maks_frp", "suhu_maks", "curah_hujan", "angin_maks", "pm2_5_rata"]
print(f"{'':<14}" + "".join(f"{k[:11]:>12}" for k in utama))
for a in utama:
    baris_teks = f"{a[:13]:<14}"
    for b in utama:
        bersama = [t for t in seri[a] if t in seri[b]
                   and seri[a][t] is not None and seri[b][t] is not None]
        if len(bersama) < 5:
            baris_teks += f"{'-':>12}"
        else:
            r = correlation([seri[a][t] for t in bersama],
                            [seri[b][t] for t in bersama], method="ranked")
            baris_teks += f"{r:>12.2f}"
    print(baris_teks)

cur.close()
conn.close()
print("\nSelesai.")
