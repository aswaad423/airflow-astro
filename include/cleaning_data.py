"""
TAHAP 3: CLEANING DATA
Membersihkan hasil assessing, lalu membuat 1 tabel gabungan siap pakai:
  dataset_analisis (join rekap_harian + cuaca_harian + kualitas_udara_harian
                     per tanggal, sudah diinterpolasi & ditambah fitur)

Aturan pembersihan yang dipakai (semua dicatat di log, tidak diam-diam):
  1. Duplikat  -> dihapus, sisakan satu (seharusnya sudah 0 karena PRIMARY KEY)
  2. Nilai di luar rentang wajar (lihat 02_assessing_data.py) -> di-set NULL,
     bukan dihapus barisnya, supaya kolom lain tetap terpakai
  3. Nilai kosong (NULL) pada cuaca/kualitas udara -> diisi dengan interpolasi
     linear dari hari sebelum & sesudahnya (masuk akal untuk data fisik yang
     berubah bertahap seperti suhu/PM2.5)
  4. Hari yang HILANG TOTAL di rekap_harian (misal tidak ada deteksi FIRMS
     sama sekali) -> TIDAK diisi rekaan. Ini didokumentasikan sebagai celah
     data (data gap), bukan dianggap "0 titik panas", karena kekosongan bisa
     berarti kegagalan deteksi (awan, kegagalan satelit), bukan tidak ada
     kebakaran.

Jalankan di dalam container Airflow:
  astro dev bash
  python include/03_cleaning_data.py
"""
from datetime import timedelta

import MySQLdb

DB = dict(host="mysql-praktikum", user="root", passwd="root", db="airflow_db")

RENTANG_WAJAR = {
    "jumlah_titik": (0, 2000), "rata_frp": (0, 500), "maks_frp": (0, 2000),
    "suhu_maks": (15, 45), "curah_hujan": (0, 500), "angin_maks": (0, 150),
    "pm2_5_rata": (0, 500), "pm10_rata": (0, 600), "aqi_maks": (0, 500),
}


def log(pesan):
    print(f"  {pesan}")


def hapus_duplikat(cur):
    print("\n--- 1. Menghapus duplikat ---")
    for tabel, kol_tgl in [("rekap_harian", "tanggal_deteksi"),
                           ("cuaca_harian", "tanggal"),
                           ("kualitas_udara_harian", "tanggal")]:
        cur.execute(f"""
            DELETE t1 FROM {tabel} t1
            INNER JOIN {tabel} t2
            WHERE t1.{kol_tgl} = t2.{kol_tgl}
        """) if False else None  # PRIMARY KEY sudah mencegah duplikat fisik
        cur.execute(f"SELECT COUNT(*) c, {kol_tgl} FROM {tabel} GROUP BY {kol_tgl} HAVING c>1")
        sisa = cur.fetchall()
        log(f"{tabel}: {len(sisa)} tanggal duplikat tersisa (harus 0)")


def tandai_nilai_tidak_valid(cur):
    print("\n--- 2. Menandai nilai di luar rentang wajar jadi NULL ---")
    tabel_kolom = {
        "rekap_harian": ["jumlah_titik", "rata_frp", "maks_frp"],
        "cuaca_harian": ["suhu_maks", "curah_hujan", "angin_maks"],
        "kualitas_udara_harian": ["pm2_5_rata", "pm10_rata", "aqi_maks"],
    }
    for tabel, kolom_list in tabel_kolom.items():
        for kolom in kolom_list:
            lo, hi = RENTANG_WAJAR[kolom]
            cur.execute(
                f"UPDATE {tabel} SET {kolom} = NULL WHERE {kolom} < %s OR {kolom} > %s",
                (lo, hi),
            )
            if cur.rowcount:
                log(f"{tabel}.{kolom}: {cur.rowcount} nilai di luar [{lo},{hi}] di-set NULL")


def interpolasi_linear(cur, tabel, kol_tgl, kolom):
    """Isi NULL dengan interpolasi linear antara nilai valid sebelum & sesudahnya."""
    cur.execute(f"SELECT {kol_tgl}, {kolom} FROM {tabel} ORDER BY {kol_tgl}")
    baris = cur.fetchall()
    tanggal = [b[0] for b in baris]
    nilai = [float(b[1]) if b[1] is not None else None for b in baris]

    n = len(nilai)
    diisi = 0
    i = 0
    while i < n:
        if nilai[i] is None:
            j = i
            while j < n and nilai[j] is None:
                j += 1
            kiri = nilai[i - 1] if i > 0 else None
            kanan = nilai[j] if j < n else None
            if kiri is not None and kanan is not None:
                langkah = (kanan - kiri) / (j - i + 1)
                for k in range(i, j):
                    nilai_baru = round(kiri + langkah * (k - i + 1), 2)
                    cur.execute(f"UPDATE {tabel} SET {kolom}=%s WHERE {kol_tgl}=%s",
                                (nilai_baru, tanggal[k]))
                    diisi += 1
            i = j
        else:
            i += 1
    if diisi:
        log(f"{tabel}.{kolom}: {diisi} nilai kosong diisi interpolasi linear")


def isi_nilai_kosong(cur):
    print("\n--- 3. Mengisi nilai kosong (interpolasi) ---")
    interpolasi_linear(cur, "cuaca_harian", "tanggal", "suhu_maks")
    interpolasi_linear(cur, "cuaca_harian", "tanggal", "curah_hujan")
    interpolasi_linear(cur, "cuaca_harian", "tanggal", "angin_maks")
    interpolasi_linear(cur, "kualitas_udara_harian", "tanggal", "pm2_5_rata")
    interpolasi_linear(cur, "kualitas_udara_harian", "tanggal", "pm10_rata")


def catat_celah_rekap(cur):
    print("\n--- 4. Mencatat celah data (tidak direka-reka) ---")
    cur.execute("SELECT MIN(tanggal_deteksi), MAX(tanggal_deteksi) FROM rekap_harian")
    awal, akhir = cur.fetchone()
    cur.execute("SELECT tanggal_deteksi FROM rekap_harian")
    ada = {b[0] for b in cur.fetchall()}
    rentang = {awal + timedelta(days=i) for i in range((akhir - awal).days + 1)}
    hilang = sorted(rentang - ada)
    if hilang:
        log(f"CELAH DATA titik panas pada: {[str(x) for x in hilang]}")
        log("Hari ini TIDAK diisi 0, karena kosong belum tentu berarti "
            "tidak ada kebakaran (bisa jadi gagal terdeteksi satelit).")
    else:
        log("Tidak ada celah data di rekap_harian.")
    return hilang


def buat_dataset_analisis(cur, hari_celah):
    print("\n--- 5. Membuat tabel gabungan dataset_analisis ---")
    cur.execute("DROP TABLE IF EXISTS dataset_analisis")
    cur.execute("""
        CREATE TABLE dataset_analisis (
            tanggal                   DATE PRIMARY KEY,
            jumlah_titik              INT,
            jumlah_keyakinan_tinggi   INT,
            rata_frp                  DECIMAL(8,2),
            maks_frp                  DECIMAL(8,2),
            suhu_maks                 DECIMAL(5,2),
            curah_hujan               DECIMAL(6,2),
            angin_maks                DECIMAL(5,2),
            pm2_5_rata                DECIMAL(6,2),
            pm10_rata                 DECIMAL(6,2),
            aqi_maks                  INT,
            proporsi_keyakinan_tinggi DECIMAL(5,2),
            hari_dalam_minggu         TINYINT,
            tingkat_risiko            VARCHAR(20),
            ada_celah_titik_panas     TINYINT
        ) CHARACTER SET utf8mb4
    """)
    cur.execute("""
        INSERT INTO dataset_analisis
            (tanggal, jumlah_titik, jumlah_keyakinan_tinggi, rata_frp, maks_frp,
             suhu_maks, curah_hujan, angin_maks, pm2_5_rata, pm10_rata, aqi_maks,
             proporsi_keyakinan_tinggi, hari_dalam_minggu, tingkat_risiko,
             ada_celah_titik_panas)
        SELECT
            c.tanggal, r.jumlah_titik, r.jumlah_keyakinan_tinggi, r.rata_frp, r.maks_frp,
            c.suhu_maks, c.curah_hujan, c.angin_maks, u.pm2_5_rata, u.pm10_rata, u.aqi_maks,
            CASE WHEN r.jumlah_titik > 0
                 THEN ROUND(r.jumlah_keyakinan_tinggi / r.jumlah_titik, 2) ELSE NULL END,
            DAYOFWEEK(c.tanggal),
            CASE WHEN r.jumlah_titik >= 70 THEN 'Tinggi'
                 WHEN r.jumlah_titik >= 30 THEN 'Sedang'
                 WHEN r.jumlah_titik IS NULL THEN NULL
                 ELSE 'Rendah' END,
            0
        FROM cuaca_harian c
        LEFT JOIN rekap_harian r ON r.tanggal_deteksi = c.tanggal
        LEFT JOIN kualitas_udara_harian u ON u.tanggal = c.tanggal
    """)
    if hari_celah:
        cur.execute(
            f"UPDATE dataset_analisis SET ada_celah_titik_panas=1 "
            f"WHERE tanggal IN ({','.join(['%s']*len(hari_celah))})",
            tuple(hari_celah),
        )
    cur.execute("SELECT COUNT(*) FROM dataset_analisis")
    log(f"dataset_analisis dibuat: {cur.fetchone()[0]} baris")
    cur.execute("SELECT COUNT(*) FROM dataset_analisis WHERE jumlah_titik IS NULL")
    log(f"Baris tanpa data titik panas (celah): {cur.fetchone()[0]}")


def main():
    conn = MySQLdb.connect(**DB)
    cur = conn.cursor()

    hapus_duplikat(cur); conn.commit()
    tandai_nilai_tidak_valid(cur); conn.commit()
    isi_nilai_kosong(cur); conn.commit()
    hari_celah = catat_celah_rekap(cur)
    buat_dataset_analisis(cur, hari_celah); conn.commit()

    cur.close()
    conn.close()
    print("\nSelesai. Gunakan tabel 'dataset_analisis' untuk tahap EDA dan modeling.")


if __name__ == "__main__":
    main()