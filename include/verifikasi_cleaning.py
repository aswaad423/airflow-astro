"""
VERIFIKASI HASIL CLEANING
Menampilkan bukti bahwa dataset_analisis sudah bersih: sampel data,
jumlah NULL per kolom, cek nilai di luar rentang wajar, dan daftar celah data.

Jalankan di dalam container Airflow:
  astro dev bash
  python include/04_verifikasi_cleaning.py
"""
import MySQLdb

DB = dict(host="mysql-praktikum", user="root", passwd="root", db="airflow_db")

RENTANG_WAJAR = {
    "jumlah_titik": (0, 2000), "rata_frp": (0, 500), "maks_frp": (0, 2000),
    "suhu_maks": (15, 45), "curah_hujan": (0, 500), "angin_maks": (0, 150),
    "pm2_5_rata": (0, 500), "pm10_rata": (0, 600), "aqi_maks": (0, 500),
}


def garis(t):
    print("\n" + "=" * 78)
    print(t)
    print("=" * 78)


def main():
    conn = MySQLdb.connect(**DB)
    cur = conn.cursor()

    garis("1. TOTAL BARIS")
    cur.execute("SELECT COUNT(*) FROM dataset_analisis")
    total = cur.fetchone()[0]
    print(f"  dataset_analisis: {total} baris")

    garis("2. SAMPEL 10 BARIS PERTAMA")
    cur.execute("""
        SELECT tanggal, jumlah_titik, rata_frp, suhu_maks, curah_hujan,
               pm2_5_rata, tingkat_risiko, ada_celah_titik_panas
        FROM dataset_analisis ORDER BY tanggal LIMIT 10
    """)
    print(f"{'tanggal':<12}{'titik':>7}{'frp':>7}{'suhu':>7}{'hujan':>8}"
          f"{'pm2.5':>8}{'risiko':>9}{'celah':>7}")
    for b in cur.fetchall():
        tanggal, titik, frp, suhu, hujan, pm25, risiko, celah = b
        print(f"{str(tanggal):<12}{str(titik):>7}{str(frp):>7}{str(suhu):>7}"
              f"{str(hujan):>8}{str(pm25):>8}{str(risiko):>9}{celah:>7}")

    garis("3. SAMPEL 10 BARIS TERAKHIR")
    cur.execute("""
        SELECT tanggal, jumlah_titik, rata_frp, suhu_maks, curah_hujan,
               pm2_5_rata, tingkat_risiko, ada_celah_titik_panas
        FROM dataset_analisis ORDER BY tanggal DESC LIMIT 10
    """)
    print(f"{'tanggal':<12}{'titik':>7}{'frp':>7}{'suhu':>7}{'hujan':>8}"
          f"{'pm2.5':>8}{'risiko':>9}{'celah':>7}")
    for b in reversed(cur.fetchall()):
        tanggal, titik, frp, suhu, hujan, pm25, risiko, celah = b
        print(f"{str(tanggal):<12}{str(titik):>7}{str(frp):>7}{str(suhu):>7}"
              f"{str(hujan):>8}{str(pm25):>8}{str(risiko):>9}{celah:>7}")

    garis("4. JUMLAH NULL PER KOLOM (setelah cleaning)")
    cur.execute("SHOW COLUMNS FROM dataset_analisis")
    kolom_semua = [k[0] for k in cur.fetchall()]
    for kolom in kolom_semua:
        cur.execute(f"SELECT COUNT(*) FROM dataset_analisis WHERE {kolom} IS NULL")
        n = cur.fetchone()[0]
        tanda = " <- masih ada NULL" if n > 0 else ""
        print(f"  {kolom:<28}: {n:>3} dari {total}{tanda}")

    garis("5. CEK NILAI DI LUAR RENTANG WAJAR (harus 0 semua)")
    for kolom, (lo, hi) in RENTANG_WAJAR.items():
        cur.execute(f"SHOW COLUMNS FROM dataset_analisis LIKE %s", (kolom,))
        if not cur.fetchone():
            continue
        cur.execute(
            f"SELECT COUNT(*) FROM dataset_analisis "
            f"WHERE {kolom} IS NOT NULL AND ({kolom} < %s OR {kolom} > %s)",
            (lo, hi),
        )
        n = cur.fetchone()[0]
        tanda = "TIDAK VALID!" if n > 0 else "OK"
        print(f"  {kolom:<20} rentang [{lo},{hi}] -> {n} nilai bermasalah [{tanda}]")

    garis("6. DAFTAR HARI DENGAN CELAH DATA TITIK PANAS")
    cur.execute("SELECT tanggal FROM dataset_analisis WHERE ada_celah_titik_panas = 1")
    celah = [str(b[0]) for b in cur.fetchall()]
    print(f"  {len(celah)} hari: {celah if celah else 'tidak ada'}")

    garis("7. DISTRIBUSI TINGKAT_RISIKO (setelah cleaning)")
    cur.execute("""
        SELECT COALESCE(tingkat_risiko, '(kosong/celah)'), COUNT(*)
        FROM dataset_analisis GROUP BY tingkat_risiko
    """)
    for label, n in cur.fetchall():
        print(f"  {label:<18}: {n} hari")

    cur.close()
    conn.close()
    print("\nVerifikasi selesai. Kalau bagian 4 dan 5 semuanya 0/OK, "
          "dataset_analisis siap dipakai untuk EDA dan modeling.")


if __name__ == "__main__":
    main()