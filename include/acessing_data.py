"""
TAHAP 2: ASSESSING DATA
Menilai kualitas data di 3 tabel utama (rekap_harian, cuaca_harian,
kualitas_udara_harian) sebelum masuk tahap cleaning.

Empat aspek yang dinilai:
  1. Completeness  -> ada tidaknya nilai kosong / hari yang hilang
  2. Uniqueness    -> ada tidaknya baris duplikat
  3. Validity      -> nilai berada di luar rentang yang masuk akal
  4. Consistency   -> tipe data dan format seragam

Jalankan di dalam container Airflow:
  astro dev bash
  python include/02_assessing_data.py
"""
from datetime import timedelta

import MySQLdb

DB = dict(host="mysql-praktikum", user="root", passwd="root", db="airflow_db")

# batas nilai wajar untuk cek validity (longgar, hanya menangkap kesalahan jelas)
RENTANG_WAJAR = {
    "jumlah_titik": (0, 2000),
    "rata_frp": (0, 500),
    "maks_frp": (0, 2000),
    "suhu_maks": (15, 45),
    "curah_hujan": (0, 500),
    "angin_maks": (0, 150),
    "pm2_5_rata": (0, 500),
    "pm10_rata": (0, 600),
    "aqi_maks": (0, 500),
}

TABEL = {
    "rekap_harian": "tanggal_deteksi",
    "cuaca_harian": "tanggal",
    "kualitas_udara_harian": "tanggal",
}


def cetak_judul(t):
    print("\n" + "=" * 70)
    print(t)
    print("=" * 70)


def main():
    conn = MySQLdb.connect(**DB)
    cur = conn.cursor()

    # ------------------------------------------------------------
    # 1. COMPLETENESS: jumlah baris, nilai NULL per kolom, hari hilang
    # ------------------------------------------------------------
    cetak_judul("1. COMPLETENESS (Kelengkapan Data)")
    rentang_tanggal = {}
    for tabel, kol_tgl in TABEL.items():
        cur.execute(f"SELECT * FROM {tabel}")
        baris = cur.fetchall()
        nama_kolom = [d[0] for d in cur.description]
        n = len(baris)
        print(f"\n--- {tabel}: {n} baris ---")

        if n == 0:
            print("  TIDAK ADA DATA sama sekali.")
            continue

        idx_tgl = nama_kolom.index(kol_tgl)
        tanggal = sorted(b[idx_tgl] for b in baris)
        rentang_tanggal[tabel] = set(tanggal)

        for i, kol in enumerate(nama_kolom):
            kosong = sum(1 for b in baris if b[i] is None)
            if kosong > 0:
                print(f"  [KOSONG] kolom '{kol}': {kosong} dari {n} baris ({kosong/n:.1%})")

        awal, akhir = tanggal[0], tanggal[-1]
        rentang_penuh = {awal + timedelta(days=i) for i in range((akhir - awal).days + 1)}
        hilang = sorted(rentang_penuh - set(tanggal))
        if hilang:
            print(f"  [HARI HILANG] {len(hilang)} hari tidak tercatat: "
                  f"{[str(x) for x in hilang]}")
        else:
            print("  Tidak ada hari yang hilang dalam rentang tanggalnya.")

    # ------------------------------------------------------------
    # 2. UNIQUENESS: duplikat tanggal
    # ------------------------------------------------------------
    cetak_judul("2. UNIQUENESS (Duplikasi)")
    for tabel, kol_tgl in TABEL.items():
        cur.execute(f"""
            SELECT {kol_tgl}, COUNT(*) c FROM {tabel}
            GROUP BY {kol_tgl} HAVING c > 1
        """)
        dup = cur.fetchall()
        if dup:
            print(f"  [DUPLIKAT] {tabel}: {len(dup)} tanggal muncul lebih dari sekali")
        else:
            print(f"  {tabel}: tidak ada duplikat tanggal (PRIMARY KEY bekerja).")

    cur.execute("""
        SELECT latitude, longitude, tanggal_deteksi, waktu_deteksi, COUNT(*) c
        FROM hotspot_harian
        GROUP BY latitude, longitude, tanggal_deteksi, waktu_deteksi HAVING c > 1
    """)
    dup_hotspot = cur.fetchall()
    print(f"  hotspot_harian: {len(dup_hotspot)} baris duplikat "
          f"(harus 0 karena PRIMARY KEY gabungan).")

    # ------------------------------------------------------------
    # 3. VALIDITY: nilai di luar rentang wajar
    # ------------------------------------------------------------
    cetak_judul("3. VALIDITY (Keabsahan Nilai)")
    for kolom, (lo, hi) in RENTANG_WAJAR.items():
        for tabel in TABEL:
            cur.execute(f"SHOW COLUMNS FROM {tabel} LIKE %s", (kolom,))
            if not cur.fetchone():
                continue
            cur.execute(
                f"SELECT COUNT(*) FROM {tabel} WHERE {kolom} < %s OR {kolom} > %s",
                (lo, hi),
            )
            n_invalid = cur.fetchone()[0]
            tanda = "[TIDAK VALID]" if n_invalid > 0 else "  OK "
            print(f"  {tanda} {tabel}.{kolom}: {n_invalid} baris di luar rentang [{lo}, {hi}]")

    cur.execute("SELECT DISTINCT keyakinan FROM hotspot_harian")
    nilai_keyakinan = [r[0] for r in cur.fetchall()]
    aneh = [v for v in nilai_keyakinan if v not in ("n", "l", "h")]
    print(f"  Nilai unik kolom 'keyakinan': {nilai_keyakinan} "
          f"{'-> ADA NILAI ANEH: ' + str(aneh) if aneh else '(semua valid)'}")

    cur.execute("SELECT DISTINCT siang_malam FROM hotspot_harian")
    nilai_dn = [r[0] for r in cur.fetchall()]
    aneh_dn = [v for v in nilai_dn if v not in ("D", "N")]
    print(f"  Nilai unik kolom 'siang_malam': {nilai_dn} "
          f"{'-> ADA NILAI ANEH: ' + str(aneh_dn) if aneh_dn else '(semua valid)'}")

    # ------------------------------------------------------------
    # 4. CONSISTENCY: tipe data tiap kolom
    # ------------------------------------------------------------
    cetak_judul("4. CONSISTENCY (Tipe Data)")
    for tabel in list(TABEL) + ["hotspot_harian", "wilayah"]:
        cur.execute(f"DESCRIBE {tabel}")
        print(f"\n--- {tabel} ---")
        for kol, tipe, null_ok, key, default, extra in cur.fetchall():
            print(f"  {kol:<24}{tipe:<18}{'NULL diizinkan' if null_ok=='YES' else 'NOT NULL'}")

    cur.close()
    conn.close()
    print("\nSelesai. Lihat baris berlabel [KOSONG]/[HARI HILANG]/[DUPLIKAT]/[TIDAK VALID] "
          "sebagai daftar masalah yang perlu ditangani di tahap Cleaning.")


if __name__ == "__main__":
    main()