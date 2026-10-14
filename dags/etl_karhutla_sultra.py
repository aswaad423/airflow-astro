from __future__ import annotations

import csv
from io import StringIO

import pendulum
import requests

from airflow.sdk import Variable, dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

MYSQL_CONN_ID = "mysql_local"
FIRMS_SOURCE = "VIIRS_SNPP_NRT"
FIRMS_AREA = "120.5,-6.2,124.0,-3.0"
DAY_RANGE = 3

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS wilayah (
    wilayah_id   INT PRIMARY KEY AUTO_INCREMENT,
    nama_wilayah VARCHAR(100) UNIQUE
) CHARACTER SET utf8mb4;

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
) CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS rekap_harian (
    tanggal_deteksi   DATE PRIMARY KEY,
    jumlah_titik      INT,
    jumlah_keyakinan_tinggi INT,
    rata_frp          DECIMAL(8,2),
    maks_frp          DECIMAL(8,2),
    jumlah_siang      INT,
    jumlah_malam      INT
) CHARACTER SET utf8mb4;

INSERT IGNORE INTO wilayah (nama_wilayah) VALUES ('Sulawesi Tenggara');
"""


@dag(
    dag_id="etl_karhutla_sultra",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Makassar"),
    schedule="@daily",
    catchup=False,
    tags=["praktikum", "etl", "firms", "karhutla", "mysql"],
)
def etl_karhutla_sultra():
    buat_tabel_mysql = SQLExecuteQueryOperator(
        task_id="buat_tabel_mysql",
        conn_id=MYSQL_CONN_ID,
        sql=CREATE_TABLE_SQL,
        split_statements=True,
    )

    @task
    def ekstrak_data() -> str:
        map_key = Variable.get("firms_map_key")
        url = (
            f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
            f"{map_key}/{FIRMS_SOURCE}/{FIRMS_AREA}/{DAY_RANGE}"
        )
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        teks = r.text.strip()
        if teks.startswith("Invalid") or teks.startswith("Rentang") or teks.startswith("Kesalahan"):
            raise ValueError(f"FIRMS API mengembalikan error: {teks}")
        return teks

    @task
    def transformasi_data(teks_csv: str) -> list[list]:
        tanggal_ambil = pendulum.now("Asia/Makassar").to_date_string()
        pembaca = csv.DictReader(StringIO(teks_csv))
        baris = []
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
            baris.append([
                lat, lon, tanggal_deteksi, waktu_deteksi,
                keyakinan, frp, b.get("daynight"), tanggal_ambil,
            ])
        if not baris:
            raise ValueError("Tidak ada baris valid setelah transformasi.")
        return baris

    @task
    def muat_ke_mysql(baris: list[list]) -> dict:
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        wilayah_id = hook.get_first(
            "SELECT wilayah_id FROM wilayah WHERE nama_wilayah = %s",
            parameters=("Sulawesi Tenggara",),
        )[0]
        baris_lengkap = [b[:4] + [wilayah_id] + b[4:] for b in baris]
        hook.insert_rows(
            table="hotspot_harian",
            rows=[tuple(b) for b in baris_lengkap],
            target_fields=[
                "latitude", "longitude", "tanggal_deteksi", "waktu_deteksi",
                "wilayah_id", "keyakinan", "frp", "siang_malam", "tanggal_ambil",
            ],
            replace=True,
            commit_every=200,
        )
        tanggal_terpengaruh = sorted({b[2] for b in baris})
        hook.run(
            """
            INSERT INTO rekap_harian
                (tanggal_deteksi, jumlah_titik, jumlah_keyakinan_tinggi,
                 rata_frp, maks_frp, jumlah_siang, jumlah_malam)
            SELECT
                tanggal_deteksi,
                COUNT(*) AS jumlah_titik,
                SUM(CASE WHEN keyakinan = 'h' THEN 1 ELSE 0 END) AS jumlah_keyakinan_tinggi,
                ROUND(AVG(frp), 2) AS rata_frp,
                MAX(frp) AS maks_frp,
                SUM(CASE WHEN siang_malam = 'D' THEN 1 ELSE 0 END) AS jumlah_siang,
                SUM(CASE WHEN siang_malam = 'N' THEN 1 ELSE 0 END) AS jumlah_malam
            FROM hotspot_harian
            WHERE tanggal_deteksi IN %s
            GROUP BY tanggal_deteksi
            ON DUPLICATE KEY UPDATE
                jumlah_titik = VALUES(jumlah_titik),
                jumlah_keyakinan_tinggi = VALUES(jumlah_keyakinan_tinggi),
                rata_frp = VALUES(rata_frp),
                maks_frp = VALUES(maks_frp),
                jumlah_siang = VALUES(jumlah_siang),
                jumlah_malam = VALUES(jumlah_malam)
            """,
            parameters=(tuple(tanggal_terpengaruh),),
        )
        return {"baris_dimuat": len(baris), "tanggal_terpengaruh": tanggal_terpengaruh}

    mentah = ekstrak_data()
    bersih = transformasi_data(mentah)
    buat_tabel_mysql >> mentah
    muat_ke_mysql(bersih)


etl_karhutla_sultra()