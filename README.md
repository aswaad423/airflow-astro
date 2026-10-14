# 🔥 Karhutla Sulawesi Tenggara — ETL Airflow + Dashboard Streamlit

Proyek data kebakaran hutan dan lahan (karhutla) Sulawesi Tenggara:
sumber titik panas **NASA FIRMS (VIIRS_SNPP_NRT)**, cuaca **Open-Meteo Archive**,
dan kualitas udara **Open-Meteo Air Quality**.

## Isi repo

| Path | Keterangan |
|---|---|
| `streamlit_app.py` | Dashboard Streamlit (8 tab: Tren, Peta, EDA, Korelasi, Model, Prediksi, ETL Live, Data) |
| `dags/etl_karhutla_sultra.py` | DAG ETL harian FIRMS → MySQL (`hotspot_harian`, `rekap_harian`) |
| `dags/karhutla_pipeline.py` | DAG EDA + visualisasi harian |
| `include/` | Skrip tahapan: gathering, assessing, cleaning, EDA, korelasi, plot, model ML |

## Cara menjalankan dashboard

```
py -m pip install streamlit pandas numpy matplotlib seaborn scikit-learn requests
py -m streamlit run streamlit_app.py
```

Buka http://localhost:8501. Tanpa database pun bisa (pilih sumber data **Demo**).
Untuk data asli: isi FIRMS MAP_KEY dari https://firms.modaps.eosdis.nasa.gov/api/area/
pada tab **ETL Live**, atau hubungkan MySQL pada sidebar.

## Cara menjalankan Airflow (opsional)

```
astro dev start
```

UI Airflow: http://localhost:8080/

## Catatan

File `.env` (berisi MAP_KEY rahasia) tidak ikut di-commit — lihat `.gitignore`.
