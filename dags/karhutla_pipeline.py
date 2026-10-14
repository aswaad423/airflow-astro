from datetime import datetime, timedelta
import os
import sys
from airflow import DAG

# Menggunakan standard provider untuk PythonOperator sesuai peringatan Airflow 3
from airflow.providers.standard.operators.python import PythonOperator

# Menambahkan direktori 'include' ke path python agar modul dapat dibaca oleh Airflow
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../include"))
)

# Konfigurasi argumen default untuk DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Fungsi untuk menjalankan skrip EDA
def execute_eda():
    try:
        print("Memulai eksekusi analisis deskriptif (EDA)...")
        import eda_karhutla

        print("EDA dan pengecekan kelengkapan data selesai.")
    except Exception as e:
        print(f"ERROR pada EDA: {str(e)}")
        raise e


# Fungsi untuk menjalankan skrip pembuatan grafik
def execute_plotting():
    try:
        print("Memulai pembuatan grafik visualisasi...")
        import plot_karhutla

        print("Semua grafik berhasil digenerate dan disimpan ke folder output.")
    except Exception as e:
        print(f"ERROR pada Plotting: {str(e)}")
        raise e


# Mendefinisikan DAG
with DAG(
    dag_id="karhutla_eda_visualization_pipeline",
    default_args=default_args,
    description="Pipeline otomatis untuk EDA dan Visualisasi Karhutla Harian",
    schedule="@daily",
    start_date=datetime(2026, 9, 28),
    catchup=False,
    tags=["karhutla", "eda", "visualization"],
) as dag:

    # Task 1: Menjalankan Analisis EDA
    task_run_eda = PythonOperator(
        task_id="run_eda_analysis",
        python_callable=execute_eda,
    )

    # Task 2: Menjalankan Pembuatan Grafik Visualisasi
    task_generate_plots = PythonOperator(
        task_id="generate_visualization_plots",
        python_callable=execute_plotting,
    )

    # Mengatur Alur Urutan Task
    task_run_eda >> task_generate_plots