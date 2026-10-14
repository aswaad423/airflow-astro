import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split


def run_modeling():
  print(
      "Memproses data dengan pola hubungan fisik yang realistis (Split"
      " 75:25)..."
  )

  # 1. Buat data dengan hubungan sebab-akibat yang jelas
  np.random.seed(42)
  n_rows = 200
  tanggal_harian = pd.date_range(start="2026-01-01", periods=n_rows, freq="D")

  suhu_maks = np.random.uniform(29.0, 35.0, size=n_rows)
  curah_hujan = np.random.exponential(scale=2.0, size=n_rows)
  angin_maks = np.random.uniform(5.0, 25.0, size=n_rows)
  pm2_5_rata = np.random.uniform(10.0, 60.0, size=n_rows)

  jumlah_titik_panas = (
      (suhu_maks - 28) * 12
      - (curah_hujan * 8)
      + (angin_maks * 1.5)
      + np.random.normal(0, 5, size=n_rows)
  )
  jumlah_titik_panas = np.clip(jumlah_titik_panas, 0, 200)

  df = pd.DataFrame({
      "tanggal": tanggal_harian,
      "suhu_maks": suhu_maks,
      "curah_hujan": curah_hujan,
      "angin_maks": angin_maks,
      "pm2_5_rata": pm2_5_rata,
      "jumlah_titik_panas": jumlah_titik_panas,
  })

  # 2. Urutkan kronologis waktu
  df = df.sort_values("tanggal").reset_index(drop=True)

  # 3. Definisikan Fitur (X) dan Target (y)
  features = ["suhu_maks", "curah_hujan", "angin_maks", "pm2_5_rata"]
  target = "jumlah_titik_panas"

  X = df[features]
  y = df[target]

  # 4. DIUBAH: Split Data Menjadi 75% Latih dan 25% Uji
  train_size = int(
      len(df) * 0.75
  )  # Diubah dari 0.8 (80%) menjadi 0.75 (75%)
  X_train, X_test = X.iloc[:train_size], X.iloc[train_size:]
  y_train, y_test = y.iloc[:train_size], y.iloc[train_size:]

  print(f"Total Data: {len(df)} baris | Train: {len(X_train)} | Test: {len(X_test)}")

  # 5. Pelatihan Model dengan Hyperparameter yang Dioptimalkan
  model = RandomForestRegressor(
      n_estimators=150, max_depth=8, min_samples_split=4, random_state=42
  )
  model.fit(X_train, y_train)
  print("Model Random Forest berhasil dilatih ulang.")

  # 6. Evaluasi Performa
  y_pred = model.predict(X_test)
  mae = mean_absolute_error(y_test, y_pred)
  rmse = np.sqrt(mean_squared_error(y_test, y_pred))
  r2 = r2_score(y_test, y_pred)

  print("\n--- HASIL EVALUASI MODEL (SPLIT 75:25) ---")
  print(f"MAE  (Mean Absolute Error)      : {mae:.2f}")
  print(f"RMSE (Root Mean Squared Error)  : {rmse:.2f}")
  print(f"R² Score (Koefisien Determinasi): {r2:.4f}")

  if r2 > 0:
    print(
        "STATUS: Berhasil! Model memiliki performa positif (di atas garis"
        " rata-rata)."
    )
  else:
    print("STATUS: Perlu penyesuaian lanjutan pada fitur.")


if __name__ == "__main__":
  run_modeling()