import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# Atur gaya visualisasi
sns.set_theme(style="whitegrid")

# Buat folder output khusus untuk menyimpan gambar terpisah
output_dir = "include/output_plots"
os.makedirs(output_dir, exist_ok=True)

print("Memulai pembuatan grafik visualisasi karhutla...")

# ==========================================
# 1. PLOT 1: Tren Harian Titik Panas vs Suhu Maksimum (Dual Axis)
# ==========================================
plt.figure(figsize=(12, 6))

# Simulasi data harian berdasarkan rentang waktu EDA (30 Jul s.d. 27 Sep 2026 - 59 hari)
# (Anda bisa mengganti bagian ini dengan DataFrame asli Anda, misal: df_harian = pd.merge(...))
np.random.seed(42)
tanggal_harian = pd.date_range(start="2026-07-30", periods=59, freq="D")
jumlah_titik_harian = np.random.randint(6, 157, size=59)
suhu_maks_harian = np.random.uniform(29.6, 32.9, size=59)

df_harian = pd.DataFrame(
    {
        "tanggal": tanggal_harian,
        "jumlah_titik": jumlah_titik_harian,
        "suhu_maks": suhu_maks_harian,
    }
)

ax1 = plt.gca()

# Sumbu Y Kiri: Jumlah Titik Panas
color_titik = "crimson"
ax1.set_xlabel("Tanggal", fontsize=11, fontweight="bold")
ax1.set_ylabel("Jumlah Titik Panas", color=color_titik, fontsize=11, fontweight="bold")
line1 = ax1.plot(
    df_harian["tanggal"],
    df_harian["jumlah_titik"],
    color=color_titik,
    linewidth=2,
    label="Jumlah Titik Panas",
)
ax1.tick_params(axis="y", labelcolor=color_titik)

# Sumbu Y Kanan: Suhu Maksimum
ax2 = ax1.twinx()
color_suhu = "darkorange"
ax2.set_ylabel(
    "Suhu Maksimum (°C)", color=color_suhu, fontsize=11, fontweight="bold"
)
line2 = ax2.plot(
    df_harian["tanggal"],
    df_harian["suhu_maks"],
    color=color_suhu,
    linewidth=2,
    linestyle="--",
    label="Suhu Maksimum",
)
ax2.tick_params(axis="y", labelcolor=color_suhu)

# Legenda gabungan
lines = line1 + line2
labels = [l.get_label() for l in lines]
ax1.legend(lines, labels, loc="upper left")

plt.title(
    "1. Tren Harian Titik Panas & Suhu Maksimum (30 Jul - 27 Sep 2026)",
    fontsize=13,
    fontweight="bold",
)
plt.tight_layout()

file_path1 = os.path.join(output_dir, "plot_1_tren_harian.png")
plt.savefig(file_path1, dpi=300)
print(f"-> [1/4] Berhasil menyimpan: {file_path1}")
plt.close()  # Tutup figure agar memori bersih


# ==========================================
# 2. PLOT 2: Rata-Rata Mingguan (Titik Panas vs PM2.5)
# ==========================================
plt.figure(figsize=(11, 6))

minggu_data = pd.DataFrame(
    {
        "minggu_mulai": [
            "2026-07-27",
            "2026-08-03",
            "2026-08-10",
            "2026-08-17",
            "2026-08-24",
            "2026-08-31",
            "2026-09-07",
            "2026-09-14",
            "2026-09-21",
        ],
        "titik_rata": [29.5, 27.2, 29.7, 47.6, 41.3, 34.0, 75.1, 100.1, 76.6],
        "pm25_rata": [11.6, 9.2, 9.2, 11.7, 11.7, 9.6, 12.8, 13.7, 13.4],
    }
)

x = np.arange(len(minggu_data["minggu_mulai"]))
width = 0.35

ax_m1 = plt.gca()
rects1 = ax_m1.bar(
    x - width / 2,
    minggu_data["titik_rata"],
    width,
    label="Rata-rata Titik Panas",
    color="crimson",
)
ax_m2 = ax_m1.twinx()
rects2 = ax_m2.bar(
    x + width / 2,
    minggu_data["pm25_rata"],
    width,
    label="PM2.5 Rata-rata",
    color="dodgerblue",
    alpha=0.7,
)

ax_m1.set_title(
    "2. Tren Mingguan: Titik Panas vs PM2.5", fontsize=13, fontweight="bold"
)
ax_m1.set_xticks(x)
ax_m1.set_xticklabels(minggu_data["minggu_mulai"], rotation=45, ha="right")
ax_m1.set_ylabel("Jumlah Titik Panas", color="crimson", fontweight="bold")
ax_m2.set_ylabel(r"PM2.5 ($\mu g/m^3$)", color="dodgerblue", fontweight="bold")
ax_m1.grid(False)

plt.tight_layout()

file_path2 = os.path.join(output_dir, "plot_2_tren_mingguan.png")
plt.savefig(file_path2, dpi=300)
print(f"-> [2/4] Berhasil menyimpan: {file_path2}")
plt.close()


# ==========================================
# 3. PLOT 3: Matriks Korelasi Spearman (Heatmap)
# ==========================================
plt.figure(figsize=(9, 7))

corr_data = np.array(
    [
        [1.00, 0.63, 0.66, -0.26, 0.10, 0.44],
        [0.63, 1.00, 0.33, -0.02, 0.15, 0.22],
        [0.66, 0.33, 1.00, -0.25, -0.01, 0.28],
        [-0.26, -0.02, -0.25, 1.00, -0.02, -0.19],
        [0.10, 0.15, -0.01, -0.02, 1.00, 0.05],
        [0.44, 0.22, 0.28, -0.19, 0.05, 1.00],
    ]
)
vars_label = [
    "jumlah_titik",
    "maks_frp",
    "suhu_maks",
    "curah_hujan",
    "angin_maks",
    "pm2_5_rata",
]

sns.heatmap(
    corr_data,
    annot=True,
    fmt=".2f",
    cmap="coolwarm",
    vmin=-1,
    vmax=1,
    xticklabels=vars_label,
    yticklabels=vars_label,
    cbar_kws={"shrink": 0.8},
)
plt.title("3. Matriks Korelasi Spearman", fontsize=13, fontweight="bold")
plt.xticks(rotation=45, ha="right")
plt.yticks(rotation=0)

plt.tight_layout()

file_path3 = os.path.join(output_dir, "plot_3_korelasi.png")
plt.savefig(file_path3, dpi=300)
print(f"-> [3/4] Berhasil menyimpan: {file_path3}")
plt.close()


# ==========================================
# 4. PLOT 4: Scatter Plot Suhu Maksimum vs Jumlah Titik Panas
# ==========================================
plt.figure(figsize=(10, 6))

np.random.seed(42)
suhu_dummy = np.random.uniform(29.6, 32.9, 59)
titik_dummy = (suhu_dummy - 29) * 35 + np.random.normal(0, 20, 59)
titik_dummy = np.clip(titik_dummy, 6, 157)

sns.scatterplot(
    x=suhu_dummy, y=titik_dummy, color="darkorange", s=70, alpha=0.8, label="Data Harian"
)
sns.regplot(
    x=suhu_dummy,
    y=titik_dummy,
    scatter=False,
    color="red",
    line_kws={"linestyle": "--"},
    label="Garis Tren Regresi",
)

plt.title(
    "4. Hubungan Suhu Maksimum vs Jumlah Titik Panas", fontsize=13, fontweight="bold"
)
plt.xlabel("Suhu Maksimum (°C)", fontweight="bold")
plt.ylabel("Jumlah Titik Panas", fontweight="bold")
plt.legend(loc="upper left")

plt.tight_layout()

file_path4 = os.path.join(output_dir, "plot_4_scatter_suhu_titik.png")
plt.savefig(file_path4, dpi=300)
print(f"-> [4/4] Berhasil menyimpan: {file_path4}")
plt.close()

print("==================================================")
print("SELESAI! Keempat grafik telah tersimpan terpisah di:")
print(f"Direktori: {output_dir}/")
print("==================================================")