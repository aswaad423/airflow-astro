"""Dashboard Streamlit Karhutla Sulawesi Tenggara — versi interaktif.

Mencakup seluruh alur kodingan Airflow/include:
- ETL FIRMS + cuaca + udara (mode demo / MySQL / upload CSV + tab ETL Live)
- EDA (statistik deskriptif, tren mingguan, hitungan kategori)
- Visualisasi interaktif (tren, peta hotspot, sebaran, korelasi)
- Modeling RandomForest (ala include/model_karhutla.py) + simulasi prediksi

Jalankan:
    py -m pip install streamlit
    py -m streamlit run streamlit_app.py
"""
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import streamlit as st
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

sns.set_theme(style="whitegrid")

st.set_page_config(
    page_title="Karhutla Sultra — Dashboard",
    page_icon="🔥",
    layout="wide",
)

# ---------- Konstanta (sama seperti DAG) ----------
FIRMS_SOURCE = "VIIRS_SNPP_NRT"
FIRMS_AREA = "120.5,-6.2,124.0,-3.0"  # barat, selatan, timur, utara (Sultra)
LON_MIN, LAT_MIN, LON_MAX, LAT_MAX = 120.5, -6.2, 124.0, -3.0
FITUR = ["suhu_maks", "curah_hujan", "angin_maks", "pm2_5_rata"]
TARGET = "jumlah_titik"

# ---------- CSS kustom ----------
st.markdown(
    """
    <style>
    .hero {
        background: linear-gradient(120deg, #7f1d1d 0%, #b91c1c 45%, #ea580c 100%);
        border-radius: 16px; padding: 26px 30px; color: white; margin-bottom: 18px;
    }
    .hero h1 { margin: 0; font-size: 2rem; }
    .hero p { margin: 6px 0 0 0; opacity: .92; }
    .risk-card { border-radius: 12px; padding: 14px 18px; margin-bottom: 14px;
                 font-weight: 600; }
    .risk-Tinggi { background: #fee2e2; border-left: 6px solid #dc2626; color: #7f1d1d; }
    .risk-Sedang { background: #fef3c7; border-left: 6px solid #d97706; color: #78350f; }
    .risk-Rendah { background: #dcfce7; border-left: 6px solid #16a34a; color: #14532d; }
    [data-testid="stMetric"] {
        background: rgba(127,127,127,0.08);
        border: 1px solid rgba(127,127,127,0.25);
        border-radius: 12px; padding: 10px 14px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------- Data demo (pola mirip output EDA asli) ----------
@st.cache_data
def buat_data_demo(n_hari: int = 120, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    tanggal = pd.date_range(end=pd.Timestamp.today().normalize(), periods=n_hari, freq="D")
    suhu_maks = rng.uniform(29.6, 32.9, n_hari).round(2)
    curah_hujan = rng.exponential(scale=2.0, size=n_hari).round(2)
    angin_maks = rng.uniform(5.0, 25.0, n_hari).round(2)
    pm2_5_rata = rng.uniform(8.0, 18.0, n_hari).round(2)
    pm10_rata = (pm2_5_rata * 1.6 + rng.normal(0, 1.5, n_hari)).clip(0).round(2)
    jumlah_titik = (
        (suhu_maks - 28) * 12
        - (curah_hujan * 8)
        + (angin_maks * 1.5)
        + rng.normal(0, 8, n_hari)
    )
    jumlah_titik = np.clip(jumlah_titik, 0, 200).round().astype(int)
    maks_frp = (jumlah_titik * 0.4 + rng.normal(5, 3, n_hari)).clip(0).round(2)
    rata_frp = (maks_frp * rng.uniform(0.4, 0.7, n_hari)).round(2)
    keyakinan_tinggi = (jumlah_titik * rng.uniform(0.2, 0.6, n_hari)).round().astype(int)
    return pd.DataFrame(
        {
            "tanggal": tanggal,
            "jumlah_titik": jumlah_titik,
            "maks_frp": maks_frp,
            "rata_frp": rata_frp,
            "keyakinan_tinggi": keyakinan_tinggi,
            "suhu_maks": suhu_maks,
            "curah_hujan": curah_hujan,
            "angin_maks": angin_maks,
            "pm2_5_rata": pm2_5_rata,
            "pm10_rata": pm10_rata,
        }
    )


@st.cache_data
def buat_titik_peta(df: pd.DataFrame, seed: int = 7) -> pd.DataFrame:
    """Titik hotspot sintetis dalam bbox FIRMS untuk peta (maks 4000 titik)."""
    rng = np.random.default_rng(seed)
    frames = []
    for _, r in df.iterrows():
        n = min(int(r["jumlah_titik"]), 60)
        if n <= 0:
            continue
        frames.append(
            pd.DataFrame(
                {
                    "tanggal": [r["tanggal"]] * n,
                    "lat": rng.uniform(LAT_MIN, LAT_MAX, n).round(4),
                    "lon": rng.uniform(LON_MIN, LON_MAX, n).round(4),
                    "frp": np.abs(rng.normal(float(r["rata_frp"]) or 8, 6, n)).round(2),
                }
            )
        )
    if not frames:
        return pd.DataFrame(columns=["tanggal", "lat", "lon", "frp"])
    pts = pd.concat(frames, ignore_index=True)
    return pts.sample(min(len(pts), 4000), random_state=seed).reset_index(drop=True)


def muat_dari_mysql(host, user, password, db):
    try:
        import MySQLdb
    except ImportError:
        return None, "Paket mysqlclient belum terinstal."
    try:
        conn = MySQLdb.connect(host=host, user=user, passwd=password, db=db)
        q = """
            SELECT c.tanggal, r.jumlah_titik, r.maks_frp, r.rata_frp,
                   r.jumlah_keyakinan_tinggi AS keyakinan_tinggi,
                   c.suhu_maks, c.curah_hujan, c.angin_maks,
                   u.pm2_5_rata, u.pm10_rata
            FROM cuaca_harian c
            LEFT JOIN rekap_harian r ON r.tanggal_deteksi = c.tanggal
            LEFT JOIN kualitas_udara_harian u ON u.tanggal = c.tanggal
            ORDER BY c.tanggal
        """
        df = pd.read_sql(q, conn)
        conn.close()
        df["tanggal"] = pd.to_datetime(df["tanggal"])
        return df.dropna(subset=["jumlah_titik"]).reset_index(drop=True), None
    except Exception as e:  # noqa: BLE001
        return None, str(e)


@st.cache_data
def latih_model(df: pd.DataFrame, test_size: float, n_est: int, max_depth: int):
    split = max(int(len(df) * (1 - test_size)), 1)
    X, y = df[FITUR], df[TARGET]
    X_train, X_test = X.iloc[:split], X.iloc[split:]
    y_train, y_test = y.iloc[:split], y.iloc[split:]
    model = RandomForestRegressor(
        n_estimators=n_est, max_depth=max_depth,
        min_samples_split=4, random_state=42,
    )
    model.fit(X_train, y_train)
    pred = model.predict(X_test)
    return model, {
        "mae": float(mean_absolute_error(y_test, pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_test, pred))),
        "r2": float(r2_score(y_test, pred)) if len(y_test) > 1 else float("nan"),
        "n_train": len(X_train),
        "n_test": len(X_test),
        "y_test": y_test.reset_index(drop=True),
        "y_pred": pd.Series(pred),
        "importance": dict(zip(FITUR, model.feature_importances_)),
    }


def tingkat_risiko(n: float) -> str:
    if n >= 70:
        return "Tinggi"
    if n >= 30:
        return "Sedang"
    return "Rendah"


# ---------- Sidebar ----------
with st.sidebar:
    st.title("🔥 Karhutla Sultra")
    st.caption("ETL → EDA → Model → Prediksi")
    sumber = st.radio("Sumber data", ["Demo (sintetis)", "Upload CSV", "MySQL"], index=0)
    df = None
    if sumber == "Demo (sintetis)":
        n_hari = st.slider("Jumlah hari", 30, 365, 120)
        df = buat_data_demo(n_hari)
    elif sumber == "Upload CSV":
        f = st.file_uploader("Upload CSV harian", type=["csv"])
        if f is not None:
            df = pd.read_csv(f, parse_dates=["tanggal"])
        else:
            st.info("Belum ada file — tampil data demo.")
            df = buat_data_demo(120)
    else:
        host = st.text_input("Host", "localhost")
        user = st.text_input("User", "root")
        password = st.text_input("Password", type="password")
        db = st.text_input("Database", "airflow_db")
        if st.button("Muat dari MySQL"):
            df, err = muat_dari_mysql(host, user, password, db)
            if err or df is None or df.empty:
                st.error(f"Gagal: {err}. Pakai data demo.")
                df = buat_data_demo(120)
            else:
                st.success(f"{len(df)} hari dimuat.")
        else:
            st.info("Klik 'Muat dari MySQL'. Sementara data demo.")
            df = buat_data_demo(120)

    st.divider()
    st.subheader("Filter tanggal")
    tgl_min, tgl_max = df["tanggal"].min().date(), df["tanggal"].max().date()
    rentang = st.date_input("Rentang", (tgl_min, tgl_max),
                            min_value=tgl_min, max_value=tgl_max)
    if isinstance(rentang, (list, tuple)) and len(rentang) == 2:
        df = df[(df["tanggal"].dt.date >= rentang[0])
                & (df["tanggal"].dt.date <= rentang[1])].reset_index(drop=True)

    st.divider()
    st.subheader("Parameter model")
    test_size = st.slider("Proporsi data uji", 0.1, 0.4, 0.25, 0.05)
    n_est = st.slider("n_estimators", 50, 300, 150, 10)
    max_depth = st.slider("max_depth", 4, 20, 8)

df["risiko"] = df["jumlah_titik"].apply(tingkat_risiko)

# ---------- Hero ----------
st.markdown(
    f"""
    <div class="hero">
      <h1>🔥 Dashboard Kebakaran Hutan & Lahan — Sulawesi Tenggara</h1>
      <p>Sumber FIRMS <b>{FIRMS_SOURCE}</b> · Area <b>{FIRMS_AREA}</b> ·
      {len(df)} hari · {df['tanggal'].min().date()} s.d. {df['tanggal'].max().date()}</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# ---------- Banner risiko hari terakhir ----------
terakhir = df.iloc[-1]
st.markdown(
    f"<div class='risk-card risk-{terakhir['risiko']}'>"
    f"Status hari terakhir ({terakhir['tanggal'].date()}): "
    f"{int(terakhir['jumlah_titik'])} titik panas — risiko <b>{terakhir['risiko']}</b> · "
    f"suhu {terakhir['suhu_maks']:.1f}°C · hujan {terakhir['curah_hujan']:.1f} mm · "
    f"PM2.5 {terakhir['pm2_5_rata']:.1f} µg/m³</div>",
    unsafe_allow_html=True,
)

# ---------- KPI ----------
if len(df) >= 14:
    rata_7 = df["jumlah_titik"].iloc[-7:].mean()
    rata_prev = df["jumlah_titik"].iloc[-14:-7].mean()
    delta_7 = f"{rata_7 - rata_prev:+.1f} vs 7 hari sebelumnya"
else:
    rata_7, delta_7 = df["jumlah_titik"].mean(), None

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total titik panas", f"{int(df['jumlah_titik'].sum()):,}".replace(",", "."))
k2.metric("Rata-rata / hari", f"{rata_7:.1f}", delta_7)
k3.metric("Puncak harian", int(df["jumlah_titik"].max()))
k4.metric("Hari risiko Tinggi", int((df["risiko"] == "Tinggi").sum()))
k5.metric("PM2.5 > 15 (WHO)", int((df["pm2_5_rata"] > 15).sum()))

tab_tren, tab_peta, tab_eda, tab_corr, tab_model, tab_prediksi, tab_etl, tab_data = st.tabs(
    ["📈 Tren", "🗺️ Peta", "🔍 EDA", "🔗 Korelasi", "🧪 Model",
     "🔮 Prediksi", "🛰️ ETL Live", "🗃️ Data"]
)

# ---------- Tren ----------
with tab_tren:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Titik panas harian")
        st.line_chart(df.set_index("tanggal")[["jumlah_titik"]], height=280)
    with c2:
        st.subheader("Suhu maks & PM2.5 harian")
        st.line_chart(df.set_index("tanggal")[["suhu_maks", "pm2_5_rata"]], height=280)

    st.subheader("Titik panas vs suhu maksimum (sumbu ganda)")
    fig, ax1 = plt.subplots(figsize=(11, 4.2))
    ax1.plot(df["tanggal"], df["jumlah_titik"], color="crimson", linewidth=2,
             label="Jumlah titik")
    ax1.set_ylabel("Jumlah titik", color="crimson", fontweight="bold")
    ax1.tick_params(axis="y", labelcolor="crimson")
    ax2 = ax1.twinx()
    ax2.plot(df["tanggal"], df["suhu_maks"], color="darkorange", linewidth=2,
             linestyle="--", label="Suhu maks")
    ax2.set_ylabel("Suhu maks (°C)", color="darkorange", fontweight="bold")
    ax2.tick_params(axis="y", labelcolor="darkorange")
    fig.tight_layout()
    st.pyplot(fig, use_container_width=True)

    st.subheader("Rata-rata mingguan: titik panas vs PM2.5")
    mingguan = df.set_index("tanggal").resample("W-MON").agg(
        {"jumlah_titik": "mean", "pm2_5_rata": "mean"}).dropna()
    mingguan.index = mingguan.index.strftime("%Y-%m-%d")
    st.bar_chart(mingguan, height=300)

    st.subheader("Komposisi hari berdasar tingkat risiko")
    komposisi = df["risiko"].value_counts().reindex(["Rendah", "Sedang", "Tinggi"]).dropna()
    c3, c4 = st.columns([1, 2])
    with c3:
        st.dataframe(komposisi.rename("jumlah hari"), use_container_width=True)
    with c4:
        fig2, ax = plt.subplots(figsize=(7, 3.6))
        ax.pie(komposisi.values, labels=komposisi.index, autopct="%1.0f%%",
               colors=["#16a34a", "#d97706", "#dc2626"], startangle=90)
        ax.set_title("Komposisi risiko harian")
        st.pyplot(fig2, use_container_width=True)

# ---------- Peta ----------
with tab_peta:
    st.subheader("Peta sebaran hotspot (dalam bbox FIRMS Sultra)")
    pts = buat_titik_peta(df)
    st.caption(f"{len(pts):,} titik ditampilkan · warna & ukuran = FRP (daya api)".replace(",", "."))
    if pts.empty:
        st.info("Tidak ada titik pada rentang ini.")
    else:
        try:
            import pydeck as pdk

            layer = pdk.Layer(
                "ScatterplotLayer", data=pts,
                get_position="[lon, lat]", get_radius="frp * 120",
                get_fill_color="[220, 60, 30, 160]",
                pickable=True, auto_highlight=True,
            )
            view = pdk.ViewState(latitude=float(pts["lat"].mean()),
                                 longitude=float(pts["lon"].mean()), zoom=6)
            st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view,
                                     tooltip={"text": "FRP: {frp}\nTgl: {tanggal}"}))
        except ImportError:
            st.map(pts.rename(columns={"lat": "lat", "lon": "lon"}))
    with st.expander("10 titik terpanas (FRP tertinggi)"):
        st.dataframe(pts.sort_values("frp", ascending=False).head(10),
                     use_container_width=True)

# ---------- EDA ----------
with tab_eda:
    st.subheader("Statistik deskriptif")
    st.dataframe(df[FITUR + [TARGET, "maks_frp", "rata_frp"]].describe().T
                 .style.background_gradient(cmap="YlOrRd"),
                 use_container_width=True)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Hari hujan (≥1 mm)", int((df["curah_hujan"] >= 1).sum()))
    c2.metric("Hari kering total (0 mm)", int((df["curah_hujan"] == 0).sum()))
    c3.metric("Hari PM2.5 > 15", int((df["pm2_5_rata"] > 15).sum()))
    c4.metric("Hari titik ≥ 100", int((df["jumlah_titik"] >= 100).sum()))

    c5, c6 = st.columns(2)
    with c5:
        st.subheader("Distribusi jumlah titik harian")
        fig3, ax3 = plt.subplots(figsize=(6, 3.8))
        sns.histplot(df["jumlah_titik"], bins=20, kde=True, color="crimson", ax=ax3)
        ax3.set_xlabel("Jumlah titik")
        st.pyplot(fig3, use_container_width=True)
    with c6:
        st.subheader("Suhu maksimum vs jumlah titik + tren regresi")
        fig4, ax4 = plt.subplots(figsize=(6, 3.8))
        sns.scatterplot(data=df, x="suhu_maks", y="jumlah_titik",
                        hue="risiko",
                        hue_order=["Rendah", "Sedang", "Tinggi"],
                        palette={"Rendah": "#16a34a", "Sedang": "#d97706",
                                 "Tinggi": "#dc2626"},
                        s=60, alpha=0.85, ax=ax4)
        sns.regplot(data=df, x="suhu_maks", y="jumlah_titik",
                    scatter=False, color="black",
                    line_kws={"linestyle": "--"}, ax=ax4)
        ax4.legend(title="Risiko")
        st.pyplot(fig4, use_container_width=True)

    st.subheader("Boxplot per tingkat risiko")
    fig5, ax5 = plt.subplots(figsize=(9, 3.6))
    sns.boxplot(data=df, x="risiko", y="jumlah_titik",
                order=["Rendah", "Sedang", "Tinggi"],
                palette={"Rendah": "#bbf7d0", "Sedang": "#fde68a", "Tinggi": "#fecaca"},
                ax=ax5)
    ax5.set_xlabel("Tingkat risiko")
    st.pyplot(fig5, use_container_width=True)

# ---------- Korelasi ----------
with tab_corr:
    st.subheader("Matriks korelasi Spearman")
    kolom_corr = ["jumlah_titik", "maks_frp", "suhu_maks",
                  "curah_hujan", "angin_maks", "pm2_5_rata"]
    corr = df[kolom_corr].corr(method="spearman")
    fig6, ax6 = plt.subplots(figsize=(7.5, 6))
    sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm",
                vmin=-1, vmax=1, square=True, ax=ax6)
    ax6.set_title("Spearman — tahan terhadap outlier")
    fig6.tight_layout()
    st.pyplot(fig6, use_container_width=True)
    st.dataframe(corr.style.background_gradient(cmap="coolwarm", vmin=-1, vmax=1),
                 use_container_width=True)
    st.info("Cara baca: +1 = naik bersama · −1 = berlawanan · 0 = tak ada hubungan linear. "
            "Spearman lebih tahan outlier dibanding Pearson.")

# ---------- Model ----------
with tab_model:
    st.subheader("Pelatihan RandomForest (split kronologis waktu)")
    model, hasil = latih_model(df, test_size, n_est, max_depth)
    m1, m2, m3 = st.columns(3)
    m1.metric("MAE", f"{hasil['mae']:.2f}")
    m2.metric("RMSE", f"{hasil['rmse']:.2f}")
    m3.metric("R²", f"{hasil['r2']:.4f}")
    st.caption(f"Train: {hasil['n_train']} hari · Test: {hasil['n_test']} hari "
               f"(fitur: {', '.join(FITUR)})")

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Pentingnya fitur")
        imp = pd.Series(hasil["importance"]).sort_values()
        fig7, ax7 = plt.subplots(figsize=(6, 3.4))
        imp.plot.barh(ax=ax7, color="seagreen")
        ax7.set_xlabel("Importance")
        fig7.tight_layout()
        st.pyplot(fig7, use_container_width=True)
    with c2:
        st.subheader("Aktual vs prediksi (data uji)")
        fig8, ax8 = plt.subplots(figsize=(6, 3.4))
        ax8.plot(hasil["y_test"].values, label="Aktual", color="crimson", linewidth=2)
        ax8.plot(hasil["y_pred"].values, label="Prediksi", color="dodgerblue",
                 linestyle="--", linewidth=2)
        ax8.legend()
        ax8.set_xlabel("Hari ke- (periode uji)")
        fig8.tight_layout()
        st.pyplot(fig8, use_container_width=True)

    st.subheader("🧪 Simulasi cepat (detail di menu 🔮 Prediksi)")
    st.info("Buka tab **🔮 Prediksi** di atas untuk simulasi lengkap + riwayat.")

# ---------- Prediksi (menu khusus) ----------
with tab_prediksi:
    st.subheader("🔮 Prediksi titik panas harian")
    st.caption(f"Model RandomForest (R² = {hasil['r2']:.3f}) dari tab 🧪 Model · "
               f"risiko: Rendah <30 · Sedang 30–69 · Tinggi ≥70")
    skenario = st.selectbox(
        "Skenario cepat",
        ["Kustom", "Kering ekstrem (panas + berangin)", "Hujan (basah)", "Normal"],
        index=0,
    )
    bawaan = {
        "Kustom": (31.5, 1.0, 12.0, 12.0),
        "Kering ekstrem (panas + berangin)": (33.5, 0.0, 22.0, 16.0),
        "Hujan (basah)": (29.8, 12.0, 8.0, 9.0),
        "Normal": (31.0, 2.0, 12.0, 11.0),
    }[skenario]
    p1, p2, p3, p4 = st.columns(4)
    s = p1.slider("Suhu maks (°C)", 25.0, 40.0, bawaan[0], 0.1, key="pred_suhu")
    h = p2.slider("Curah hujan (mm)", 0.0, 100.0, bawaan[1], 0.5, key="pred_hujan")
    a = p3.slider("Angin maks (km/jam)", 0.0, 60.0, bawaan[2], 0.5, key="pred_angin")
    p = p4.slider("PM2.5 rata-rata", 0.0, 200.0, bawaan[3], 0.5, key="pred_pm")
    if "riwayat_prediksi" not in st.session_state:
        st.session_state["riwayat_prediksi"] = []
    if st.button("Prediksi sekarang", type="primary", key="btn_prediksi"):
        pred = float(model.predict(pd.DataFrame([[s, h, a, p]], columns=FITUR))[0])
        risiko = tingkat_risiko(pred)
        saran = {
            "Tinggi": "Siagakan tim & patroli, siapkan sarana padam, "
                      "imbau larangan bakar lahan.",
            "Sedang": "Pantau hotspot harian dan peringatkan desa rawan.",
            "Rendah": "Kondisi relatif aman — lanjutkan pemantauan rutin.",
        }[risiko]
        st.markdown(
            f"<div class='risk-card risk-{risiko}'>Prediksi: <b>{pred:.1f} titik panas</b> "
            f"→ risiko <b>{risiko}</b><br>{saran}</div>",
            unsafe_allow_html=True,
        )
        st.session_state["riwayat_prediksi"].append(
            {"waktu": pd.Timestamp.now().strftime("%H:%M:%S"),
             "suhu": s, "hujan": h, "angin": a, "pm25": p,
             "prediksi": round(pred, 1), "risiko": risiko}
        )
    if st.session_state["riwayat_prediksi"]:
        st.subheader("Riwayat prediksi sesi ini")
        st.dataframe(pd.DataFrame(st.session_state["riwayat_prediksi"]),
                     use_container_width=True)
        if st.button("Hapus riwayat"):
            st.session_state["riwayat_prediksi"] = []
            st.rerun()

# ---------- ETL Live ----------
with tab_etl:
    st.subheader("🛰️ Tarik data FIRMS asli (logika DAG etl_karhutla_sultra)")
    st.caption("Ekstrak → filter keyakinan rendah (l) → tampilkan hasil. "
               "Butuh MAP_KEY dari https://firms.modaps.eosdis.nasa.gov/api/area/")
    map_key = st.text_input("FIRMS MAP_KEY", type="password")
    day_range = st.slider("Day range per request (maks API)", 1, 10, 3)
    if st.button("Tarik data live"):
        if not map_key.strip():
            st.warning("Isi MAP_KEY dulu.")
        else:
            import requests
            from io import StringIO

            url = (f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
                   f"{map_key.strip()}/{FIRMS_SOURCE}/{FIRMS_AREA}/{day_range}")
            try:
                with st.spinner("Menarik dari NASA FIRMS..."):
                    r = requests.get(url, timeout=30)
                    r.raise_for_status()
                    teks = r.text.strip()
                if teks.startswith(("Invalid", "Rentang", "Kesalahan")):
                    st.error(f"API menolak: {teks[:200]}")
                else:
                    import csv as csvmod

                    rows = list(csvmod.DictReader(StringIO(teks)))
                    df_live = pd.DataFrame(rows)
                    n_awal = len(df_live)
                    if "confidence" in df_live.columns:
                        df_live = df_live[df_live["confidence"] != "l"]
                    st.success(f"Berhasil: {n_awal} baris mentah → "
                               f"{len(df_live)} baris valid (buang keyakinan 'l').")
                    st.dataframe(df_live.head(20), use_container_width=True)
                    if {"latitude", "longitude"}.issubset(df_live.columns):
                        pts_live = pd.DataFrame({
                            "lat": pd.to_numeric(df_live["latitude"], errors="coerce"),
                            "lon": pd.to_numeric(df_live["longitude"], errors="coerce"),
                        }).dropna().head(1000)
                        if not pts_live.empty:
                            st.map(pts_live)
            except Exception as e:  # noqa: BLE001
                st.error(f"Gagal menarik: {e}")

# ---------- Data ----------
with tab_data:
    st.subheader("Data harian (sesudah filter)")
    st.dataframe(
        df.style.background_gradient(subset=["jumlah_titik"], cmap="YlOrRd"),
        use_container_width=True,
    )
    st.download_button("⬇️ Unduh CSV", df.to_csv(index=False),
                       file_name="karhutla_sultra.csv", mime="text/csv")

st.divider()
st.caption("Logika diadaptasi dari: dags/etl_karhutla_sultra.py · include/plot_karhutla.py · "
           "analisis_korelasi.py · model_karhutla.py · cleaning_data.py")
