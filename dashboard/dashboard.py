import streamlit as st
import pandas as pd
import os
import time
import plotly.express as px
import plotly.graph_objects as go

# ==========================================================
# 1. KONFIGURASI HALAMAN & STYLE
# ==========================================================
st.set_page_config(
    page_title="Real-Time E-Commerce Analytics",
    page_icon="💰",
    layout="wide"
)

# Menghilangkan padding berlebih dan mempercantik tampilan
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stMetric {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 12px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        border: 1px solid #e1e4e8;
    }
    </style>
    """, unsafe_allow_html=True)

DATA_PATH = "data/serving/stream"

# ==========================================================
# 2. FUNGSI LOAD DATA (ANTI-ERROR)
# ==========================================================
def load_stream_data():
    if not os.path.exists(DATA_PATH):
        return pd.DataFrame()
    
    # Ambil file .parquet yang sudah selesai ditulis (> 0 bytes)
    files = [f for f in os.listdir(DATA_PATH) if f.endswith(".parquet")]
    
    df_list = []
    for f in files:
        f_path = os.path.join(DATA_PATH, f)
        try:
            if os.path.getsize(f_path) > 0:
                temp_df = pd.read_parquet(f_path)
                df_list.append(temp_df)
        except:
            continue # Skip file jika sedang dikunci sistem
            
    if not df_list:
        return pd.DataFrame()
    
    return pd.concat(df_list, ignore_index=True)

# ==========================================================
# 3. SIDEBAR NAVIGATION & FILTER
# ==========================================================
st.sidebar.image("https://cdn-icons-png.flaticon.com/512/3081/3081559.png", width=80)
st.sidebar.title("Control Panel")
st.sidebar.markdown("---")

# Slider interaktif untuk kecepatan refresh
refresh_rate = st.sidebar.slider("Kecepatan Update (detik)", 2, 10, 5)

st.sidebar.markdown("### 🔍 Filter Wilayah")
# Tempat menaruh placeholder filter agar bisa update dinamis
city_filter_placeholder = st.sidebar.empty()

st.sidebar.markdown("---")
st.sidebar.info("""
**Sistem Status:**
- Generator: ✅ Running
- Spark: ✅ Streaming
- Database: ✅ Parquet
""")

# ==========================================================
# 4. MAIN DASHBOARD LOOP
# ==========================================================
placeholder = st.empty()

while True:
    with placeholder.container():
        df_raw = load_stream_data()

        if df_raw.empty:
            st.warning("🔄 Menghubungkan ke Spark Streaming... Pastikan data generator sudah berjalan.")
            time.sleep(refresh_rate)
            continue

        # --- PREPROCESSING ---
        df_raw["timestamp"] = pd.to_datetime(df_raw["timestamp"])
        
        # --- UPDATE FILTER DI SIDEBAR ---
        cities = ["Semua Kota"] + sorted(df_raw["city"].unique().tolist())
        selected_city = city_filter_placeholder.selectbox("Pilih Kota:", cities)

        # Apply Filter
        if selected_city != "Semua Kota":
            df = df_raw[df_raw["city"] == selected_city].copy()
        else:
            df = df_raw.copy()

        # --- HEADER SECTION ---
        col_title, col_time = st.columns([3, 1])
        with col_title:
            st.title("🚀 Real-Time Business Intelligence")
            st.caption(f"Data Source: `{DATA_PATH}` | Mode: Streaming")
        with col_time:
            st.markdown(f"### 🕒 {time.strftime('%H:%M:%S')}")

        # --- KPI METRICS ---
        m1, m2, m3, m4 = st.columns(4)
        total_rev = df["price"].sum()
        avg_val = df["price"].mean()
        
        m1.metric("📦 Total Transaksi", f"{len(df):,}")
        m2.metric("💰 Total Revenue", f"Rp {int(total_rev):,}")
        m3.metric("📈 Avg. Transaction", f"Rp {int(avg_val):,}")
        m4.metric("🏙️ Kota Tercover", len(df["city"].unique()))

        st.markdown("---")

        # --- VISUALIZATION SECTION ---
        row1_col1, row1_col2 = st.columns(2)

        with row1_col1:
            st.subheader("📊 Revenue per Product")
            prod_revenue = df.groupby("product")["price"].sum().sort_values(ascending=True).reset_index()
            fig_bar = px.bar(prod_revenue, x="price", y="product", orientation='h',
                             color="price", color_continuous_scale='Blues',
                             labels={'price':'Total Revenue (Rp)', 'product':'Nama Produk'})
            fig_bar.update_layout(margin=dict(l=20, r=20, t=30, b=20), height=350)
            st.plotly_chart(fig_bar, use_container_width=True)

        with row1_col2:
            st.subheader("📍 Kontribusi Penjualan Kota")
            city_revenue = df.groupby("city")["price"].sum().reset_index()
            fig_pie = px.pie(city_revenue, values="price", names="city", hole=0.4,
                             color_discrete_sequence=px.colors.qualitative.Pastel)
            fig_pie.update_layout(margin=dict(l=20, r=20, t=30, b=20), height=350)
            st.plotly_chart(fig_pie, use_container_width=True)

        # --- TREND SECTION ---
        st.subheader("📈 Real-Time Revenue Trend (Per 10 Detik)")
        df_trend = df.set_index("timestamp").resample("10S")["price"].sum().reset_index()
        fig_line = px.line(df_trend, x="timestamp", y="price", render_mode="svg")
        fig_line.update_traces(line_color='#2ecc71', line_width=3, fill='tozeroy')
        fig_line.update_layout(xaxis_title="Waktu Transaksi", yaxis_title="Revenue (Rp)", height=300)
        st.plotly_chart(fig_line, use_container_width=True)

        # --- DATA TABLE ---
        with st.expander("📄 Lihat 20 Transaksi Terbaru"):
            st.dataframe(df.sort_values("timestamp", ascending=False).head(20), use_container_width=True)

    time.sleep(refresh_rate)