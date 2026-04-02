import streamlit as st
import pandas as pd
import time
import sys
import os

# ==========================================
# FIX MODULE PATH (WAJIB)
# ==========================================
# Memastikan folder utama terdeteksi agar bisa import dari folder lain
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# IMPORT MODULE CUSTOM
try:
    from analytics import transportation_analytics as ta
    from alerts import transportation_alert as alert
except ImportError as e:
    st.error(f"Gagal mengimport modul: {e}. Pastikan struktur folder benar.")

# ==========================================
# CONFIGURATION
# ==========================================
DATA_PATH = "data/serving/transportation"
REFRESH_INTERVAL = 5  # Detik

st.set_page_config(
    page_title="Smart Transportation Dashboard", 
    layout="wide",
    page_icon="🚀"
)

st.title("🚀 Smart Transportation Real-Time Analytics")
st.markdown("---")

# Placeholder untuk dashboard yang dinamis (Auto-refresh container)
placeholder = st.empty()

# ==========================================
# MAIN LOOP (REAL-TIME UPDATE)
# ==========================================
while True:
    with placeholder.container():
        # 1. LOAD DATA LAKE
        try:
            df_raw = ta.load_data(DATA_PATH)
        except Exception as e:
            st.error(f"Error loading data: {e}")
            df_raw = pd.DataFrame()

        if df_raw.empty:
            st.warning("🔄 Waiting for streaming transportation data from Spark...")
            time.sleep(REFRESH_INTERVAL)
            continue
        
        # 2. PREPROCESS DATA
        df = ta.preprocess(df_raw)

        # --- OPTIMASI PENGAMBILAN DATA (DOWNSAMPLING) ---
        # Ambil subset 1000 data terbaru agar rendering grafik tetap ringan
        df_sample = df.tail(1000)
        
        # 3. METRICS SECTION
        st.subheader("📊 Key Performance Indicators")
        try:
            metrics = ta.compute_metrics(df)
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Trips", f"{metrics['total_trips']:,}")
            m2.metric("Total Fare", f"Rp {int(metrics['total_fare']):,}")
            m3.metric("Top Location", metrics["top_location"])
        except Exception as e:
            st.error(f"Error computing metrics: {e}")
            
        st.divider()
        
        # 4. TRAFFIC ALERTS & INSIGHTS
        col_info1, col_info2 = st.columns([1, 2])
        
        with col_info1:
            st.subheader("💡 Operational Insight")
            try:
                peak_hour = ta.detect_peak_hour(df)
                st.info(f"**Peak Traffic Hour:** {peak_hour}:00")
            except Exception:
                st.warning("Menghitung peak hour...")
        
        with col_info2:
            st.subheader("⚠️ Traffic Alerts")
            try:
                alerts = alert.generate_alert(df)
                if alerts:
                    for a in alerts:
                        st.error(a)
                else:
                    st.success("Normal traffic conditions detected.")
            except Exception as e:
                st.warning(f"Alert system delay: {e}")
            
        st.divider()
        
        # 5. VISUALISASI SKALA BESAR (WINDOW AGGREGATION)
        try:
            # A. Real-Time Traffic Trend
            st.subheader("📈 Real-Time Traffic (Window Aggregation per Minute)")
            traffic_window = ta.traffic_per_window(df)
            if traffic_window is not None:
                st.line_chart(traffic_window)
            
            st.write("") # Spacer

            col_chart1, col_chart2 = st.columns(2)
            with col_chart1:
                # B. Traffic Density (Bar Chart)
                st.subheader("📍 Traffic Density per Location")
                st.bar_chart(ta.fare_per_location(df_sample))
            
            with col_chart2:
                # C. Vehicle Distribution (Bar Chart)
                st.subheader("🚗 Vehicle Type Distribution")
                st.bar_chart(ta.vehicle_distribution(df_sample))
            
            # D. Mobility Trend
            st.subheader("📉 Mobility Trend (Fare Analysis)")
            st.line_chart(df_sample['fare'])

        except Exception as e:
            st.warning(f"Visualization error: {e}")
            
        st.divider()
        
        # 6. ANOMALY DETECTION & LIVE DATA
        col_bottom1, col_bottom2 = st.columns(2)
        
        with col_bottom1:
            st.subheader("🔍 Abnormal Trips Detection")
            try:
                anomaly_df = ta.detect_anomaly(df)
                if not anomaly_df.empty:
                    st.dataframe(anomaly_df.tail(10), use_container_width=True)
                else:
                    st.success("No anomalies in recent trips.")
            except Exception as e:
                st.warning(f"Anomaly analysis error: {e}")
        
        with col_bottom2:
            st.subheader("📋 Live Trip Data (Latest 50)")
            # Menampilkan limited table agar tidak crash
            st.dataframe(df.tail(50), use_container_width=True)
        
        # Footer
        st.caption(f"Last updated: {time.strftime('%H:%M:%S')} | Refresh interval: {REFRESH_INTERVAL}s")
        
        # Jeda refresh loop
        time.sleep(REFRESH_INTERVAL)