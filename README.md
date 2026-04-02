# 🚀 Smart City: Real-Time Transportation Analytics (P6)
[![Big Data](https://img.shields.io/badge/Tech-Big%20Data-blue.svg)](https://spark.apache.org/)
[![Spark](https://img.shields.io/badge/Processing-Apache%20Spark-orange.svg)](https://spark.apache.org/)
[![Streamlit](https://img.shields.io/badge/Dashboard-Streamlit-red.svg)](https://streamlit.io/)

Proyek ini adalah sistem pemantauan lalu lintas kota pintar (Smart City) berbasis **Big Data Streaming**. Sistem ini mensimulasikan data perjalanan kendaraan, memprosesnya secara real-time menggunakan Apache Spark, dan memvisualisasikannya ke dalam dashboard interaktif.

## 🌟 Fitur Utama (Update Praktikum 6)
- **Real-Time Data Streaming**: Menggunakan generator data JSON untuk mensimulasikan trafik kendaraan.
- **Spark Structured Streaming**: Memproses data masuk secara berkelanjutan dan menyimpannya dalam format **Parquet** (Data Lake).
- **Window Aggregation**: Mengoptimalkan visualisasi dengan agregasi data per menit untuk performa rendering yang ringan.
- **Anomaly Detection**: Mendeteksi tarif perjalanan (fare) yang tidak wajar (> 80.000).
- **Downsampling Strategy**: Dashboard hanya merender data terbaru untuk menjaga stabilitas memori.

## 🏗️ Arsitektur Sistem
1. **Data Source**: Python script (`trip_generator.py`) menghasilkan data JSON.
2. **Stream Processing**: Apache Spark (`streaming_trip_layer.py`) membaca JSON dan menulis ke Parquet.
3. **Storage**: Data tersimpan di folder `data/serving/transportation` dalam format Parquet.
4. **Analytics**: Modul `transportation_analytics.py` melakukan perhitungan metrik dan windowing.
5. **Visualization**: Streamlit Dashboard menampilkan grafik tren dan KPI secara real-time.

## 🛠️ Cara Menjalankan

### 1. Persiapan Environment
```bash
# Masuk ke folder project
cd ~/bigdata-project

# Aktifkan Virtual Environment
source venv/bin/activate

### 2. Jalankan Data Pipeline (Buka 3 Terminal)
Terminal 1: Trip Generator
RUN : python3 scripts/transportation/trip_generator.py
Terminal 2: Spark Streaming Processor
RUN : spark-submit scripts/transportation/streaming_trip_layer.py
Terminal 3: Streamlit Dashboard
RUN : streamlit run dashboard/dashboard_transportation.py

📊 Tampilan Dashboard
Dashboard akan menampilkan:

Metrics Card: Total Trips, Total Revenue, Top Location.
Traffic Trend: Grafik garis hasil Window Aggregation per menit.
Vehicle Distribution: Distribusi jenis kendaraan (Mobil, Motor, Bus, dll).
Anomaly Table: Daftar perjalanan dengan tarif abnormal.

📁 Struktur Folder
.
├── alerts/               # Modul notifikasi/alert
├── analytics/            # Logika perhitungan (Windowing & Metrics)
├── dashboard/            # Kode UI Streamlit
├── data/                 # Data Lake (Parquet Files)
├── scripts/              # Script Generator & Spark Streaming
└── venv/                 # Virtual Environment
