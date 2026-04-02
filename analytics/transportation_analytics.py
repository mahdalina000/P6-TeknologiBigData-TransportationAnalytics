import pandas as pd
import os

# ==========================================
# LOAD DATA
# ==========================================
def load_data(path):
    if not os.path.exists(path):
        return pd.DataFrame()
    
    # Mencari file parquet di dalam folder data lake
    files = [f for f in os.listdir(path) if f.endswith(".parquet")]
    if not files:
        return pd.DataFrame()
    
    # Menggabungkan semua part-file parquet menjadi satu dataframe
    df = pd.concat(
        [pd.read_parquet(os.path.join(path, f)) for f in files],
        ignore_index=True
    )
    return df

# ==========================================
# PREPROCESS
# ==========================================
def preprocess(df):
    if df.empty:
        return df
    
    # Mengubah string timestamp menjadi objek datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    
    # Menghapus data yang timestamp-nya tidak valid (NaT)
    df = df.dropna(subset=["timestamp"])
    return df

# ==========================================
# METRICS
# ==========================================
def compute_metrics(df):
    if df.empty:
        return {
            "total_trips": 0,
            "total_fare": 0,
            "top_location": "-"
        }
    
    return {
        "total_trips": len(df),
        "total_fare": df["fare"].sum(),
        "top_location": df.groupby("location")["fare"].sum().idxmax()
    }

# ==========================================
# ANALYSIS FUNCTIONS
# ==========================================
def detect_peak_hour(df):
    if df.empty:
        return None
    
    df["hour"] = df["timestamp"].dt.hour
    return df.groupby("hour").size().idxmax()

def detect_anomaly(df):
    """Contoh: Tarif (fare) di atas 80.000 dianggap anomali"""
    if df.empty:
        return pd.DataFrame()
    
    return df[df["fare"] > 80000]

# ==========================================
# VISUALIZATION DATA
# ==========================================
def fare_per_location(df):
    if df.empty:
        return pd.Series(dtype='float64')
    
    return df.groupby("location")["fare"].sum().sort_values(ascending=False)

def vehicle_distribution(df):
    if df.empty:
        return pd.Series(dtype='int64')
    
    return df.groupby("vehicle_type").size().sort_values(ascending=False)

def mobility_trend(df):
    if df.empty:
        return pd.Series(dtype='float64')
    
    # Resample per 10 detik untuk melihat pergerakan cepat
    df_trend = df.set_index("timestamp")
    return df_trend["fare"].resample("10s").sum()

# ==========================================================
# NEW (PRAKTIKUM 6) - WINDOW AGGREGATION
# ==========================================================
def traffic_per_window(df):
    """
    Agregasi jumlah trip per menit (windowing).
    Digunakan untuk visualisasi skala besar (efficient rendering) agar dashboard ringan.
    """
    if df.empty:
        return None
    
    # Pastikan kolom timestamp dalam format datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    
    # Mengubah data mentah menjadi agregasi per menit
    return df.set_index("timestamp") \
             .resample("1min") \
             .size()