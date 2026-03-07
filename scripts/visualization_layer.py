# ==========================================================
# VISUALIZATION LAYER
# Big Data Dashboard
# ==========================================================

from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import os

print("========================================")
print("       VISUALIZATION LAYER STARTED      ")
print("========================================")

# =========================
# INITIALIZE SPARK
# =========================
spark = SparkSession.builder \
    .appName("VisualizationLayer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# =========================
# LOAD CLEAN DATA
# =========================
print("Loading Clean Parquet Data...")

df = spark.read.parquet("data/clean/parquet/")

print("Total Records:", df.count())
print("----------------------------------------")

# =========================
# CREATE REPORT FOLDER
# =========================
if not os.path.exists("reports"):
    os.makedirs("reports")

# =========================
# CATEGORY REVENUE
# =========================
print("Generating Category Revenue Chart...")

df = df.withColumn(
    "total_amount",
    df.price * df.quantity
)

category_df = df.groupBy("category") \
    .sum("total_amount") \
    .toPandas()

category_df = category_df.sort_values(
    "sum(total_amount)",
    ascending=False
)

plt.figure(figsize=(8,5))
plt.bar(
    category_df["category"],
    category_df["sum(total_amount)"]
)

plt.xticks(rotation=45)

plt.title("Revenue per Category")
plt.ylabel("Total Revenue")

plt.tight_layout()

plt.savefig("reports/category_revenue.png")

print("Visualization saved to reports/category_revenue.png")

# =========================
# STOP SPARK
# =========================
spark.stop()

print("========================================")
print("       VISUALIZATION COMPLETED          ")
print("========================================")