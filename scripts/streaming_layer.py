from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# 1. Inisialisasi Spark
spark = SparkSession.builder \
    .appName("EcommerceStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Schema harus pas dengan Generator (user_id, product, price, city, timestamp)
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# 3. Baca Stream
df = spark.readStream \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .json("stream_data")

# 4. Transformasi (Ubah string ke Timestamp)
df_clean = df.withColumn("timestamp", to_timestamp(col("timestamp")))

# 5. TULIS KE PARQUET (Agar dibaca Dashboard)
# PASTIKAN PATH INI SESUAI DENGAN YANG DIBACA DASHBOARD
query = df_clean.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "data/serving/stream") \
    .option("checkpointLocation", "logs/stream_checkpoint") \
    .trigger(processingTime="5 seconds") \
    .start()

print("🚀 Spark sedang memproses data... Cek dashboard dalam 10 detik!")
query.awaitTermination()