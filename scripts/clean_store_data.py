from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Spark session başlat
spark = SparkSession.builder \
    .appName("Clean Store Transactions") \
    .getOrCreate()

# MinIO veya yerel dosyadan veriyi oku (CSV varsayalım)
df = spark.read.option("header", "true").csv("s3a://dataops/dirty_store_transactions.csv")

# Basit bir temizlik örneği
clean_df = df.filter(col("amount").isNotNull())

# PostgreSQL'e yaz
clean_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/traindb") \
    .option("dbtable", "public.clean_data_transactions") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

# Bitir
spark.stop()
