from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Clean Store Transactions") \
    .getOrCreate()

# S3 yerine doğrudan mount edilen path
df = spark.read.option("header", "true").csv("/dataops/scripts/dirty_store_transactions.csv")

# Temizleme işlemleri...
clean_df = df.dropna()

# Yine aynı klasöre temizlenmiş halini yaz
clean_df.write.mode("overwrite").option("header", "true").csv("/dataops/scripts/clean_data_transactions.csv")

spark.stop()
