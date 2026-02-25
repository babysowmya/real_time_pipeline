from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, current_timestamp
from pyspark.sql.types import StructType, StringType, TimestampType
import os

# Initialize Spark
spark = SparkSession.builder.appName("RealTimePipeline").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Kafka & DB configs
kafka_bootstrap = os.environ['KAFKA_BOOTSTRAP_SERVERS']
db_url = os.environ['DB_URL']
db_user = os.environ['DB_USER']
db_password = os.environ['DB_PASSWORD']

# Define Schema
schema = StructType() \
    .add("event_time", TimestampType()) \
    .add("user_id", StringType()) \
    .add("page_url", StringType()) \
    .add("event_type", StringType())

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", "user_activity") \
    .option("startingOffsets", "latest") \
    .load()

df = df.selectExpr("CAST(value AS STRING)")
df = df.withColumn("jsonData", from_json(col("value"), schema)).select("jsonData.*")

# Watermark
df = df.withWatermark("event_time", "2 minutes")

# 1-min tumbling window page views
page_views = df.filter(col("event_type") == "page_view") \
    .groupBy(window("event_time", "1 minute"), col("page_url")) \
    .count() \
    .withColumnRenamed("count", "view_count") \
    .selectExpr("window.start as window_start", "window.end as window_end", "page_url", "view_count")

page_views.writeStream \
    .foreachBatch(lambda batch_df, _: batch_df.write \
                  .format("jdbc") \
                  .option("url", db_url) \
                  .option("dbtable", "page_view_counts") \
                  .option("user", db_user) \
                  .option("password", db_password) \
                  .mode("append") \
                  .save()) \
    .outputMode("update") \
    .start()
spark.streams.awaitAnyTermination()