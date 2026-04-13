# Multi-sink pipeline: PostgreSQL + Kafka + Parquet
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, current_timestamp
from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql.functions import approx_count_distinct
from pyspark.sql.functions import to_date
from pyspark.sql.functions import to_json, struct
from pyspark.sql.functions import expr
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import to_timestamp
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
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "user_activity") \
    .option("startingOffsets", "latest") \
    .load()
df = df.selectExpr("CAST(value AS STRING)")
df = df.withColumn("jsonData", from_json(col("value"), schema)).select("jsonData.*")

# Watermark
df = df.withWatermark("event_time", "10 minutes")

# 1-min tumbling window page views
page_views = df.filter(col("event_type") == "page_view") \
    .groupBy(window("event_time", "1 minute"), col("page_url")) \
    .count() \
    .withColumnRenamed("count", "view_count") \
    .selectExpr("window.start as window_start", "window.end as window_end", "page_url", "view_count")

def upsert_page_views(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", "page_view_counts") \
        .option("user", db_user) \
        .option("password", db_password) \
        .mode("append") \
        .save()

page_views.writeStream \
    .foreachBatch(upsert_page_views) \
    .outputMode("update") \
    .trigger(processingTime='10 seconds') \
    .start()
active_users = df \
    .groupBy(
        window(col("event_time"), "5 minutes", "1 minute")
    ) \
    .agg(
        approx_count_distinct("user_id").alias("active_users")
    ) \
    .selectExpr(
        "window.start as window_start",
        "window.end as window_end",
        "active_users"
    )
def write_active_users(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", "active_users") \
        .option("user", db_user) \
        .option("password", db_password) \
        .mode("append") \
        .save()
active_users.writeStream \
    .foreachBatch(write_active_users) \
    .outputMode("update") \
    .trigger(processingTime='10 seconds') \
    .start()
df_with_date = df.withColumn("event_date", to_date("event_time"))

df_with_date.writeStream \
    .format("parquet") \
    .option("path", "/tmp/lake") \
    .option("checkpointLocation", "/tmp/checkpoint_parquet")\
    .start()
enriched_df = df.withColumn("processing_time", current_timestamp())

kafka_df = enriched_df.selectExpr(
    "CAST(user_id AS STRING) AS key",
    "to_json(struct(*)) AS value"
)

kafka_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("topic", "enriched_activity") \
    .option("checkpointLocation", "/tmp/kafka-checkpoint") \
    .start()

sessions = df.filter(col("event_type").isin("session_start", "session_end"))

sessionized = sessions.groupBy("user_id", window("event_time", "10 minutes")) \
    .agg(
        expr("min(case when event_type='session_start' then event_time end)").alias("session_start"),
        expr("max(case when event_type='session_end' then event_time end)").alias("session_end")
    ) \
    .withColumn(
        "duration",
        unix_timestamp("session_end") - unix_timestamp("session_start")
    )

def write_sessions(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", "user_sessions") \
        .option("user", db_user) \
        .option("password", db_password) \
        .mode("append") \
        .save()

sessionized.writeStream \
    .foreachBatch(write_sessions) \
    .outputMode("complete") \
    .trigger(processingTime='10 seconds') \
    .start()
spark.streams.awaitAnyTermination()