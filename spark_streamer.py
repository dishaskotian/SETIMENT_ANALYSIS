import time
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, split, regexp_replace, lower
from pyspark.sql.types import StructType, StringType

# Schema for Kafka messages
schema = StructType() \
    .add("username", StringType()) \
    .add("text", StringType())

# Start Spark Session
spark = SparkSession.builder \
    .appName("HashtagCounter") \
    .config("spark.sql.shuffle.partitions", "6") \
    .config("spark.default.parallelism", "6") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Connect to Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tweetsTopic3") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse and clean tweet text
tweets = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.username", "data.text") \
    .withColumn("text", lower(col("text"))) \
    .withColumn("text", regexp_replace(col("text"), r"[^a-zA-Z0-9#@\s]", ""))

# Extract hashtags
hashtags = tweets.select(explode(split(col("text"), " ")).alias("word")) \
                 .filter(col("word").rlike("^#\\w+"))

# Count hashtags
hashtag_counts = hashtags.groupBy("word").count()

# ✅ Define batch writer: timestamp per batch, top 20 rows
def write_to_csv_and_console(batch_df, batch_id):
    start_time = time.time()

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"output/hashtags/batch_{timestamp}"

    print(f"\n🕒 Batch Timestamp: {timestamp}")
    print(f"-------------------------------------------")
    print(f"Batch ID: {batch_id}")
    print(f"-------------------------------------------")

    top20 = batch_df.orderBy(col("count").desc()).limit(20)

    # No coalesce – allow parallelism
    top20.write.mode("overwrite").parquet(output_path)

    top20.show(truncate=False)

    end_time = time.time()
    print(f"⏱ Batch {batch_id} processed in {end_time - start_time:.2f} seconds\n")

# ✅ Write to CSV and print using foreachBatch
csv_query = hashtag_counts.writeStream \
    .outputMode("update") \
    .trigger(processingTime="1 seconds") \
    .foreachBatch(write_to_csv_and_console) \
    .option("checkpointLocation", "output/checkpoints") \
    .start()

# Wait for stream to finish
csv_query.awaitTermination()