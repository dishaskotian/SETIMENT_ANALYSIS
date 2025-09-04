from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, lower, regexp_replace
from pyspark.sql.types import IntegerType
import time

# Initialize Spark
spark = SparkSession.builder.appName("BatchHashtagProcessor").getOrCreate()

# Start timer
start_time = time.time()

# Load CSVs
df = spark.read.option("header", "false").csv("output/hashtags/*/*.csv").toDF("word", "count")

# Cast count to IntegerType
df = df.withColumn("count", col("count").cast(IntegerType()))

# Clean hashtags
df = df.withColumn("word", lower(col("word"))) \
       .withColumn("word", regexp_replace(col("word"), r"[^a-zA-Z0-9#]", ""))

# Group and aggregate
result = df.groupBy("word").sum("count").withColumnRenamed("sum(count)", "total_count") \
           .orderBy(col("total_count").desc())

# Show top 10
result.show(10, truncate=False)

# Timer end
end_time = time.time()
print(f"\n⏱️ Batch processing took {end_time - start_time:.2f} seconds")
