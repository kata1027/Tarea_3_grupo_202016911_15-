#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("StressStreamingApp") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

schema = StructType() \
    .add("post_id", StringType()) \
    .add("text", StringType()) \
    .add("label", StringType())

df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stress-topic") \
    .option("startingOffsets", "earliest") \
    .load()

df_stream = df_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

count_df = df_stream.groupBy("label").count()

query = count_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()


# In[ ]:




