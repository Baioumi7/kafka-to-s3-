
# from pyspark.sql import SparkSession, DataFrame
# from pyspark.sql.functions import from_json, col, to_timestamp
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
# import os

# # Kafka topics
# topics = ["WEATHER_CAIRO", "WEATHER_ALEXANDRIA", "WEATHER_NYC"]

# # S3 configuration
# s3_bucket = "weather-iti1"

# def main():
#     spark = SparkSession.builder.appName("weather-iti") \
#         .config("spark.jars.packages", 
#                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
#                 "org.apache.hadoop:hadoop-aws:3.3.1") \
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#         .config("spark.hadoop.fs.s3a.access.key", "?") \
#         .config("spark.hadoop.fs.s3a.secret.key", "?") \
#         .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
#                 "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
#         .getOrCreate()
    
#     # Define the schema for the Kafka messages
#     weather_schema = StructType([
#         StructField("date", StringType()),
#         StructField("city", StringType()),
#         StructField("temperature_2m", DoubleType()),
#         StructField("relative_humidity_2m", DoubleType()),
#         StructField("rain", DoubleType()),
#         StructField("snowfall", DoubleType()),
#         StructField("weather_code", IntegerType()),
#         StructField("surface_pressure", DoubleType()),
#         StructField("cloud_cover", IntegerType()),
#         StructField("cloud_cover_low", IntegerType()),
#         StructField("cloud_cover_high", IntegerType()),
#         StructField("wind_direction_10m", IntegerType()),
#         StructField("wind_direction_100m", IntegerType()),
#         StructField("soil_temperature_28_to_100cm", DoubleType())
#     ])

#     def read_kafka_topic(topic, schema):
#         return (spark.readStream
#             .format("kafka")
#             .option("kafka.bootstrap.servers", "localhost:9092")  # Updated to match producer
#             .option("subscribe", topic)
#             .option("startingOffsets", "earliest")
#             .load()
#             .selectExpr("CAST(value AS STRING)")
#             .select(from_json(col("value"), schema).alias("data"))
#             .select("data.*")
#             .withColumn("date", to_timestamp("date")))  # Convert date string to timestamp

#     def streamWriter(input: DataFrame, checkpointFolder, output):
#         return (input.writeStream
#             .format("parquet")
#             .option("checkpointLocation", checkpointFolder)
#             .option("path", output)
#             .partitionBy("city")  # Partition by city for efficient querying
#             .outputMode("append")
#             .start())

#     # Create dataframes for each city
#     for topic in topics:
#         df = read_kafka_topic(topic, weather_schema)
#         city_name = topic.split("_")[1].lower()
#         query = streamWriter(df, 
#                              f"s3a://{s3_bucket}/checkpoints/{city_name}_data", 
#                              f"s3a://{s3_bucket}/data/{city_name}_data")

#     spark.streams.awaitAnyTermination()

# if __name__ == "__main__":
#     main()
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import os
import sys
from pyspark.errors import StreamingQueryException

# Kafka topics
topics = ["WEATHER_CAIRO", "WEATHER_ALEXANDRIA", "WEATHER_NYC"]

# S3 configuration
s3_bucket = "weather-iti1"

def create_spark_session():
    return SparkSession.builder.appName("weather-iti") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", "?") \
        .config("spark.hadoop.fs.s3a.secret.key", "?") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.metrics.system.enabled", "false") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.streaming.kafka.consumer.cache.enabled", "false") \
        .getOrCreate()

def define_schema():
    return StructType([
        StructField("date", StringType()),
        StructField("city", StringType()),
        StructField("temperature_2m", DoubleType()),
        StructField("relative_humidity_2m", DoubleType()),
        StructField("rain", DoubleType()),
        StructField("snowfall", DoubleType()),
        StructField("weather_code", IntegerType()),
        StructField("surface_pressure", DoubleType()),
        StructField("cloud_cover", IntegerType()),
        StructField("cloud_cover_low", IntegerType()),
        StructField("cloud_cover_high", IntegerType()),
        StructField("wind_direction_10m", IntegerType()),
        StructField("wind_direction_100m", IntegerType()),
        StructField("soil_temperature_28_to_100cm", DoubleType())
    ])

def read_kafka_topic(spark, topic, schema):
    return (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:29092")  # Changed to broker:29092
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "1000")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
        .withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
    )

def streamWriter(input: DataFrame, checkpointFolder, output):
    return (input.writeStream
        .format("parquet")
        .option("checkpointLocation", checkpointFolder)
        .option("path", output)
        .partitionBy("city")
        .outputMode("append")
        .start())

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    weather_schema = define_schema()

    active_streams = []

    try:
        for topic in topics:
            df = read_kafka_topic(spark, topic, weather_schema)
            city_name = topic.split("_")[1].lower()
            query = streamWriter(df, 
                                 f"s3a://{s3_bucket}/checkpoints/{city_name}_data", 
                                 f"s3a://{s3_bucket}/data/{city_name}_data")
            active_streams.append(query)

        print("Streams started successfully. Waiting for termination...")
        spark.streams.awaitAnyTermination()

    except StreamingQueryException as sqe:
        print(f"Streaming query failed: {sqe.message}")
        print(f"Error code: {sqe.errorClass}")
        print(f"Query ID: {sqe.query.id}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
    finally:
        print("Stopping all active streams...")
        for stream in active_streams:
            stream.stop()
        print("All streams stopped. Exiting.")

if __name__ == "__main__":
    main()
