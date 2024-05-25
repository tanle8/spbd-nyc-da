#!/usr/bin/env python
# coding: utf-8

# Tip Analysis Script for Google Cloud Dataproc

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, avg, dayofweek, hour

def main():
    # Setup the Spark session
    spark = SparkSession.builder.appName("Tip Analysis").getOrCreate()

    # Define paths for GCP execution
    BUCKET_NAME = "spbd-nyc-taxi-bucket"
    DATA_FILE = f"gs://{BUCKET_NAME}/data/processed/yellow_tripdata_2024-01.parquet"
    OUTPUT_DIR = f"gs://{BUCKET_NAME}/results/analysed_data/"

    # Load the processed data from GCS
    df = spark.read.parquet(DATA_FILE)
    df.printSchema()

    # Calculate tip percentage by trip
    df = df.withColumn("tip_percentage", col("tip_amount") / (col("fare_amount") + col("extra") + col("mta_tax")) * 100)
    tip_percentage_by_trip = df.select("tip_percentage")
    tip_percentage_by_trip.show()

    # Investigate geographical variations in tipping
    avg_tips_by_location = df.groupBy("PULocationID", "DOLocationID").agg(avg("tip_percentage").alias("avg_tip_percentage"))
    avg_tips_by_location.show()

    # Study variations in tipping by time of day and week
    df = df.withColumn("pickup_dayofweek", dayofweek("tpep_pickup_datetime"))
    df = df.withColumn("pickup_hour", hour("tpep_pickup_datetime"))
    avg_tips_by_time = df.groupBy("pickup_dayofweek", "pickup_hour").agg(avg("tip_percentage").alias("avg_tip_percentage"))
    avg_tips_by_time.show()

    # Explore the impact of different payment types on tipping behavior
    avg_tips_by_payment_type = df.groupBy("payment_type").agg(avg("tip_percentage").alias("avg_tip_percentage"))
    avg_tips_by_payment_type.show()

    # Save results to Google Cloud Storage
    avg_tips_by_location.write.format("parquet").save(f"{OUTPUT_DIR}avg_tips_by_location.parquet")
    avg_tips_by_time.write.format("parquet").save(f"{OUTPUT_DIR}avg_tips_by_time.parquet")
    avg_tips_by_payment_type.write.format("parquet").save(f"{OUTPUT_DIR}avg_tips_by_payment_type.parquet")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
