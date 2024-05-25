#!/usr/bin/env python
# coding: utf-8

# Fare Analysis Script for Google Cloud Dataproc

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, corr

def main():
    # Setup the Spark session
    spark = SparkSession.builder.appName("Fare Analysis").getOrCreate()

    # Define paths for GCP execution
    BUCKET_NAME = "spbd-nyc-taxi-bucket"
    TRAIN_PROCESSED = f"gs://{BUCKET_NAME}/data/processed/yellow_tripdata_2024-01.parquet"
    OUTPUT_DIR = f"gs://{BUCKET_NAME}/results/analysed_data/"

    # Load the processed data from GCS
    df = spark.read.parquet(TRAIN_PROCESSED)
    df.printSchema()

    # Calculate average fares by pickup and drop-off locations
    avg_fares_location = df.groupBy("PULocationID", "DOLocationID").agg(avg("fare_amount").alias("avg_fare"))
    avg_fares_location.show()

    # Calculate average fares by passenger count
    avg_fares_passenger = df.groupBy("passenger_count").agg(avg("fare_amount").alias("avg_fare"))
    avg_fares_passenger.show()

    # Explore correlations between fare amounts and trip distances
    fare_distance_correlation = df.select(corr("fare_amount", "trip_distance").alias("correlation"))
    fare_distance_correlation.show()

    # Save the results to GCS
    avg_fares_location.write.format("parquet").save(f"{OUTPUT_DIR}avg_fares_location.parquet")
    avg_fares_passenger.write.format("parquet").save(f"{OUTPUT_DIR}avg_fares_passenger.parquet")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
