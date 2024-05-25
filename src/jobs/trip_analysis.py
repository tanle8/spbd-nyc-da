#!/usr/bin/env python
# coding: utf-8

# Trip Analysis Script for Google Cloud Dataproc

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, hour, dayofweek, month

def main():
    # Setup the Spark session
    spark = SparkSession.builder.appName("NYC Taxi Trip Analysis").getOrCreate()

    # Define GCS path for data
    BUCKET_NAME = "spbd-nyc-taxi-bucket"
    TRAIN_PROCESSED = f"gs://{BUCKET_NAME}/data/processed/train_processed.parquet"

    # Load the processed data from GCS
    df = spark.read.parquet(TRAIN_PROCESSED)

    # Calculate average trip duration and distance
    avg_trip_stats = df.select(
        avg("trip_duration").alias("average_trip_duration"),
        avg("trip_distance_km").alias("average_trip_distance_km")
    )
    avg_trip_stats.show()

    # Add time columns for detailed analysis
    df = df.withColumn("hour", hour("pickup_datetime"))
    df = df.withColumn("day_of_week", dayofweek("pickup_datetime"))
    df = df.withColumn("month", month("pickup_datetime"))

    # Cache the DataFrame to optimize multiple actions
    df.cache()

    # Trigger caching with an action
    df.count()

    # Group by new time columns and calculate averages
    time_analysis = df.groupBy("hour", "day_of_week", "month").agg(
        avg("trip_duration").alias("avg_duration"),
        avg("trip_distance_km").alias("avg_distance")
    ).orderBy("day_of_week", "hour", "month")
    time_analysis.show()

    # Identify top 10 pickup and drop-off locations
    top_pickup_locations = df.groupBy("pickup_latitude", "pickup_longitude").count().orderBy(col("count").desc()).limit(10)
    top_pickup_locations.show()

    top_dropoff_locations = df.groupBy("dropoff_latitude", "dropoff_longitude").count().orderBy(col("count").desc()).limit(10)
    top_dropoff_locations.show()

    # Unpersist the df to free up resources
    df.unpersist()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
