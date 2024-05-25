from google.cloud import storage
import io
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, month, avg, stddev, when

BUCKET_NAME = "spbd-nyc-taxi-bucket"

def upload_blob(bucket_name, source_file, destination_blob_name, content_type='text/csv'):
    """Uploads a file to the bucket using a file-like object."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    try:
        blob.upload_from_file(source_file, content_type=content_type)
        print("File uploaded successfully to:", destination_blob_name)
    except Exception as e:
        print(f"Failed to upload file: {str(e)}")

def main():
    spark = SparkSession.builder.appName("Traffic Analysis").getOrCreate()
    
    # Adjust the path if running on GCP
    TRAIN_PROCESSED = f"gs://{BUCKET_NAME}/data/processed/train_processed.parquet"
    df = spark.read.parquet(TRAIN_PROCESSED)

    df = df.withColumn("duration_hours", col("trip_duration") / 3600)
    df = df.withColumn("trip_speed_km_per_hr", col("trip_distance_km") / col("duration_hours"))

    # Adding time components for grouping
    df = df.withColumn("hour", hour("pickup_datetime"))
    df = df.withColumn("day_of_week", dayofweek("pickup_datetime"))
    df = df.withColumn("month", month("pickup_datetime"))

    # Calculate average speeds and standard deviations
    speed_stats = df.groupBy("hour", "day_of_week", "month").agg(
        avg("trip_speed_km_per_hr").alias("avg_speed"),
        stddev("trip_speed_km_per_hr").alias("speed_stddev")
    )
    speed_stats.show()

    # Define distance categories and calculate average speeds
    df = df.withColumn("distance_category",
                       when(col("trip_distance_km") <= 2, "Short (<2km)")
                       .when((col("trip_distance_km") > 2) & (col("trip_distance_km") <= 5), "Medium (2-5km)")
                       .otherwise("Long (>5km)"))
    
    distance_speed_stats = df.groupBy("distance_category").agg(
        avg("trip_speed_km_per_hr").alias("avg_speed_by_dist")
    )
    distance_speed_stats.show()

    # Save data to CSV in memory
    speed_csv = io.StringIO()
    pd_speed_stats = speed_stats.toPandas()
    pd_speed_stats.to_csv(speed_csv, index=False)
    speed_csv.seek(0)

    distance_csv = io.StringIO()
    pd_distance_stats = distance_speed_stats.toPandas()
    pd_distance_stats.to_csv(distance_csv, index=False)
    distance_csv.seek(0)

    # Saving results
    RESULTS_DIR = "results"

    # Upload the CSV files
    upload_blob(BUCKET_NAME, speed_csv, f"{RESULTS_DIR}/traffic_speed_analysis-speed_stats.csv")
    upload_blob(BUCKET_NAME, distance_csv, f"{RESULTS_DIR}/traffic_speed_analysis-distance_speed_stats.csv")

    # Visualize the results
    plt.figure(figsize=(14, 7))
    sns.barplot(x="hour", y="avg_speed", hue="day_of_week", data=pd_speed_stats)
    plt.title("Average Trip Speed by Hour and Day of Week")
    plt.xlabel("Hour of Day")
    plt.ylabel("Average Speed (km/hr)")

    # Save the plot to a buffer
    buffer = io.BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)

    # Upload the plot to GCS
    plot_path = f"{RESULTS_DIR}/traffic_speed_analysis.png"
    upload_blob(BUCKET_NAME, buffer, plot_path, content_type='image/png')
    buffer.close()
    plt.close()

    # Clean up Spark session
    spark.stop()

if __name__ == "__main__":
    main()
