from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, dayofweek
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

def main():
    BUCKET_NAME = "spbd-nyc-taxi-bucket"

    spark = SparkSession.builder.appName("Demand Prediction").getOrCreate()
    
    # Load data from GCS
    df = spark.read.parquet(f"gs://{BUCKET_NAME}/data/processed/train_processed.parquet")
    
    # Feature engineering
    df = df.withColumn("year", year("pickup_datetime"))
    df = df.withColumn("month", month("pickup_datetime"))
    df = df.withColumn("day", dayofmonth("pickup_datetime"))
    df = df.withColumn("hour", hour("pickup_datetime"))
    df = df.withColumn("day_of_week", dayofweek("pickup_datetime"))

    # Group by date, hour, and day of the week to count pickups
    hourly_pickups = df.groupBy("year", "month", "day", "hour", "day_of_week").count().withColumnRenamed("count", "num_pickups")

    # Split the data into training and test sets (80% training, 20% test)
    train_data, test_data = hourly_pickups.randomSplit([0.8, 0.2], seed=1234)


    # Prepare data for the model
    assembler = VectorAssembler(
        inputCols=["year", "month", "day", "hour", "day_of_week"], outputCol="features")
    
    # Initialize the linear regression model
    lr = LinearRegression(featuresCol="features", labelCol="num_pickups")
    
    
    pipeline = Pipeline(stages=[assembler, lr])
    
    model = pipeline.fit(train_data)

    # Save the model to GCS
    model.save(f"gs://{BUCKET_NAME}/results/models/taxi_demand_prediction_model")
    
    spark.stop()

if __name__ == "__main__":
    main()
