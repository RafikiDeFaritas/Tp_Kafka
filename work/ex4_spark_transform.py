import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json, when
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

shutil.rmtree("/tmp/checkpoint_ex4", ignore_errors=True)
spark = SparkSession.builder.appName("Ex4_WeatherTransform").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("current", StructType([
        StructField("temperature_2m", DoubleType()),
        StructField("wind_speed_10m", DoubleType()),
        StructField("weather_code", IntegerType()),
    ])),
])

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "weather_stream")
    .option("startingOffsets", "earliest")
    .load()
)

transformed_df = (
    raw_df.select(
        col("timestamp").alias("event_time"),
        from_json(col("value").cast("string"), schema).alias("data"),
    )
    .filter(col("data.current").isNotNull())
    .select(
        "event_time",
        col("data.city").alias("city"),
        col("data.country").alias("country"),
        col("data.current.temperature_2m").alias("temperature"),
        col("data.current.wind_speed_10m").alias("windspeed"),
        col("data.current.weather_code").alias("weather_code"),
    )
    .withColumn(
        "wind_alert_level",
        when(col("windspeed") < 10, "level_0")
        .when(col("windspeed") <= 20, "level_1")
        .otherwise("level_2"),
    )
    .withColumn(
        "heat_alert_level",
        when(col("temperature") < 25, "level_0")
        .when(col("temperature") <= 35, "level_1")
        .otherwise("level_2"),
    )
)

def process_batch(batch_df, batch_id):
    if not batch_df.isEmpty():
        (
            batch_df.select(to_json(struct("*")).alias("value"))
            .write.format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("topic", "weather_transformed")
            .save()
        )
        batch_df.show(truncate=False)
        print(f"Batch {batch_id} envoyé vers 'weather_transformed'")

query = (
    transformed_df.writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", "/tmp/checkpoint_ex4")
    .start()
)

query.awaitTermination()
