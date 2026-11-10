from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, from_json, max as _max, min as _min, sum as _sum, window
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

spark = SparkSession.builder.appName("Ex5_WeatherAggregates").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_time", TimestampType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("temperature", DoubleType()),
    StructField("windspeed", DoubleType()),
    StructField("weather_code", IntegerType()),
    StructField("wind_alert_level", StringType()),
    StructField("heat_alert_level", StringType()),
])

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "weather_transformed")
    .option("startingOffsets", "earliest")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("d"))
    .select("d.*")
)

wind_alert = col("wind_alert_level").isin("level_1", "level_2").cast("int")
heat_alert = col("heat_alert_level").isin("level_1", "level_2").cast("int")

agg_df = (
    df.withWatermark("event_time", "1 minute")
    .groupBy(
        window(col("event_time"), "1 minute", "30 seconds"),
        col("country"),
        col("city"),
    )
    .agg(
        _sum(wind_alert).alias("wind_alerts_l1_l2"),
        _sum(heat_alert).alias("heat_alerts_l1_l2"),
        _sum(wind_alert + heat_alert).alias("total_alerts"),
        avg("temperature").alias("avg_temperature"),
        _min("temperature").alias("min_temperature"),
        _max("temperature").alias("max_temperature"),
    )
)

query = (
    agg_df.writeStream.outputMode("update")
    .format("console")
    .option("truncate", False)
    .start()
)

query.awaitTermination()

