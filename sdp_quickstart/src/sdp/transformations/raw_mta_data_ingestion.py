from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, DateType, TimestampType
from pyspark.sql import SparkSession

spark = SparkSession.active()

mta_schema = StructType([
    StructField('actual_track', StringType(), True),
    StructField('arrival_time', StringType(), True),   # Raw ISO8601 string
    StructField('departure_time', StringType(), True), # Raw ISO8601 string
    StructField('direction', StringType(), True),
    StructField('ingestion_ts', StringType(), True),
    StructField('is_assigned', BooleanType(), True),
    StructField('route_id', StringType(), True),
    StructField('scheduled_track', StringType(), True),
    StructField('stop_id', StringType(), True),
    StructField('train_id', StringType(), True),
    StructField('trip_id', StringType(), True),
    StructField('date', DateType(), True),
    StructField('hour', IntegerType(), True)
])

input_path = "/app/sdp/pipeline-storage/"

@dp.table(name="streaming_windowed_counts")
def ingest_and_calculate():
    """Ingests raw MTA data and performs stateful windowed aggregation.

    This function defines a streaming table that parses JSON files, normalizes
    route IDs, and calculates route frequencies over a 10-minute sliding window
    with watermarking to handle late-arriving data.

    Returns:
        pyspark.sql.DataFrame: A streaming DataFrame containing windowed route counts.
    """
    # Loading data from a streaming source
    raw_stream = (spark.readStream
                  .format("json")
                  .schema(mta_schema)
                  .option("multiLine", "false")
                  .load(input_path))

    # Incremental transformations for cleansing and event-time extraction
    cleaned_df = (raw_stream
                  .withColumn("route_id", F.upper(F.col("route_id")))
                  .withColumn("event_time", F.to_timestamp(F.col("arrival_time")))
                  .withColumn("processing_ts", F.current_timestamp()))

    # Stateful Aggregation (Watermarking + Windowing)
    windowed_counts = (cleaned_df
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            F.window(F.col("event_time"), "10 minutes", "5 minutes"),
            F.col("route_id")
        )
        .count()
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window"))

    return windowed_counts

@dp.materialized_view(name="top_5_routes_every10mins")
def top_5_sink():
    """Materializes a batch snapshot of the top 5 busiest subway routes.

    Reads from the intermediate 'streaming_windowed_counts' table and applies
    global sorting and limiting. Using a materialized view ensures the result
    is precomputed into a table.

    Returns:
        pyspark.sql.DataFrame: A batch DataFrame representing the current top 5 leaderboard.
     """
    # Querying a table defined earlier in the pipeline
    ranked_df = (spark.table("streaming_windowed_counts")
            .orderBy(F.col("count").desc())
            .limit(5))

    return ranked_df
