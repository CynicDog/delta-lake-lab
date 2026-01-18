# Reference
#   - https://docs.databricks.com/aws/en/delta/delta-change-data-feed

from pyspark.sql import functions as F

# Read the change data feed as a stream.
# By using readStream, Spark tracks progress via the checkpoint.
cdf_stream_df = spark.readStream \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .table("customers")

updates_only_df = cdf_stream_df.filter(F.col("_change_type") == "update_postimage")

# Write with a Checkpoint
# The checkpointLocation is what prevents "duplicate entries" on next run.
query = updates_only_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/telemetry/checkpoints/customers_updates") \
    .outputMode("append") \
    .toTable("customers_updates")
