%python
# NOTE: Update the default path below to your accessible S3 bucket or Unity Catalog volume
# Example: "s3://your-bucket/raw/events/" or "/Volumes/<catalog>/<schema>/<volume>/events/"
dbutils.widgets.text("source_path", "/Volumes/workspace/default/events_data/events_cdc_sample.csv")
source_path = dbutils.widgets.get("source_path")

from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Use Unity Catalog schema (do NOT use hive_metastore)
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")

bronze_table = "workspace.default.events_raw"

schema = StructType([
  StructField("event_id", StringType(), True),
  StructField("user_id", StringType(), True),
  StructField("event_type", StringType(), True),
  StructField("event_ts", StringType(), True),  # parsed in silver
  StructField("payload", StringType(), True)
])

df = (spark.read
      .schema(schema)
      .json(source_path))

(df
 .withColumn("_ingest_time", current_timestamp())
 .withColumn("_source_file", col("_metadata.file_path"))
 .write
 .format("delta")
 .mode("append")
 .saveAsTable(bronze_table))