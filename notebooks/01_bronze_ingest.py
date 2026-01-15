from pyspark.sql.functions import input_file_name, current_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

source_path = dbutils.widgets.get("source_path") if dbutils.widgets.get("source_path") else "/mnt/raw/events/"
bronze_table = "bronze.events_raw"

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
 .withColumn("_source_file", input_file_name())
 .write
 .format("delta")
 .mode("append")
 .saveAsTable(bronze_table))
