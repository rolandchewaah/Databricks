from pyspark.sql.functions import to_timestamp, sha2, concat_ws, col

bronze_table = "bronze.events_raw"
silver_table = "silver.events"

raw = spark.table(bronze_table)

clean = (raw
  .withColumn("event_time", to_timestamp(col("event_ts")))
  .drop("event_ts")
  .filter(col("event_id").isNotNull())
)

# Deduplicate by event_id (or a deterministic hash key if needed)
deduped = clean.dropDuplicates(["event_id"])

(deduped
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(silver_table))
