from pyspark.sql.functions import to_timestamp, sha2, concat_ws, col

# Use the standard Unity Catalog namespace for CE
bronze_table = "default.events_raw"
silver_table = "default.events_silver"

# Ensure the source table actually exists
try:
    raw = spark.table(bronze_table)
except Exception as e:
    print(f"Error: {bronze_table} not found. Ensure you created it in the Catalog.")
    # Fallback: if you haven't created the table yet, read from your Volume path
    # raw = spark.read.load("/Volumes/main/default/raw_data/events")

clean = (raw
  .withColumn("event_time", to_timestamp(col("event_ts")))
  .drop("event_ts")
  .filter(col("event_id").isNotNull())
)

deduped = clean.dropDuplicates(["event_id"])

# Save the silver table
deduped.write.mode("overwrite").saveAsTable(silver_table)
