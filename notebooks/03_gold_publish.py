from pyspark.sql.functions import to_date, count, col, lit

# 1. Ensure the Gold schema exists in the Hive Metastore
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

events = spark.table("silver.events")

# 2. Check if event_type exists; if not, use a fallback or create a placeholder
if "event_type" not in events.columns:
    # If the column is missing, we'll create a dummy one so the code doesn't break
    # Or, if you meant to use 'ip' or 'temp' from your earlier error, change "event_type" below
    events = events.withColumn("event_type", lit("standard"))

daily = (events
  .withColumn("event_date", to_date(col("event_time")))
  .groupBy("event_date", "event_type")
  .agg(count("*").alias("event_count"))
)

# 3. Save to the Gold table
(daily
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("gold.daily_metrics"))
