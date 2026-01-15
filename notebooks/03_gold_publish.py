from pyspark.sql.functions import to_date, count, col

silver_table = "silver.events"
gold_table = "gold.daily_metrics"

events = spark.table(silver_table)

daily = (events
  .withColumn("event_date", to_date(col("event_time")))
  .groupBy("event_date", "event_type")
  .agg(count("*").alias("event_count"))
)

(daily
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(gold_table))
