from pyspark.sql.functions import col

silver = spark.table("silver.events")

assert silver.filter(col("event_id").isNull()).count() == 0, "Null event_id found"
assert silver.filter(col("event_time").isNull()).count() == 0, "Unparsed event_time found"

# Example: enforce uniqueness
dupes = silver.groupBy("event_id").count().filter(col("count") > 1).count()
assert dupes == 0, f"Duplicate event_id found: {dupes}"
