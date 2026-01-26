# --- CELL 1: Setup & Transformation ---
from pyspark.sql.functions import to_timestamp, col

# Ensure schema exists in Hive Metastore
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# Load your source (update this path to your actual file location)
# Since /mnt/ is disabled, use Workspace or DBFS path
raw = spark.read.format("json").load("/databricks-datasets/learning-spark-v2/iot-devices/iot_devices.json")

clean = (raw
  .withColumn("event_time", to_timestamp(col("timestamp") / 1000)) # Example fix
  .filter(col("device_id").isNotNull())
)

# Define 'deduped' variable
deduped = clean.dropDuplicates(["device_id"])

# WRITE to the table so it exists for the next cell
deduped.write.format("delta").mode("overwrite").saveAsTable("silver.events")


# --- CELL 2: Assertions ---
# Now 'deduped' is defined in memory, AND the table is saved
silver = spark.table("silver.events")

assert silver.filter(col("device_id").isNull()).count() == 0, "Null ID found"
print("Assertions passed!")
