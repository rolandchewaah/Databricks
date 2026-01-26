import sys
from config.settings import Tables, KEY_COLUMN
from bronze.ingest import run as bronze_run
from silver.cdc_merge import run as silver_run
from quality.checks import run as quality_run
from gold.publish import run as gold_run
from pyspark.sql import SparkSession  # Assuming PySpark is used

tables = Tables()

# Initialize Spark session
spark = SparkSession.builder.appName("DataPipeline").getOrCreate()

if len(sys.argv) < 2:
    raise ValueError("Task argument is required. Usage: python main.py [bronze|silver|quality|gold]")

task = sys.argv[1]

if task == "bronze":
    bronze_run(spark, "/mnt/raw/events/", tables.bronze)

elif task == "silver":
    silver_run(spark, tables.bronze, tables.silver, KEY_COLUMN)

elif task == "quality":
    quality_run(spark, tables.silver, KEY_COLUMN)

elif task == "gold":
    gold_run(spark, tables.silver, tables.gold)

else:
    raise ValueError(f"Unknown task: {task}")
