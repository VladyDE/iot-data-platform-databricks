from pyspark import pipelines as dp
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField
from pyspark.sql.functions import *
from utilities import utils

#-----------------------------------BRONZE-----------------------------------------
@dp.table(
    name="bronze_iot_telemetry",
    comment="Raw data from parquet files in the telemetry directory"
)
def bronze_iot_telemetry():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/Volumes/workspace/projects/iot_devices/_checkpoints/bronze_telemetry")
        .option("cloudFiles.schemaEvolutionMode","rescue")
        .load("/Volumes/workspace/projects/iot_devices/iot_telemetry/")
        .select(
                "*", 
                current_timestamp().alias("ingestion_timestamp"),
                col("_metadata.file_path").alias("source_file")
            )
    )

#----------------------------------SILVER--------------------------------------------
@dp.table(
    name="silver_iot_telemetry",
    comment="Ensuring data quality expectations are met, and data is cleaned"
)
@dp.expect_all_or_drop(utils.rulesDrop)
@dp.expect_all(utils.rulesWarn)
def silver_iot_telemetry():
    df=spark.readStream.table("bronze_iot_telemetry")
    df=df.withColumn("timestamp", timestamp_micros(col("timestamp_us")))
    return(df)

