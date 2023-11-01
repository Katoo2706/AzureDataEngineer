# Databricks notebook source
# create parameter for file date
dbutils.widgets.text("p_data_source", "")
dbutils.widgets.text("p_file_date", "")

# get the variables
v_data_source = dbutils.widgets.get("p_data_source")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Ingest Pipstops file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the Json using the Spark dataframe reader

# COMMAND ----------

display(dbutils.fs.ls("/mnt/datalake01course/raw/"))

# COMMAND ----------

from pyspark.sql.types import *

# Create schema for results data
pit_stops_schema = StructType(
    [
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), True),
        StructField("stop", StringType(), True),
        StructField("lap", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("duration", StringType(), True),
        StructField("milliseconds", IntegerType(), True)
    ]
)

pit_stops_df = spark.read.json(
    f"/mnt/datalake01course/raw/{v_file_date}/pit_stops.json",
    schema=pit_stops_schema,
    multiLine=True  # For JSON (one record per file), set the multiLine parameter to true (\n). Ignore #_corrupt_record:string
)
display(pit_stops_df.head(5))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Transform & Write data

# COMMAND ----------

# drop column
from pyspark.sql.functions import lit, col, concat, current_timestamp, from_utc_timestamp
pit_stops_df = pit_stops_df.drop(col('statusId'))

# rename and create new columns
pit_stops_df = pit_stops_df.withColumnsRenamed({
    "raceId": "race_id",
    "driverId": "driver_id"
}).withColumns({
    'ingestion_date': from_utc_timestamp(current_timestamp(), 'UTC'),  # get current UTC timestamp
    'data_source': lit(v_data_source),
    'file_date': lit(v_file_date)
    })
display(pit_stops_df)

# COMMAND ----------

# MAGIC %run "./../includes/common_functions"

# COMMAND ----------

# # load data
# pit_stops_df.write.parquet(r"/mnt/datalake01course/processed/pit_stops",
#                          mode="overwrite")

# save as table
# pit_stops_df.write.saveAsTable("f1_processed.pit_stops",
#                             mode="overwrite",
#                             format="parquet")

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
ETLFunction().merge_delta_data(
    input_df=pit_stops_df,
    db_name="f1_processed",
    table_name="pit_stops",
    folder_path="/mnt/datalake01course/processed/pit_stops",
    merge_condition=merge_condition,
    partition_col="race_id"
)

# COMMAND ----------

dbutils.notebook.exit("Success")