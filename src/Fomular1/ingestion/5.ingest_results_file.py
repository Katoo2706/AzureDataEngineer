# Databricks notebook source
# create parameter for file date
dbutils.widgets.text("p_data_source", "")
dbutils.widgets.text("p_file_date", "")

# get the variables
v_data_source = dbutils.widgets.get("p_data_source")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Ingest Results file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the Json using the Spark dataframe reader

# COMMAND ----------

display(dbutils.fs.ls("/mnt/datalake01course/raw/"))

# COMMAND ----------

from pyspark.sql.types import *

# Create schema for results data
results_schema = StructType(
    [
        StructField("resultId", IntegerType(), False),
        StructField("raceId", IntegerType(), True),
        StructField("driverId", IntegerType(), True),
        StructField("constructorId", IntegerType(), True),
        StructField("number", IntegerType(), True),
        StructField("grid", IntegerType(), True),
        StructField("position", IntegerType(), True),
        StructField("positionText", StringType(), True),
        StructField("positionOrder", IntegerType(), True),
        StructField("points", FloatType(), True),
        StructField("laps", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
        StructField("fastestLap", IntegerType(), True),
        StructField("rank", IntegerType(), True),
        StructField("fastestLapTime", StringType(), True),
        StructField("fastestLapSpeed", FloatType(), True),
        StructField("statusId", StringType(), True)
    ]
)

# COMMAND ----------

results_df = spark.read.json(f"/mnt/datalake01course/raw/{v_file_date}/results.json",
                             schema=results_schema
                             )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Transform

# COMMAND ----------

# Drop duplicate
results_df = results_df.dropDuplicates(['raceId', 'resultId'])

# COMMAND ----------

# drop column
from pyspark.sql.functions import lit, col, concat, current_timestamp, from_utc_timestamp
results_df = results_df.drop(col('statusId'))

# rename and create new columns
results_df = results_df.withColumnsRenamed({
    "resultId": "result_id",
    "raceId": "race_id",
    "driverId": "driver_id",
    "constructorId": "constructor_id",
    "positionText": "position_text",
    "positionOrder": "position_order",
    "fastestLap": "fastest_lap",
    "fastestLapTime": "fastest_lap_time",
    "fastestLapSpeed": "fastest_lap_speed"
}).withColumns({
    'ingestion_date': from_utc_timestamp(current_timestamp(), 'UTC'),  # get current UTC timestamp
    'data_source': lit(v_data_source),
    'file_date': lit(v_file_date)
})
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write data to parquet format

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Method 1**

# COMMAND ----------

spark.catalog.tableExists("f1_processed.results") == True

# COMMAND ----------

# for race_id_list in results_df.select("race_id").distinct().collect():
#     if spark.catalog.tableExists("f1_processed.results") == True:
#         spark.sql(f"""
#             ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})
#         """)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Method 2**
# MAGIC
# MAGIC Using write.insertInto

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

# MAGIC %run "./../includes/common_functions"

# COMMAND ----------

# partition_column = "race_id"

# results_df = ETLFunction().rearrange_partition_col(
#     table=results_df,
#     partition_column=partition_column
# )

# COMMAND ----------

# ETLFunction().overwrite_partition(
#     input_df=results_df,
#     db_name="f1_processed",
#     table_name="results",
#     partition_col="race_id"
# )

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
ETLFunction().merge_delta_data(
    input_df=results_df,
    db_name="f1_processed",
    table_name="results",
    folder_path="/mnt/datalake01course/processed/results",
    merge_condition=merge_condition,
    partition_col="race_id"
)

# COMMAND ----------

# # load data to storage account
# results_df.write.parquet(r"/mnt/datalake01course/processed/results",
#                          mode="overwrite",
#                          partitionBy="race_id")

# save as managed table
# results_df.write.saveAsTable("f1_processed.results",
#                             mode="append",
#                             format="parquet",
#                             partitionBy="race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, COUNT(1) 
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC HAVING COUNT(1) > 1
# MAGIC ORDER BY race_id, driver_id DESC;