# Databricks notebook source
# create parameter for file date
dbutils.widgets.text("p_data_source", "")
dbutils.widgets.text("p_file_date", "")

# get the variables
v_data_source = dbutils.widgets.get("p_data_source")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Ingest lap time files in lap time folder

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read all the csv file in lap times folder

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/datalake01course/raw/{v_file_date}/lap_times/"))

# COMMAND ----------

from pyspark.sql.types import *

# Create schema for lap_times data
lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

lap_times_df = spark.read.csv(
    f"/mnt/datalake01course/raw/{v_file_date}/lap_times/lap_times_split*.csv",
    schema=lap_times_schema,
    header=True
)
display(lap_times_df.head(5))
print(lap_times_df.count())


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Transform & Write data

# COMMAND ----------

# drop column
from pyspark.sql.functions import lit, col, concat, current_timestamp, from_utc_timestamp
lap_times_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumn("ingestion_date", current_timestamp()
    ).withColumn("data_source", lit(v_data_source)
    ).withColumn("file_date", lit(v_file_date))

display(lap_times_df)

# COMMAND ----------

# MAGIC %run "./../includes/config"

# COMMAND ----------

# MAGIC %run "./../includes/common_functions"

# COMMAND ----------

# # load data
# lap_times_df.write.parquet(r"/mnt/datalake01course/processed/lap_times",
#                          mode="overwrite")

# lap_times_df.write.saveAsTable("f1_processed.lap_times",
#                             mode="overwrite",
#                             format="parquet")

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
ETLFunction().merge_delta_data(lap_times_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")