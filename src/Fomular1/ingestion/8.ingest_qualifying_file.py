# Databricks notebook source
# create parameter for file date
dbutils.widgets.text("p_data_source", "")
dbutils.widgets.text("p_file_date", "")

# get the variables
v_data_source = dbutils.widgets.get("p_data_source")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Ingest qualifying files in qualifying folder

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read all the csv file in qualifying folder

# COMMAND ----------

# qualifying_df = spark.read.json(
#     f"/mnt/datalake01course/raw/{v_file_date}/qualifying/qualifying_split_1.json",
#     multiLine=True
# )

# COMMAND ----------

from pyspark.sql.types import *

# Create schema for qualifying data
qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])


qualifying_df = spark.read.json(
    f"/mnt/datalake01course/raw/{v_file_date}/qualifying/qualifying*.json",
    schema=qualifying_schema,
    multiLine=True
)
display(qualifying_df.head(5))
print(qualifying_df.count())


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Transform & Write data

# COMMAND ----------

# drop column
from pyspark.sql.functions import lit, col, concat, current_timestamp, from_utc_timestamp
final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumn("ingestion_date", current_timestamp()
    ).withColumn("data_source", lit(v_data_source)
    ).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %run "./../includes/config"

# COMMAND ----------

# MAGIC %run "./../includes/common_functions"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE f1_processed.lap_times;

# COMMAND ----------

# # load data
# final_df.write.parquet(r"/mnt/datalake01course/processed/qualifying",
#                          mode="overwrite")

# final_df.write.saveAsTable("f1_processed.qualifying",
#                             mode="overwrite",
#                             format="parquet")

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
ETLFunction().merge_delta_data(input_df=final_df, db_name='f1_processed', table_name='lap_times', folder_path=processed_folder_path, merge_condition=merge_condition, partition_col='race_id')

# COMMAND ----------

# dbutils.fs.rm("/mnt/formula1dl/processed", True)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/"))

# COMMAND ----------

dbutils.notebook.exit("Success")