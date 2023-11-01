# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest constructors file

# COMMAND ----------

# create parameter for file date
dbutils.widgets.text("p_data_source", "")
dbutils.widgets.text("p_file_date", "")

# get the variables
v_data_source = dbutils.widgets.get("p_data_source")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the Json using the Spark dataframe reader

# COMMAND ----------

display(dbutils.fs.ls("/mnt/datalake01course/raw"))

# COMMAND ----------

from pyspark.sql.types import *

constructors_schmema = StructType(
    [
        StructField(name="constructorId", dataType=IntegerType(), nullable=False),
        StructField(name="constructorRef", dataType=StringType(), nullable=False),
        StructField(name="name", dataType=StringType(), nullable=False),
        StructField(name="nationality", dataType=StringType(), nullable=False),
        StructField(name="url", dataType=StringType(), nullable=False),
    ])

# COMMAND ----------

constructors_df = spark.read.json(f"/mnt/datalake01course/raw/{v_file_date}/constructors.json",
                                   schema=constructors_schmema)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Transform & Write data

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, from_utc_timestamp, lit
constructors_df = constructors_df.drop(col('url'))  # or construcutors_df['url]

# COMMAND ----------

constructors_df = constructors_df.withColumnsRenamed({
    "constructorId": "constructor_id",
    "constructorRef": "constructor_ref"
}).withColumn(
    'ingestion_date', from_utc_timestamp(current_timestamp(), 'UTC')  # get current UTC timestamp
).withColumn("data_source", lit(v_data_source)
).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# save as parquet file
# constructors_df.write.parquet(r"/mnt/datalake01course/processed/constructors",
#                               mode="overwrite")

# # save as managed table db
# constructors_df.write.saveAsTable("f1_processed.constructors",
#                                   mode="overwrite",
#                                   format="parquet")

constructors_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")