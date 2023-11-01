# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest Drivers file

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

# Create schema for structype
name_schema = StructType([
    StructField(name='forename', dataType=StringType(), nullable=True),
    StructField(name='surname', dataType=StringType(), nullable=True)
])

drivers_schema = StructType(
    [
        StructField(name="driverId", dataType=IntegerType(), nullable=False),
        StructField(name="driverRef", dataType=StringType(), nullable=True),
        StructField(name="number", dataType=StringType(), nullable=True),
        StructField(name="code", dataType=StringType(), nullable=True),
        StructField(name="name", dataType=name_schema),
        StructField(name="dob", dataType=DateType(), nullable=True),
        StructField(name="nationality", dataType=StringType(), nullable=True),
        StructField(name="url", dataType=StringType(), nullable=True),
    ])

# COMMAND ----------

drivers_df = spark.read.json(f"/mnt/datalake01course/raw/{v_file_date}/drivers.json",
                             schema=drivers_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Transform & Write data

# COMMAND ----------

# drop column
from pyspark.sql.functions import lit, col, concat, current_timestamp, from_utc_timestamp
drivers_df = drivers_df.drop(col('url'))  # or construcutors_df['url]

# rename and create new columns
drivers_df = drivers_df.withColumnsRenamed({
    "driverId": "driver_id",
    "driverRef": "driver_ref"
}).withColumns({
    'ingestion_date': from_utc_timestamp(current_timestamp(), 'UTC'),  # get current UTC timestamp
    'name': concat(col('name.forename'), lit(' '), col('name.surname'))
})
display(drivers_df)

# COMMAND ----------

# # load data
# drivers_df.write.parquet(r"/mnt/datalake01course/processed/drivers",
#                          mode="overwrite")

# # save as table
# drivers_df.write.saveAsTable("f1_processed.drivers",
#                             mode="overwrite",
#                             format="parquet")

drivers_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")