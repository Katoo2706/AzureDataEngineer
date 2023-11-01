# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING, 
# MAGIC   surname STRING,
# MAGIC   createdDate DATE, 
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Using SQL
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta;

# COMMAND ----------

# Using pySpark
df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/datalake01course/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# dbutils.fs.rm("/mnt/datalake01course/demo/drivers_convert_to_delta_new", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CONVERT TO DELTA parquet.`/mnt/datalake01course/demo/drivers_convert_to_delta_new`

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.convertToDelta(spark, "parquet.`/mnt/datalake01course/demo/drivers_convert_to_delta_new`")

# COMMAND ----------

deltaTable.detail()

# COMMAND ----------

deltaTable.createOrReplaceTempView("table_1")