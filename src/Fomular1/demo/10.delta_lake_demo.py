# Databricks notebook source
# MAGIC %md
# MAGIC #### Objectives:
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (Table)
# MAGIC 4. Read data from delta lake (File)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION "/mnt/datalake01course/demo";
# MAGIC
# MAGIC DESCRIBE DATABASE f1_demo;

# COMMAND ----------

results_df = spark.read.option("inferSchema", True).json(path="/mnt/datalake01course/raw/2021-03-28/results.json")

# COMMAND ----------

# Write to Delta lake
results_df.write.format("delta").saveAsTable("f1_demo.results_managed",
                                             mode="overwrite")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE EXTENDED f1_demo.results_managed;

# COMMAND ----------

# Save as external table
results_df.write.format("delta").save("/mnt/datalake01course/demo/results_external",
                                      mode="overwrite")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/datalake01course/demo/results_external";

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/datalake01course/demo/results_external")

# COMMAND ----------

# Write table by Partition
results_df.write.format("delta").saveAsTable("f1_demo.results_partitioned",
                                            mode="overwrite",
                                            partitionBy="constructorId")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Objectives:
# MAGIC 1. Update Delta Table
# MAGIC 2. Delete From Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update table
# MAGIC UPDATE f1_demo.results_managed
# MAGIC   SET points = 11 - position
# MAGIC WHERE position <= 10

# COMMAND ----------

# Update table
from delta.tables import * # Import the Delta Lake API
from pyspark.sql.functions import *

# Create a DeltaTable object for the specified path
deltaTable = DeltaTable.forPath(spark, "/mnt/datalake01course/demo/results_managed")

# Update records in the Delta table
# Condition: "position <= 10"
# Columns to update: Set "points" to "21 - position"
deltaTable.update("position <= 10", { "points": "21 - position" })

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/datalake01course/demo/results_managed")

deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Upsert using merge
# MAGIC 1. Using SQL
# MAGIC 2. Using Pyspark

# COMMAND ----------

drivers_day1_df = spark.read.option("inferSchema", True).json(
    "/mnt/datalake01course/raw/2021-03-28/drivers.json"
    ).filter("driverId <= 10"
    ).select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read.option("inferSchema", True).json(
    "/mnt/datalake01course/raw/2021-03-28/drivers.json"
    ).filter("driverId BETWEEN 6 AND 15"
    ).select(
        "driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname")
    )

# COMMAND ----------

drivers_day3_df = spark.read.option("inferSchema", True).json(
    "/mnt/datalake01course/raw/2021-03-28/drivers.json"
    ).filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20"
    ).select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))


# COMMAND ----------

display(drivers_day1_df)
display(drivers_day2_df)
display(drivers_day3_df)

drivers_day1_df.createOrReplaceTempView("drivers_day1")
drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_demo.drivers_merge;
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING, 
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC DESCRIBE EXTENDED f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Merge table to target table
# MAGIC MERGE INTO f1_demo.drivers_merge as tgt
# MAGIC USING drivers_day1 as upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   tgt.dob = upd.dob,
# MAGIC   tgt.forename = upd.forename,
# MAGIC   tgt.surname = upd.surname,
# MAGIC   tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   driverId, dob, forename, surname, createdDate)
# MAGIC   VALUES (
# MAGIC   driverId, dob, forename, surname, current_timestamp )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Merge table to target table
# MAGIC MERGE INTO f1_demo.drivers_merge as tgt
# MAGIC USING drivers_day2 as upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   tgt.dob = upd.dob,
# MAGIC   tgt.forename = upd.forename,
# MAGIC   tgt.surname = upd.surname,
# MAGIC   tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   driverId, dob, forename, surname, createdDate)
# MAGIC   VALUES (
# MAGIC   driverId, dob, forename, surname, current_timestamp )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Upsert using Pyspark

# COMMAND ----------

# Import the DeltaTable class from the delta.tables module
from delta.tables import DeltaTable

# Create a DeltaTable instance for the specified path
deltaTable = DeltaTable.forPath(spark, "/mnt/datalake01course/demo/drivers_merge")

# Alias the target (destination) DeltaTable as "tgt" and merge it with the source dataframe "drivers_day3_df" with a matching condition
deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"), # table to upsert
    "tgt.driverId = upd.driverId"
).whenMatchedUpdate(
    set={
        "dob": "upd.dob", 
        "forename": "upd.forename", 
        "surname": "upd.surname", 
        "updatedDate": "current_timestamp()"
    } # Update existing records with new values and update the "updatedDate" column
).whenNotMatchedInsert(
    values={
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename" : "upd.forename", 
      "surname" : "upd.surname", 
      "createdDate": "current_timestamp()"
    } # Insert new records and populate the "createdDate" column with the current timestamp
).execute() # Execute the merge operation

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM f1_demo.drivers_merge;