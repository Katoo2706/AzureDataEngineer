# Databricks notebook source
# MAGIC %md
# MAGIC #### Explode DBFS root
# MAGIC 1. List all folders in DBFS root
# MAGIC 2. Interact with DBFS files browser
# MAGIC 3. Upload to DBFS root

# COMMAND ----------

# get directories from root
display(dbutils.fs.ls('/'))

# COMMAND ----------

# get directories from root
display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/circuits.csv", header='true')
display(df)