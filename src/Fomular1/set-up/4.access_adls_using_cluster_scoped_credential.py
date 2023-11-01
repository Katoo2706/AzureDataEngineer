# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake using cluster scoped credentials
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster
# MAGIC 2. List files from demo
# MAGIC 3. Read data
# MAGIC

# COMMAND ----------

# display file
storage_account = "datalake01course"
demo_container = f"abfss://demo@{storage_account}.dfs.core.windows.net"
display(dbutils.fs.ls(demo_container))

# COMMAND ----------

df = spark.read.csv(f"{demo_container}/circuits.csv", header=True)
df.show(4)