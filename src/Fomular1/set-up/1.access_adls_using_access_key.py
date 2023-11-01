# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake Storage key
# MAGIC 1. Set the spark config fs.azure.account.key (using Azure Blob Filesystem Driver - ABFS driver)
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file
# MAGIC

# COMMAND ----------

# Scope name was created by home/#secrets/createScope

# COMMAND ----------

# get access_token from key vault
fomula1_access_key = dbutils.secrets.get(scope="fomula1-scope", key="fomula1-access-token")

# COMMAND ----------

# Set up the access key
spark.conf.set(
    "fs.azure.account.key.datalake01course.dfs.core.windows.net",
    fomula1_access_key
)

# COMMAND ----------

# abfss://<container>@<storageAccountName>.dfs.core.windows.net
demo_container = "abfss://demo@datalake01course.dfs.core.windows.net"
display(dbutils.fs.ls(demo_container))

# COMMAND ----------

df = spark.read.csv(f"{demo_container}/circuits.csv", header=True)
df.show(4)