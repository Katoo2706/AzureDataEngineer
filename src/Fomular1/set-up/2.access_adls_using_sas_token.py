# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake Storage using Shared access signature Token
# MAGIC 1. Set the spark config for SAS token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file
# MAGIC

# COMMAND ----------

# get sas token & sas url

# list scope list
scope_list = dbutils.secrets.listScopes()
print("Scope list:", scope_list)

# list secrets
secret_list = dbutils.secrets.list(scope=scope_list[0].name)
print("Secret list:", secret_list)

# Get SAS token & SAS url
blob_sas_token = dbutils.secrets.get(scope=scope_list[0].name,
                                        key="fomula1-blob-sas-token")
fomula1_sas_url = dbutils.secrets.get(scope=scope_list[0].name,
                                        key="fomula1-blob-sas-url")


# COMMAND ----------

# Set up the access key
spark.conf.set("fs.azure.account.auth.type.datalake01course.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.datalake01course.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.datalake01course.dfs.core.windows.net", blob_sas_token)

# COMMAND ----------

# abfss://<container>@<storageAccountName>.dfs.core.windows.net
demo_container = "abfss://demo@datalake01course.dfs.core.windows.net"
display(dbutils.fs.ls(demo_container))

# COMMAND ----------

df = spark.read.csv(f"{demo_container}/circuits.csv", header=True)
df.show(4)