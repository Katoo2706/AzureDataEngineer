# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 3. Call file system utlity mount to mount the storage
# MAGIC 4. Explore other file system utlities related to mount (list all mounts, unmount)
# MAGIC
# MAGIC  Reference: https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts#--mount-adls-gen2-or-blob-storage-with-abfs

# COMMAND ----------

# get sas token & sas url

# list scope list
scope_list = dbutils.secrets.listScopes()
print("Scope list:", scope_list)

# list secrets
secret_list = dbutils.secrets.list(scope=scope_list[0].name)
print("Secret list:", secret_list)

# Get SAS token & SAS url
client_id = dbutils.secrets.get(scope=scope_list[0].name,
                                key="fomula1-app-client-id")

tenant_id = dbutils.secrets.get(scope=scope_list[0].name,
                                key="fomula1-app-tenant-id")

client_secret = dbutils.secrets.get(scope=scope_list[0].name,
                                key="fomula1-app-client-secret")

# COMMAND ----------

# Run the following in your notebook to authenticate and create a mount point.
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------


# Set up the env
storage_acount = "datalake01course"
container = "demo"
mount_name = "fomula1dl/demo"

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container}@{storage_acount}.dfs.core.windows.net/",
  mount_point = f"/mnt/{mount_name}",
  extra_configs = configs)

# COMMAND ----------

# # abfss://<container>@<storageAccountName>.dfs.core.windows.net
# demo_container = "abfss://demo@datalake01course.dfs.core.windows.net"
file_list = dbutils.fs.ls(f"/mnt/{mount_name}")
file_list[0].path

# COMMAND ----------

df = spark.read.csv(file_list[0].path, header=True)
df.show(4)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# Unmount
dbutils.fs.unmount('/mnt/fomula1/demo')