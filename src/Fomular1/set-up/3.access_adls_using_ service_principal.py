# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake Storage using Service Principal
# MAGIC **Step to follow**
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret / password for the Application 
# MAGIC 3. Get Spark Config with App/ Client id, Directory / Tenant Id & Secret 
# MAGIC 4. Get Client id, Directory / Tenant Id & Secret from Vault
# MAGIC 5. Assign Role 'Storage Blob Data Contributor' to the Data Lake
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
client_id = dbutils.secrets.get(scope=scope_list[0].name,
                                key="fomula1-app-client-id")

tenant_id = dbutils.secrets.get(scope=scope_list[0].name,
                                key="fomula1-app-tenant-id")

client_secret = dbutils.secrets.get(scope=scope_list[0].name,
                                key="fomula1-app-client-secret")

# COMMAND ----------

# Set up the env
storage_acount = "datalake01course"

spark.conf.set(f"fs.azure.account.auth.type.{storage_acount}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_acount}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_acount}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_acount}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_acount}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# abfss://<container>@<storageAccountName>.dfs.core.windows.net
demo_container = "abfss://demo@datalake01course.dfs.core.windows.net"
display(dbutils.fs.ls(demo_container))

# COMMAND ----------

df = spark.read.csv(f"{demo_container}/circuits.csv", header=True)
df.show(4)