# Databricks notebook source
# MAGIC %md
# MAGIC ### Explore the capabilities of the dbutils.secrets utitlity

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

# list scope list
scope_list = dbutils.secrets.listScopes()
print(scope_list)

# COMMAND ----------

# list secrets
secret_list = dbutils.secrets.list(scope=scope_list[0].name)
print(secret_list)

# COMMAND ----------

# get secrets value
dbutils.secrets.get(scope=scope_list[0].name, key=secret_list[0].key)

# Or we can hard code
dbutils.secrets.get(scope="fomula1-scope", key="fomula1-access-token")