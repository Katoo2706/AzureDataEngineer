# Databricks notebook source
# Mount Azure Data Lake Storage containers to project
def mount_adls(storage_account_name: str, container_name: str) -> str:
    # list scope list
    scope_list = dbutils.secrets.listScopes()

    # Get secret from Key Vaults
    client_id = dbutils.secrets.get(scope=scope_list[0].name, key="fomula1-app-client-id")
    tenant_id = dbutils.secrets.get(scope=scope_list[0].name, key="fomula1-app-tenant-id")
    client_secret = dbutils.secrets.get(scope=scope_list[0].name, key="fomula1-app-client-secret")

    # Run the following in your notebook to authenticate and create a mount point.
    configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Unmount and remount the container
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    # Optionally, you can add <directory-name> to the source URI of your mount point.
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs
    )

    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls(container_name="demo",
           storage_account_name="datalake01course")

# COMMAND ----------

mount_adls(container_name="raw",
           storage_account_name="datalake01course")
mount_adls(container_name="processed",
           storage_account_name="datalake01course")
mount_adls(container_name="presentation",
           storage_account_name="datalake01course")

# COMMAND ----------

# # # abfss://<container>@<storageAccountName>.dfs.core.windows.net
# # demo_container = "abfss://demo@datalake01course.dfs.core.windows.net"
# mount_name = f"{storage_account_name}/{container_name}"
# file_list = dbutils.fs.ls(f"/mnt/{mount_name}")
# file_list[0].path

# df = spark.read.csv(file_list[0].path, header=True)
# df.show(4)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# Unmount
dbutils.fs.unmount('/mnt/fomula1dl/demo')