# Data Lake & connect to Azure Data Lake Storage account
<!-- TOC -->
* [Data Lake & connect to Azure Data Lake Storage account](#data-lake--connect-to-azure-data-lake-storage-account)
  * [Create Azure Storage Account](#create-azure-storage-account)
  * [Case 1: Session Scoped Authentication (Via notebook)](#case-1-session-scoped-authentication-via-notebook)
    * [1. Access Azure Data Lake storage using access key](#1-access-azure-data-lake-storage-using-access-key)
    * [2. Access Azure Data Lake storage using Shared access signature](#2-access-azure-data-lake-storage-using-shared-access-signature)
    * [3. Access Azure Data Lake using Service Principal](#3-access-azure-data-lake-using-service-principal)
  * [Case 2: Cluster Scoped Authentication (Via cluster)](#case-2-cluster-scoped-authentication-via-cluster)
    * [1. Enable credential passthrough for user-level data access](#1-enable-credential-passthrough-for-user-level-data-access)
<!-- TOC -->

## Create Azure Storage Account
![Storage account](media/create_storage_account.png)

*Select hierarchical to enables file and directory semantics, accelerates big data analytics workloads, and enables access control lists (ACLs)*
![Storage account](media/create_storage_account_2.png)

*Choose the storage account*
![Storage account](media/storage_3.png)

*Download storage browser*
![Storage account](media/storage_brower.png)
[Download](https://azure.microsoft.com/en-us/products/storage/storage-explorer/)

## Case 1: Session Scoped Authentication (Via notebook)

### 1. Access Azure Data Lake storage using access key
```bash
abfss://<container>@<storageAccountName>.dfs.core.windows.net
```
![Access ky](media/access_key.png)
[The Azure Blob Filesystem driver (ABFS)](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-abfs-driver)
**Set up the access key**
https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-abfs-driver
```
spark.conf.set(
    "fs.azure.account.key.datalake01course.dfs.core.windows.net",
    "**access_key**"
)
```

### 2. Access Azure Data Lake storage using Shared access signature
- [Connect to Azure Data Lake Storage Gen2 using SAS token](https://learn.microsoft.com/en-us/azure/databricks/storage/azure-storage#sastokens:~:text=Active%20Directory%20application.-,Sas%C2%A0tokens,-You%20can%20configure)
- [Grant limited access to Azure Storage resources using shared access signatures (SAS)](https://learn.microsoft.com/en-us/azure/storage/common/storage-sas-overview)
    ```python
    # Set up the access key
    spark.conf.set("fs.azure.account.auth.type.datalake01course.dfs.core.windows.net", "SAS")
    spark.conf.set("fs.azure.sas.token.provider.type.datalake01course.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set("fs.azure.sas.fixed.token.datalake01course.dfs.core.windows.net", blob_sas_token)
    ```
Generate SAS token from container
  ![generate sas token](media/sas_token.png)

  ![generate sas token](media/grant_privileges.png)

Otherwise, SAS can be generated from Microsoft Azure Storage Explorer
![SAS via Microsoft Azure Storage Explorer](media/sas_app.png)

### 3. Access Azure Data Lake using Service Principal
[Azure service principal](https://learn.microsoft.com/en-us/azure/databricks/storage/azure-storage#adls-gen2-oauth-20-with-azure-service-principals-notebook:~:text=or%20a%20notebook%3A-,Azure%C2%A0service%C2%A0principal,-Use%20the%20following)

**Step to follow:**

- Create app: Choose service Azure Active Directory / Choose App registrations / New registration (Choose default setting)
![App registration](media/app_registration.png)
Copy client_id, tenant_id
```python
  client_id = "9f567be4-00d2-43e3-a598-5ad66ab4b7a2"
  tenant_id = "65125821-fb0a-471b-9f01-9c5a0b740768"
  
  # To get client secret, choose Certificates & secrets / + New client secret -> copy value
  client_secret = "3tR8Q~jXwP~ikTFFVh0BYxoloj7ZSceQleML2btr" 
```

- Assign role to app: In storage account, choose Access Control (IAM) / Add / Add role assignment (Storage Blob Data Contributor)
![Add role assignment](media/storage_blob.png)

## Case 2: Cluster Scoped Authentication (Via cluster)
### 1. Enable credential passthrough for user-level data access
![Add role assignment](media/cluster-scoped.png)
> Same as Access Azure Data Lake using Service Principal, we need to grant access to creator account (who own cluster) in Storage account\
> Even though I'm the owner, I do not have the role to read data from the storage account.
- Assign role to Cluster: In storage account, choose Access Control (IAM) / Add / Add role assignment (Storage Blob Data Contributor)
