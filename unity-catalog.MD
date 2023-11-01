# I. Process

## Step 1: Create Azure Databrick Workspace
![uc-create-db-ws.png](media%2Fuc-create-db-ws.png)
> Only premium tier Databricks workspace can use Unity Catalog

## Step 2: Create Storage Account (ADLS)

![uc-storage-account.png](media%2Fuc-storage-account.png)

*Select hierarchical to enables file and directory semantics, accelerates big data analytics workloads, and enables access control lists (ACLs)*
> Advanced / Hierarchical Namespace

## Step 3: Create Access Connector for Azure Databricks
**Go to resource Access Connector for Azure Databricks**
![uc-access-connector.png](media%2Fuc-access-connector.png)

- Create container `metastore` and specify the path when create metastore in Databricks workspace

## Step 4: Assign role Storage Blob Data Contributor to Access Connector in Storage Account IAM
![uc-role-assignment.png](media%2Fuc-role-assignment.png)
- Role:                 Storage Blob Data Contributor
- Assign access to: 	Managed identity
- Member:               Azure subscription 1 / Access Connector for Azure Databricks


## Step 5: Create Unity Catalog Metastore
In Databricks workspace
- Click top right menu / Manage Account
- Navigate to tab Data / Create Metastore
- Copy the Access Connector Id in `Access Connector for Azure Databricks`/ `Property`
```markdown
Name: databrickcouse_uc_metastore
Region: southestasia
ADLS Gen 2 path: metastore@databrickcourseucdl.dfs.core.windows.net/   (metastore is the container we've created)
Access Connector Id: /subscriptions/79698cb5-3fc6-48fa-a42c-ce8b84d1143e/resourceGroups/databrickcourse-uc-rg/providers/Microsoft.Databricks/accessConnectors/databrickcourse-uc-access
```
- Assign Metastore to Databricks Workspace 
![uc-assign-ws.png](media%2Fuc-assign-ws.png)

# II. Create Compute for Databricks workspace with Unity Catalog
*Note: When assign workspace to Unity Catalog successfully, compute will have Unity Catalog*
![uc-db-compute.png](media%2Fuc-db-compute.png)

In Databricks workspace
![uc-ws-catalog.png](media%2Fuc-ws-catalog.png)

### Click create a new Catalog to create Unity Catalog
```markdown
Catalog name: demo_catalog
Type: Standard
```
![uc-demo-catalog.png](media%2Fuc-demo-catalog.png)

- In Catalog, Create a New Schema
- In Schema, can Add data from many external data sources (3rd party integration)
- Choose the Catalog: demo_catalog, demo_schema
  - See the Table type, storage location in Details.

# Accessing External Locations
![uc-external-locations.png](media%2Fuc-external-locations.png)

# III. Create Storage Credential
### Step 1: Create Access Connector for Azure Databricks
![uc-storage-credential.png](media%2Fuc-storage-credential.png)

### Step 2: Create Storage Account: databrickcourseextdl
![uc-storage-account-credential.png](media%2Fuc-storage-account-credential.png)
- Then, click `Enable Hierarchical Namespace`
- Create `demo` container
- Upload `circuits.csv` to the container

### Step 3: Grant access to the container
- IAM / Add role Assignment
- Choose `Storage Blob Data Contributor`
- Choose `Managed Identity` in `Members`
- Select `+ Member` / Access Connector for Azure Databricks / databricks-course-uc-ext-access
- Review + Assign

### Step 4: Create Storage Credential
- Go to Databricks Workspace
- Catalog / External Data / `Storage Credentials`
- Click `Create credential` and fill value for:
  - Storage credential name: databrickcourse-ext-storage-credential
  - Access connector ID: *Copy in the Property of the Access Connector*
![uc-ext-credential.png](media%2Fuc-ext-credential.png)

### Step 5: Create external Container
> External location is simply an object that wraps the storage credential with the Azure Data Lake Storage Container.
- Go to Databricks Workspace
- Catalog / External Data / `External Locations`
- Click `Create location` and fill value for:
  - External location name: databrickcourseucdl_demo (Should using container name as a part)
  - Storage credential: databrickcourse-ext-storage-credential (Above storage credential)
  - URL: abfss://demo@databrickcourseextdl.dfs.core.windows.net/ (`abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<path>`)
![uc-external-location.png](media%2Fuc-external-location.png)

### Create external location for project
Reference: https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-location#syntax
```sql
CREATE EXTERNAL LOCATION IF NOT EXISTS databrickcourseextdl_bronze
 URL 'abfss://bronze@databrickcourseextdl.dfs.core.windows.net/'
 WITH (STORAGE CREDENTIAL `databrickcourse-ext-storage-crendential`);
```
### Access data in external location
![uc-ext-location.png](media%2Fuc-ext-location.png)
