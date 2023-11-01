# Azure Data Factory
#### Learning resource:
- **Tutorial**: https://learn.microsoft.com/en-us/azure/data-factory/data-factory-tutorials
- **Video**: https://www.youtube.com/channel/UC2S0k7NeLcEm5_IhHUwpN0g/featured

![data_factories.png](media%2Fdata_factories.png)

#### Create CI/CD with git
![adf.png](media%2Fadf.png)

Or after launching the Studio, choose Linked service and set up the git repo
![adf-git.png](media%2Fadf-git.png)

Create pipeline
![adf-pipeline.png](media%2Fadf-pipeline.png)

**Set up the linked Databrick Notebook**
![adf-config-task.png](media%2Fadf-config-task.png)

To use Managed Service Identity
- Change access (IAM) in Azure Databricks Service - workspace
- Privileged Administrator Roles / Contributor
![adf-access-databrick.png](media%2Fadf-access-databrick.png)
- Add menber etl-adls-course (Data Factory)
![adf-add-member.png](media%2Fadf-add-member.png)
- After that, choose the access from existing cluster
![adf-config-task-access.png](media%2Fadf-config-task-access.png)
*If it's not loaded, click other cluster and re-click the existing cluster for refresh UI*

- Add the dynamic parameters by using Variables from the pipeline
![adf-dynamic-parameter-using-var.png](media%2Fadf-dynamic-parameter-using-var.png)
- Or using parameter (Can not be changed)
![adf-dynamic-parameter-using-parameter.png](media%2Fadf-dynamic-parameter-using-parameter.png)
- Config the DAGs
![adf-DAGs.png](media%2Fadf-DAGs.png)

#### Add Get metadata to check if file exists

| Set up the storage account                | Get parameter as directory                 |
|-------------------------------------------|--------------------------------------------|
| ![Left Image](./media/adf-metadata-1.png) | ![Right Image](./media/adf-metadata-2.png) |

**Check if data exists or not**
![adf-if-condition.png](media%2Fadf-if-condition.png)

Then, click True (edit) and paste the Notebook into Window
![adf-if-true.png](media%2Fadf-if-true.png)

#### Final: Create master execute pipeline
![adf-execute-pipeline.png](media%2Fadf-execute-pipeline.png)