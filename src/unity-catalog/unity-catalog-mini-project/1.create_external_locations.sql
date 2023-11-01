-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC #### Create the external locations required for this project
-- MAGIC 1. Bronze
-- MAGIC 2. Silver
-- MAGIC 3. Gold

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databrickcourseextdl_bronze
    URL 'abfss://bronze@databrickcourseextdl.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL `databrickcourse-ext-storage-crendential`);

CREATE EXTERNAL LOCATION IF NOT EXISTS databrickcourseextdl_silver
    URL 'abfss://silver@databrickcourseextdl.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL `databrickcourse-ext-storage-crendential`);

CREATE EXTERNAL LOCATION IF NOT EXISTS databrickcourseextdl_gold
    URL 'abfss://gold@databrickcourseextdl.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL `databrickcourse-ext-storage-crendential`);

-- COMMAND ----------

DESC EXTERNAL LOCATION databrickcourseextdl_bronze;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'abfss://bronze@databrickcourseextdl.dfs.core.windows.net/'