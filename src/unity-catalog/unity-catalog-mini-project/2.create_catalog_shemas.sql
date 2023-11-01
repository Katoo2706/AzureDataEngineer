-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ### Create Catalogs and Schemas required for the project
-- MAGIC 1. Catalog - formula1_dev (**without managed location**)
-- MAGIC 2. Schemas - bronze, silver, and gold (**with managed location**)

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS formula1_dev;

USE CATALOG formula1_dev;

-- COMMAND ----------

-- Create schema with managed location
CREATE SCHEMA IF NOT EXISTS bronze
MANAGED LOCATION "abfss://bronze@databrickcourseextdl.dfs.core.windows.net/";

-- COMMAND ----------

-- Create schema with managed location
CREATE SCHEMA IF NOT EXISTS silver
MANAGED LOCATION "abfss://silver@databrickcourseextdl.dfs.core.windows.net/";

-- Create schema with managed location
CREATE SCHEMA IF NOT EXISTS gold
MANAGED LOCATION "abfss://gold@databrickcourseextdl.dfs.core.windows.net/";