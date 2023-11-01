-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### CREATE PROCCESSED DATABASE

-- COMMAND ----------

-- specify the location
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/datalake01course/processed"

-- COMMAND ----------

-- %fs
-- ls "/mnt/datalake01course/processed/"

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r "dbfs:/mnt/datalake01course/processed/"

-- COMMAND ----------

DESCRIBE DATABASE f1_processed;