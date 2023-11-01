-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ### DROP ALL THE TABLES

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/datalake01course/processed"

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION  "/mnt/datalake01course/presentation"

-- COMMAND ----------

-- %fs
-- rm -r "/mnt/datalake01course/raw"