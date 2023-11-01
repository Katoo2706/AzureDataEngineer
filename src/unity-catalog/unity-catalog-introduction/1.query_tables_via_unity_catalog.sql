-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Query table via Unity Catalog

-- COMMAND ----------

SELECT * FROM demo_catalog.demo_schema.circuits;

-- COMMAND ----------

-- Error if not specify the catalog name
SELECT * FROM demo_schema.circuits;

-- COMMAND ----------

SELECT current_catalog();

-- COMMAND ----------

SHOW CATALOGS;

USE CATALOG demo_catalog;

SELECT current_catalog();

-- COMMAND ----------

SHOW SCHEMAS;

USE SCHEMA demo_schema;

-- COMMAND ----------

USE CATALOG demo_catalog;
USE SCHEMA demo_schema;
SELECT * FROM circuits;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Show all table
-- MAGIC display(spark.sql("SHOW TABLES;"));

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Get table in catalog
-- MAGIC df = spark.table('demo_catalog.demo_schema.circuits')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC display(df)