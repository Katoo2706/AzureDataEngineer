-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Objectives:
-- MAGIC **1. Use Spark Dataframe with SQL**
-- MAGIC - Spark SQL documentation
-- MAGIC - Create Database demo
-- MAGIC - Data tab in the UI
-- MAGIC - SHOW command
-- MAGIC - DESCRIBE command
-- MAGIC - Find the current database

-- COMMAND ----------

-- CREATE DATABASE demo;

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- See the current db
SELECT CURRENT_DATABASE();

USE demo;

-- COMMAND ----------

DESCRIBE DATABASE demo;

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Learning object - MANAGED TABLE
-- MAGIC 1. Create managed table using Python
-- MAGIC 2. Create managed table using SQL
-- MAGIC 3. Drop tables
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "./../includes/config"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.saveAsTable("demo.race_results_python", format="parquet")

-- COMMAND ----------

USE demo;
SHOW TABLES;

DESCRIBE EXTENDED race_results_python;

-- COMMAND ----------

CREATE TABLE race_results_sql
AS (
  SELECT * 
  FROM demo.race_results_python
  WHERE race_year > 2020
);

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Learning object - EXTERNAL TABLE
-- MAGIC 1. Create external table using Python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Drop tables
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results_df.write.format("parquet").mode("overwrite").option("path", f"{presentation_folder_path}/ext_race_results_python").saveAsTable("demo.ext_race_results_python")

-- COMMAND ----------

DESC EXTENDED demo.ext_race_results_python;

-- DROP TABLE demo.ext_race_results_python;

-- COMMAND ----------

CREATE EXTERNAL TABLE demo.ext_race_results_sql
USING parquet
OPTIONS (
  'path' '/mnt/datalake01course/presentation/ext_race_results_sql'
)
AS
SELECT *
FROM demo.race_results_python;

-- COMMAND ----------

DESC EXTENDED demo.ext_race_results_sql;

-- COMMAND ----------

INSERT INTO demo.ext_race_results_sql
SELECT * FROM demo.ext_race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

SELECT COUNT(1) from demo.ext_race_results_python;