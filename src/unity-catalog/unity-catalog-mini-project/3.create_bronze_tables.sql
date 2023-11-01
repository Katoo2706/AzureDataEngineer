-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC #### Create Bronze Tables
-- MAGIC 1. drivers.json
-- MAGIC 2. results.json Bronze folder path - 'abfss://bronze@databrickcourseextdl.dfs.core.windows.net/'

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS formula1_dev.bronze.drivers (
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path "abfss://bronze@databrickcourseextdl.dfs.core.windows.net/drivers.json")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS formula1_dev.bronze.results (
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points INT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId STRING
)
USING json
OPTIONS (path "abfss://bronze@databrickcourseextdl.dfs.core.windows.net/results.json");