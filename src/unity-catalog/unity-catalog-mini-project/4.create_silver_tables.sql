-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC #### Create managed table in the silver shema
-- MAGIC 1. drivers
-- MAGIC 2. results

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_dev.silver.drivers;

CREATE TABLE IF NOT EXISTS formula1_dev.silver.drivers
AS
SELECT 
  driverId as driver_id,
  driverRef as driver_ref,
  number,
  code,
  concat(name.forename, ' ', name.surname) as name,
  dob,
  nationality,
  current_timestamp() AS ingestion_date
FROM formula1_dev.bronze.drivers;

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_dev.silver.results;

CREATE TABLE IF NOT EXISTS formula1_dev.silver.results
AS
SELECT 
  resultId AS result_id,
  raceId AS race_id,
  driverId AS driver_id,
  constructorId AS constructor_id,
  number,
  grid,
  position,
  positionText AS position_text,
  positionOrder AS position_order,
  points,
  laps,
  time,
  milliseconds,
  fastestLapTime AS fastest_lap_time,
  fastestLapSpeed AS fastest_lap_speed,
  statusId AS status_id,
  current_timestamp() AS ingestion_date
FROM formula1_dev.bronze.results;