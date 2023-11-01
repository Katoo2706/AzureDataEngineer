-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

USE f1_processed;

-- COMMAND ----------

SELECT driver_id, driver_ref, split(name, ' ')[0] as forename,
  CONCAT(driver_ref, '-', code) as new_driver_ref
FROM f1_processed.drivers;

-- COMMAND ----------

SELECT driver_id, name, code, date_format(dob, 'dd-MM-yy')
FROM f1_processed.drivers