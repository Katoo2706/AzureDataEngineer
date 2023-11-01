-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Views on tables
-- MAGIC 1. Create Temp View
-- MAGIC 2. Create Global Temp View
-- MAGIC 3. Create Permanent View

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS (
  SELECT * 
  FROM demo.race_results_python
  WHERE race_year = 2018
);

-- COMMAND ----------

use demo;
SHOW TABLES;

-- COMMAND ----------

-- Create global temp view
CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS (
  SELECT * 
  FROM demo.race_results_python
  WHERE race_year = 2018
);

-- COMMAND ----------

use demo;
SHOW TABLES in global_temp;

-- COMMAND ----------

-- Permanent view
CREATE OR REPLACE VIEW demo.pv_race_results
AS (
  SELECT * 
  FROM demo.race_results_python
  WHERE race_year = 2018
);

-- Permanent view
CREATE OR REPLACE VIEW default.pv_race_results
AS (
  SELECT * 
  FROM demo.race_results_python
  WHERE race_year = 2018
);

-- COMMAND ----------

use demo;
SHOW TABLES;