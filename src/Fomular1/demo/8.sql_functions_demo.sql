-- Databricks notebook source
SELECT COUNT(*), nationality
FROM f1_processed.drivers
WHERE nationality = 'British'
GROUP BY nationality;

-- COMMAND ----------

SELECT 
  nationality, 
  name, 
  dob,
  RANK() OVER(PARTITION BY nationality ORDER BY dob DESC) AS age_rank
FROM f1_processed.drivers
ORDER BY nationality, age_rank;