-- Databricks notebook source
DESC f1_processed.drivers;

-- COMMAND ----------

SELECT *
FROM f1_processed.circuits as cc
LEFT JOIN f1_processed.races as rc
  ON cc.circuit_id = rc.circuit_id;