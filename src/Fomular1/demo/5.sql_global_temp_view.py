# Databricks notebook source
# race_results_df.createGlobalTempView("gv_race_results")
# race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.gv_race_results;

# COMMAND ----------

spark.sql("""
        SELECT *
        FROM global_temp.gv_race_results
        WHERE race_year BETWEEN 2019 AND 2020;
    """).show()