# Databricks notebook source
# MAGIC %md
# MAGIC #### Acess dataframes using SQL
# MAGIC ##### Objectives:
# MAGIC 1. Create temporary view on Dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell

# COMMAND ----------

# MAGIC %run "./../includes/config"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;
# MAGIC
# MAGIC SHOW TABLES in global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from v_race_results
# MAGIC WHERE driver_number = 44;

# COMMAND ----------

race_year =2018
spark.sql(f"""SELECT * FROM v_race_results 
          WHERE race_year = {race_year}""")

location = "Suzuka"
spark.sql(f"""SELECT * FROM v_race_results 
          WHERE circuit_location = '{location}'""").show(2)

# COMMAND ----------

# race_results_df.createGlobalTempView("gv_race_results")
race_results_df.createOrReplaceGlobalTempView("gv_race_results")