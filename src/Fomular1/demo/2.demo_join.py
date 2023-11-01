# Databricks notebook source
# MAGIC %run ./../includes/config

# COMMAND ----------

# read data
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("name", "circuit_name")
races_df = spark.read.parquet(f"{processed_folder_path}/races",
                              filter="race_year > 2019")
circuits_df.show(5)
races_df.show(5)

# COMMAND ----------

# this join can only be done in spark dataframe
races_circuits_df = races_df.join(circuits_df, on = (races_df.circuit_id == circuits_df.circuit_id), how="left")
display(races_circuits_df.select("circuit_name"))

# COMMAND ----------

# full outerjoin
races_circuits_df = races_df.join(circuits_df, on = (races_df.circuit_id == circuits_df.circuit_id), how="full")
display(races_circuits_df.select("circuit_name"))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Semi join, anti join

# COMMAND ----------

# full outerjoin
races_circuits_df = races_df.join(circuits_df, on = (races_df.circuit_id == circuits_df.circuit_id), how="semi")
display(races_circuits_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Cross join

# COMMAND ----------

races_circuits_df = races_df.crossJoin(circuits_df)
display(races_circuits_df)