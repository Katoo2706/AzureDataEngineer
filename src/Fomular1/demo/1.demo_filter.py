# Databricks notebook source
# MAGIC %run ./../includes/config

# COMMAND ----------

# read data
races_df = spark.read.parquet(f"{processed_folder_path}/races")

# otherwise, we can use filter in read parquet data
# Spark will apply the filter predicate during the data loading process
races_df = spark.read.parquet(f"{processed_folder_path}/races",
                              filter="race_year > 2019")

# COMMAND ----------

races_df.show(4)

# COMMAND ----------

# filter by spark function
from pyspark.sql.functions import year
filter_data = races_df.filter(year(races_df["date"]) == 2020)


# filter by sql
filter_data = races_df.filter("race_year = 2020")

filter_data = races_df.filter("race_year = 2020 and round > 10")
display(filter_data)