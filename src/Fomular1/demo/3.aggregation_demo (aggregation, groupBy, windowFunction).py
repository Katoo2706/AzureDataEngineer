# Databricks notebook source
# MAGIC %run ./../includes/config

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregation demo, using sql.functions as select.

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

demo_df = race_results_df.filter("race_year = 2020")

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

# show number of records which is not null
demo_df.select(count("*")).show()

# COMMAND ----------

# use sum, count, aggregation function as select in sql
# Use alias as column name
demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points"), countDistinct("driver_name").alias("Unique drivers"), sum("points")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Group by

# COMMAND ----------

demo_df.groupBy("driver_name").sum("points").show()

# COMMAND ----------

demo_df.groupBy("driver_name").agg({
    "points": "sum",
    "race_name": "count"
}).show(3)

# or we can use other way
demo_df.groupBy("driver_name").agg(
    sum("points").alias("Sum of point"),
    countDistinct("race_name")
).show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Window Functions

# COMMAND ----------

demo_df = race_results_df.filter("race_year between 2019 and 2020")

# COMMAND ----------

demo_grouped_df = demo_df.groupBy("race_year", "driver_name").agg(
    sum("points").alias("total_points"),
    countDistinct("race_name").alias("number_of_races")
)
display(demo_grouped_df.head(4))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# get the Window spec
driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_grouped_df.withColumn("rank", rank().over(driverRankSpec)).show()

# COMMAND ----------

