# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "./../includes/config"

# COMMAND ----------

# MAGIC %run "./../includes/common_functions"

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results",
                                     filter=f"file_date = '{v_file_date}'")

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

driver_standing_df = race_results_df.groupBy(
    "race_year",
    "driver_name",
    "driver_nationality",
    "team"
).agg(
    sum("points").alias("total_points"),
    count(when(col("position") == 1, True)).alias("wins")
)

# COMMAND ----------

display(driver_standing_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"))

driver_rank_df = driver_standing_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

# display(driver_rank_df)
# driver_rank_df.write.parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------

# # save as drivers
# driver_rank_df.write.saveAsTable("f1_presentation.driver_standings",
#                            mode="overwrite",
#                            format="parquet")

# ETLFunction().overwrite_partition(
#     input_df=driver_rank_df,
#     db_name="f1_presentation",
#     table_name="driver_standings",
#     partition_col="race_year"
# )

merge_condition="tgt.race_year = src.race_year AND tgt.driver_name = src.driver_name"

ETLFunction().merge_delta_data(
    input_df=driver_rank_df,
    db_name="f1_presentation",
    table_name="driver_standings",
    folder_path=presentation_folder_path,
    merge_condition=merge_condition,
    partition_col="race_year"
)