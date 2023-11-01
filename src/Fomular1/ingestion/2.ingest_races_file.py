# Databricks notebook source
# MAGIC %fs
# MAGIC ls /mnt/datalake01course/raw/

# COMMAND ----------

# create parameter for file date
dbutils.widgets.text("p_data_source", "")
dbutils.widgets.text("p_file_date", "")

# get the variables
v_data_source = dbutils.widgets.get("p_data_source")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

races_df = spark.read.csv(f"/mnt/datalake01course/raw/{v_file_date}/races.csv",
                          header=True)
races_df.printSchema()
display(races_df)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType

# Create schema for races dataframe
races_schema = StructType(fields=[
    StructField(name='raceId', dataType=IntegerType(), nullable=False),
    StructField(name='year', dataType=IntegerType(), nullable=False),
    StructField(name='round', dataType=IntegerType(), nullable=False),
    StructField(name='circuitId', dataType=IntegerType(), nullable=False),
    StructField(name='name', dataType=StringType(), nullable=False),
    StructField(name='date', dataType=DateType(), nullable=False),
    StructField(name='time', dataType=StringType(), nullable=False)
])

races_df = spark.read.csv(f"/mnt/datalake01course/raw/{v_file_date}/races.csv",
                          header=True,
                          schema=races_schema)

# COMMAND ----------

# Rename columns
races_df = races_df.withColumnsRenamed({
    "raceId": "race_id",
    "year": "race_year",
    "circuitId": "circuit_id",
    "time": "time"
})

# COMMAND ----------

# Add ingestion date column
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, lit, col

final_races_df = races_df.withColumn(
    colName="race_timestampt", col=to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')
    ).withColumn(colName="ingestion_date", col=current_timestamp()
    ).withColumn("data_source", lit(v_data_source)
    ).withColumn("file_date", lit(v_file_date))
    

display(final_races_df)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/datalake01course/

# COMMAND ----------

# Save to parquet
# Partitioning by year

# final_races_df.write.parquet(path="/mnt/datalake01course/processed/races",
#                              mode="overwrite",
#                              partitionBy='race_year') # partition


# # Save to parquet
# # Partitioning by year

# final_races_df.write.saveAsTable("f1_processed.races",
#                                  format="parquet",
#                                  mode="overwrite",
#                                  partitionBy="race_year") # partition

# Save as Delta table
final_races_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

display(spark.table(r"f1_processed.races"))

# COMMAND ----------

dbutils.notebook.exit("Success")