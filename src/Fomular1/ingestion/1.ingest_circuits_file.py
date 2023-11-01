# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

# create parameter for file date
dbutils.widgets.text("p_data_source", "")
dbutils.widgets.text("p_file_date", "")

# get the variables
v_data_source = dbutils.widgets.get("p_data_source")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# Command to run config file

# COMMAND ----------

# MAGIC %run "./../includes/config"

# COMMAND ----------

# MAGIC %run "./../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

# After mounting to ADLS containers
display(dbutils.fs.mounts())

# get file name
file_list = dbutils.fs.ls("/mnt/datalake01course/raw") # raw_folder_path

# COMMAND ----------

for file in file_list:
    print(file.path + " | " + file.name)

# COMMAND ----------

circuits_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv", 
                            header='true',
                            inferSchema='true') # 
display(circuits_df)

# COMMAND ----------

# information about data in Dataframe
circuits_df.printSchema()
circuits_df.describe().show()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Create variable that contain schema
circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False), # False - Nullable or not
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", StringType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", DoubleType(), True),
    StructField("url", StringType(), True),               
])

# Now we can you that schema to read the data
circuits_df =spark.read.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv", 
                            header='true',
                            # inferSchema='true',
                            schema=circuits_schema # pyspark.sql.types.StructType or str, optional 
                            )
display(circuits_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Select & drop columns

# COMMAND ----------

circuits_df[['circuitId', 'country']]
circuits_df.select('circuitId', 'country')

from pyspark.sql.functions import col, lit
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# Drop column
circuits_df.drop('country','lat')

# COMMAND ----------

# Rename the column
circuits_renamed_df = circuits_df.withColumnRenamed("circuitId", "circuit_id")\
    .withColumnRenamed("circuitRef", "circuit_ref")\
    .withColumnRenamed("lat", "latitude")\
    .withColumnRenamed("lng", "longitude")\
    .withColumnRenamed("alt", "altitude")\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Add ingestion date to the dataframe
# MAGIC - Add mutil columns: withColumns({'age2': df.age + 2, 'age3': df.age + 3}).show()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

circuits_final_df = ingestionFunction.add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumns({"ingestion_date": current_timestamp(),
                                                    "env": lit("Production")})

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data to datalake as parquet / delta lake

# COMMAND ----------

# rm -r /mnt/fomula1

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/

# COMMAND ----------

display(dbutils.fs.ls("/mnt/datalake01course/"))

# COMMAND ----------

# circuits_final_df.write.parquet(path=f"{processed_folder_path}/circuits",
#                                 mode="overwrite").saveAsTable("f1_processed.circuits")

# Create parquet managed table using pyspark
# circuits_final_df.write.saveAsTable("f1_processed.circuits",
#                                     mode="overwrite",
#                                     format="parquet")

# Create delta managed table using pyspark
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/datalake01course/processed/circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM f1_processed.circuits;

# COMMAND ----------

df = spark.read.format("delta").load("/mnt/datalake01course/processed/circuits/")
display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")