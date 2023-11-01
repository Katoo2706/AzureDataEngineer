# Databricks notebook source
from pyspark.sql.functions import current_timestamp, from_utc_timestamp
class ingestionFunction:
    def __init__(self) -> None:
        print("Using ingestion function")
        pass
    def add_ingestion_date(df):
        return df.withColumn(
    'ingestion_date', from_utc_timestamp(current_timestamp(), 'UTC')  # get current UTC timestamp
)

# COMMAND ----------

from delta.tables import DeltaTable

class ETLFunction:
    def __init__(self) -> None:
        print("Using ETL function")
        pass

    def rearrange_partition_col(self, table, partition_column: str) -> list:
        column_list = []

        for col_name in table.schema.names:
            if col_name != partition_column:
                column_list.append(col_name)

        column_list.append(partition_column)
        return table.select(column_list)
    
    def overwrite_partition(self, input_df, db_name, table_name, partition_col):
        input_df = self.rearrange_partition_col(input_df, partition_column=partition_col)
        if spark.catalog.tableExists(f"{db_name}.{table_name}") == True:
            input_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
        else:
            input_df.write.saveAsTable(f"{db_name}.{table_name}",
                                    mode="overwrite",
                                    format="delta",
                                    partitionBy=partition_col)
    
    def merge_delta_data(self, input_df, db_name: str, table_name: str, folder_path: str, merge_condition: str, partition_col: str):
        spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
        if spark.catalog.tableExists(f"{db_name}.{table_name}") == True:
            input_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
            deltaTable = DeltaTable.forPath(spark, path=folder_path)

            # Upsert
            deltaTable.alias("tgt").merge(
                input_df.alias("src"),
                merge_condition
            ).whenMatchedUpdateAll(
            ).whenNotMatchedInsertAll(
            ).execute()
        else:
            input_df.write.format("delta").saveAsTable(f"{db_name}.{table_name}",
                                            mode="overwrite",
                                            partitionBy=partition_col)