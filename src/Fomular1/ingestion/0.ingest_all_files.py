# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

# Run all the notebook
v_result = dbutils.notebook.run("1.ingest_circuits_file",
                     0, # timeout_seconds
                     {"p_data_source": "Ergast API",
                      "p_file_date": "2021-03-21"} # params
) 

v_result

# COMMAND ----------

# Run all the notebook
v_result = dbutils.notebook.run("2.ingest_races_file",
                     0, # timeout_seconds
                     {"p_data_source": "Ergast API",
                      "p_file_date": "2021-03-21"} # params
) 

v_result

# COMMAND ----------

# Run all the notebook
v_result = dbutils.notebook.run("3.ingest_constructors_file",
                     0, # timeout_seconds
                     {"p_data_source": "Ergast API",
                      "p_file_date": "2021-03-21"} # params
) 

v_result

# COMMAND ----------

# Run all the notebook
v_result = dbutils.notebook.run("4.ingest_drivers_file",
                     0, # timeout_seconds
                     {"p_data_source": "Ergast API",
                      "p_file_date": "2021-03-21"} # params
) 

v_result

# COMMAND ----------

# Run all the notebook
v_result = dbutils.notebook.run("5.ingest_results_file",
                     0, # timeout_seconds
                     {"p_data_source": "Ergast API",
                      "p_file_date": "2021-03-21"} # params
) 

v_result

# COMMAND ----------

# Run all the notebook
v_result = dbutils.notebook.run("6.ingest_pit_stops_file",
                     0, # timeout_seconds
                     {"p_data_source": "Ergast API",
                      "p_file_date": "2021-03-21"} # params
) 

v_result

# COMMAND ----------

# Run all the notebook
v_result = dbutils.notebook.run("7.ingest_lap_times_file",
                     0, # timeout_seconds
                     {"p_data_source": "Ergast API",
                      "p_file_date": "2021-03-21"} # params
) 

v_result

# COMMAND ----------

# Run all the notebook
v_result = dbutils.notebook.run("8.ingest_qualifying_file",
                     0, # timeout_seconds
                     {"p_data_source": "Ergast API",
                      "p_file_date": "2021-03-21"} # params
) 

v_result