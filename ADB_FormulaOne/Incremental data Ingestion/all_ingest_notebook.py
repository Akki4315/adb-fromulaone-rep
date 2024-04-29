# Databricks notebook source
dbutils.widgets.text("file_date", "")
file_dt = dbutils.widgets.get("file_date")

# COMMAND ----------

file_dt

# COMMAND ----------

dbutils.notebook.run('Circuits_ingest',0,{"file_date": file_dt , "source_name":"circuits"})

# COMMAND ----------

dbutils.notebook.run('Constructors_ingest',0, {"file_date": file_dt , "source_name":"constructors"})

# COMMAND ----------

dbutils.notebook.run('Drivers_ingest',0, {"file_date": file_dt , "source_name":"drivers"})

# COMMAND ----------

dbutils.notebook.run('Lap_Times_ingest',0, {"file_date": file_dt , "source_name":"lap_times"})

# COMMAND ----------

dbutils.notebook.run('Pitstops_ingest',0, {"file_date": file_dt , "source_name":"pit_stops"})

# COMMAND ----------

dbutils.notebook.run('Qualifying_ingest',0, {"file_date": file_dt , "source_name":"qualifying"})

# COMMAND ----------

dbutils.notebook.run('Race_ingest',0, {"file_date": file_dt , "source_name":"races"})

# COMMAND ----------

dbutils.notebook.run('Results_ingest',0, {"file_date": file_dt , "source_name":"results"})
