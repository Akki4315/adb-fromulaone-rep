# Databricks notebook source
dbutils.notebook.run('Circuits_ingest',0,{"source_name":"circuits"})

# COMMAND ----------

dbutils.notebook.run('Constructors_ingest',0)

# COMMAND ----------

dbutils.notebook.run('Drivers_ingest',0)

# COMMAND ----------

dbutils.notebook.run('Lap_Times_ingest',0)

# COMMAND ----------

dbutils.notebook.run('Pitstops_ingest',0)

# COMMAND ----------

dbutils.notebook.run('Qualifying_ingest',0)

# COMMAND ----------

dbutils.notebook.run('Race_ingest',0)

# COMMAND ----------

dbutils.notebook.run('Results_ingest',0)
