# Databricks notebook source
dbutils.widgets.text("file_date","2021-03-21")
file_dt = dbutils.widgets.get("file_date")

# COMMAND ----------

file_dt

# COMMAND ----------

dbutils.notebook.run("Qualifying_Result",0,{"file_date": file_dt})

# COMMAND ----------

dbutils.notebook.run("Race_Result",0,{"file_date": file_dt})

# COMMAND ----------

dbutils.notebook.run("Driver_Standing_Result",0,{"file_date": file_dt})

# COMMAND ----------

dbutils.notebook.run("Constructor_Standing_Result",0,{"file_date": file_dt})
