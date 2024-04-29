# Databricks notebook source
# MAGIC %md
# MAGIC #####Managed table:
# MAGIC 1. create Managed table using python
# MAGIC 2. create Managedd table using sql
# MAGIC 3. effect of dropping managed table
# MAGIC 4. describe table

# COMMAND ----------

race_result_df = spark.read.parquet("/mnt/gold/Race_result")

# COMMAND ----------

race_result_df.write.format('delta').saveAsTable("default.race_result_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table formulaone_db.race_result_table

# COMMAND ----------

# MAGIC %sql
# MAGIC use default;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended race_result_table;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.race_result_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table default.race_result_sql
# MAGIC as
# MAGIC select * from default.race_result_table
# MAGIC where race_year = 2020;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended default.race_result_sql;

# COMMAND ----------

# MAGIC %md
# MAGIC #####External table:
# MAGIC ######1. create External table using python:
# MAGIC       race_result_df.write.format("parquet").option("path", "dbfs:/mnt/gold/race_result_ext_py").saveAsTable("race_result_ext_py")
# MAGIC ######2. create External table using sql:
# MAGIC       create table race_result_ext_sql 
# MAGIC       location "/mnt/gold/race_result_ext_sql" 
# MAGIC       as 
# MAGIC       (select * from race_result_ext_py where race_year = 2020)
# MAGIC ######3. effect of dropping external table:
# MAGIC       drop table race_result_ext_sql
# MAGIC       this will drop the table metadata but, the table data remains as it is in specified external location
# MAGIC
# MAGIC ######4. describe table:
# MAGIC       

# COMMAND ----------

race_result_df.write.format("delta").option("path", "dbfs:/mnt/gold/race_result_ext_py").saveAsTable("default.race_result_ext_py")
