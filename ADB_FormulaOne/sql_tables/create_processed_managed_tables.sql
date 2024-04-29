-- Databricks notebook source
drop database if exists f1_processed;
create database if not exists f1_processed
location "dbfs:/mnt/formulaoneadb/mng_tbl/processed";
describe database extended f1_processed;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/mnt/formulaoneadb/mng_tbl/processed'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Creating a Managed Table for Processed Files:
-- MAGIC ######We have created database with specifying the location and each ingestion notebook we have added one cell as "df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.<table_name>')" for creating a managed table through that location. 
