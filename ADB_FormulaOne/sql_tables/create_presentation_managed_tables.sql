-- Databricks notebook source
create database if not exists f1_presentation
location "dbfs:/mnt/formulaoneadb/mng_tbl/presentation";
describe database extended f1_presentation;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/mnt/formulaoneadb/mng_tbl/presentation'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Creating a Managed Table for Presentation Files:
-- MAGIC ######We have created database with specifying the location and each ingestion notebook we have added one cell as "df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.(table_name)')" for creating a managed table through that location. 
