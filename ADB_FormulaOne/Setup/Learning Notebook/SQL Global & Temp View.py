# Databricks notebook source
# MAGIC %md
# MAGIC #### SQL Global & Temp View Creation and Their Uses:
# MAGIC #####TempView:
# MAGIC 1. create tempview on top of dataframe by using createTempView or createOrReplaceTempViewb(recommanded to avoid exception) command.
# MAGIC 2. temp view is valid only for the perticular spark session (i.e. perticular notbook only). once the session is off (cluster is terminated). view will be vanished. 
# MAGIC 3. view can be use by sql as well as python cell both. generally when we want to do some tranformation or pass some varible in runtime 
# MAGIC on dataframe that time python cell is used.
# MAGIC

# COMMAND ----------

race_result_df = spark.read.parquet("/mnt/gold/Race_result")

# COMMAND ----------

race_result_df.createOrReplaceTempView("v_race_result")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_result

# COMMAND ----------

spark.sql("select * from v_race_result where race_year = 2020").display()

# COMMAND ----------

year = 2016
spark.sql(f"select * from v_race_result where race_year = {year}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####GlobalView:
# MAGIC 1. create global view on top of dataframe by using createGlobalTempView or createOrReplaceGlobalTempViewb(recommanded to avoid exception) command.
# MAGIC 2. Global view is valid for spark application( i.e. in all the notebook created inside spark application) but, once the session is off (cluster is terminated). global view will also get deleted. 
# MAGIC 3. to use of globle view in sql as well as python cell, we need to define the database name along with view name eg(global_temp.gv_race_result). only then it will display the output.

# COMMAND ----------

race_result_df.createOrReplaceGlobalTempView("gv_race_result")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_result

# COMMAND ----------

spark.sql("select * from global_temp.gv_race_result").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Create a View by using SQL query:
# MAGIC 1. Temp View - same work as explain above
# MAGIC 2. Global View - same work as explain above
# MAGIC 3. Permanent View - this view is permanentyly stores and we can access it any time. it won't be vanish when the spark session ends.

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view v_race_result
# MAGIC as 
# MAGIC select * from default.race_result_table

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view gv_race_result
# MAGIC as 
# MAGIC select * from default.race_result_table
# MAGIC where race_year = 2012;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view pv_race_result
# MAGIC as 
# MAGIC select * from default.race_result_table
# MAGIC where race_year = 2015;
