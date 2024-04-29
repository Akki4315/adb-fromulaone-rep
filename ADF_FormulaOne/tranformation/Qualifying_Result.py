# Databricks notebook source
# MAGIC %run ../utils/adf_common_functions

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/formulaoneadf/silver/"))

# COMMAND ----------

driver_df = spark.read.parquet("/mnt/formulaoneadf/silver/drivers/20240428/")
constructor_df = spark.read.parquet("/mnt/formulaoneadf/silver/constructors/20240428/")
qualify_df = spark.read.parquet(f"/mnt/formulaoneadf/silver/qualify/{transformation_dt}/")
race_df = spark.read.parquet(f"/mnt/formulaoneadf/silver/race/{transformation_dt}/")
circuit_df = spark.read.parquet("/mnt/formulaoneadf/silver/circuits/20240428/")

# COMMAND ----------

race_circuit_df = race_df.join(circuit_df,race_df.circuit_id==circuit_df.circuit_ref, 'inner').select('circuit_ref','race_date', 'race_name','season','location')

# COMMAND ----------

quali_driver_df = qualify_df.join(driver_df, qualify_df.driver_id == driver_df.driver_ref, 'inner')\
    .join(constructor_df, qualify_df.constructor_id == constructor_df.constructor_ref).select('race_date','qualifying_1','qualifying_2', 'qualifying_3', 'driver_no', 'driver_name', driver_df.driver_nationality, 'team_name')

# COMMAND ----------

quali_result_df = race_circuit_df.join(quali_driver_df, 'race_date', 'inner')\
    .select('season','race_name', 'race_date', 'location', 'driver_nationality', 'driver_name','driver_no', 'team_name', 'qualifying_1','qualifying_2','qualifying_3')
    
quali_result_df.count()

# COMMAND ----------

quali_result_df.filter('season = 2024 and race_name = "Chinese Grand Prix"').display()

# COMMAND ----------

quali_driver_df.write.mode('overwrite').parquet(f"/mnt/formulaoneadf/gold/Qualifying_Result/{transformation_dt}/")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

spark.read.parquet(f"/mnt/formulaoneadf/gold/Qualifying_Result/{transformation_dt}/").count()
