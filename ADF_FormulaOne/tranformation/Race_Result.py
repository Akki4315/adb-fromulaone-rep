# Databricks notebook source
# MAGIC %run ../utils/adf_common_functions

# COMMAND ----------

driver_df = spark.read.parquet(f"/mnt/formulaoneadf/silver/drivers/20240428/").drop('driver_dob').withColumnRenamed('driver_ref','driver_id')
constructor_df = spark.read.parquet(f"/mnt/formulaoneadf/silver/constructors/20240428/").drop('driver_nationality').withColumnRenamed('constructor_ref','constructor_id')
race_df = spark.read.parquet(f"/mnt/formulaoneadf/silver/race/{transformation_date}/")
circuit_df = spark.read.parquet(f"/mnt/formulaoneadf/silver/circuits/20240428/")
pitstop_df = spark.read.parquet(f"/mnt/formulaoneadf/silver/pitstops/{transformation_date}/").select('driver_id',col('stop').alias('Pits'))
result_df = spark.read.parquet(f"/mnt/formulaoneadf/silver/results/{transformation_date}/")
result_df.count()

# COMMAND ----------

race_circuit_df = race_df.join(circuit_df,race_df.circuit_id==circuit_df.circuit_ref, 'inner').select(race_df.circuit_id,'race_date', 'race_name','season','location')
race_circuit_df.count()

# COMMAND ----------

#.join(pitstop_df, 'driver_id', 'inner')\

# COMMAND ----------

result_race_df = result_df.join(race_circuit_df, 'race_date', 'inner')\
    .join(driver_df, 'driver_id', 'inner')\
        .join(constructor_df, 'constructor_id', 'inner')\
                .select('season','race_name','race_date','location','driver_nationality','driver_name','driver_no','team_name','grid','fast_lap','race_time','points','result_position')

result_race_df.count()

# COMMAND ----------

result_race_df.filter('season = 2024 and race_name = "Chinese Grand Prix"').display()

# COMMAND ----------

result_race_df.write.mode('overwrite').parquet(f"/mnt/formulaoneadf/gold/Race_Result/{transformation_dt}/")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

spark.read.parquet(f"/mnt/formulaoneadf/gold/Race_Result/{transformation_dt}").count()
