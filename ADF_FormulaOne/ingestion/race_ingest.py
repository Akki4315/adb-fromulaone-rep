# Databricks notebook source
# MAGIC %run ../utils/adf_common_functions

# COMMAND ----------

df = spark.read.json("/mnt/formulaoneadf/bronze/races/20240427/")
df.printSchema()

# COMMAND ----------

windowspec = Window.partitionBy(lit('Test')).orderBy(desc('season'))

# COMMAND ----------

race_df = df.select('*',explode('MRData.RaceTable.Races').alias('flatten_races'))\
    .select(col('flatten_races.date').alias('race_date'),
            col('flatten_races.raceName').alias('race_name'),
            col('flatten_races.round').alias('round'),
            col('flatten_races.season').alias('season'),
            col('flatten_races.time').alias('race_time'),
            col('flatten_races.Circuit.circuitId').alias('circuit_id')
            )
race_df = race_df.withColumn('race_id', row_number().over(windowspec))\
    .select('race_id', 'circuit_id', 'race_date', 'race_time', 'race_name', 'round', 'season')
race_df.display()

# COMMAND ----------

race_df.write.mode("overwrite").parquet(f"/mnt/formulaoneadf/silver/race/{transformation_dt}/")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

spark.read.parquet(f"/mnt/formulaoneadf/silver/race/{transformation_dt}/").display()
