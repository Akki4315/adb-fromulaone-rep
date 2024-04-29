# Databricks notebook source
# MAGIC %run ../utils/adf_common_functions

# COMMAND ----------

df = spark.read.json("/mnt/formulaoneadf/bronze/qualifying/20240429/", multiLine=True)
df.printSchema()

# COMMAND ----------

qualify_df = df.select('*', explode('MRData.RaceTable.Races').alias('flatten_races'))\
              .select('*', explode('flatten_races.QualifyingResults').alias('flatten_qualify'))\
              .select(col('flatten_qualify.Driver.driverId').alias('driver_id'),
                      col('flatten_races.date').alias('race_date'),
                      col('flatten_qualify.Constructor.constructorId').alias('constructor_id'),
                      col('flatten_qualify.number').alias('number'),
                      col('flatten_qualify.position').alias('position'),
                      col('flatten_qualify.q1').alias('qualifying_1'),
                      col('flatten_qualify.q2').alias('qualifying_2'),
                      col('flatten_qualify.q3').alias('qualifying_3'))
qualify_df.count()

# COMMAND ----------

qualify_df.write.mode("overwrite").parquet(f"/mnt/formulaoneadf/silver/qualify/{transformation_dt}/")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

spark.read.parquet(f"/mnt/formulaoneadf/silver/qualify/{transformation_dt}/").count()
