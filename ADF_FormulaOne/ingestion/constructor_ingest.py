# Databricks notebook source
# MAGIC %run ../utils/adf_common_functions

# COMMAND ----------

df = spark.read.json("/mnt/formulaoneadf/bronze/constructors/20240427/")
df.printSchema()

# COMMAND ----------

constructor_df = df.select( explode('MRData.ConstructorTable.Constructors').alias('flatten_cons'))\
              .select(col('flatten_cons.constructorId').alias('constructor_ref'),
                      col('flatten_cons.name').alias('team_name'),
                      col('flatten_cons.nationality').alias('driver_nationality'))

constructor_df.display()

# COMMAND ----------

constructor_df.write.mode("overwrite").parquet(f"/mnt/formulaoneadf/silver/constructors/{transformation_dt}/")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

spark.read.parquet(f"/mnt/adfformulaone/silver/constructors/{transformation_dt}/").display()
