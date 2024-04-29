# Databricks notebook source
# MAGIC %run ../utils/adf_common_functions

# COMMAND ----------

df = spark.read.json(f"/mnt/formulaoneadf/bronze/circuits/{current_date}/circuits.json")
df.printSchema()

# COMMAND ----------

windowspec = Window.partitionBy(lit('Test')).orderBy(lit('Test'))

# COMMAND ----------

circuit_df = df.select('*',explode('MRData.CircuitTable.Circuits').alias('flatten_circuit'))\
    .select(col('flatten_circuit.circuitId').alias('circuit_ref'),
            col('flatten_circuit.circuitName').alias('circuit_name'),
            col('flatten_circuit.Location.country').alias('country'),
            col('flatten_circuit.Location.locality').alias('location'),
            col('flatten_circuit.Location.lat').alias('latitude'),
            col('flatten_circuit.Location.long').alias('longitude'))
    
circuit_df = circuit_df.select(row_number().over(windowspec).alias('circuit_id'),'*')
circuit_df.display()

# COMMAND ----------

circuit_df.write.mode("overwrite").parquet(f"/mnt/formulaoneadf/silver/circuits/{transformation_dt}/")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

spark.read.parquet(f"/mnt/formulaoneadf/silver/circuits/{transformation_dt}/").display()
