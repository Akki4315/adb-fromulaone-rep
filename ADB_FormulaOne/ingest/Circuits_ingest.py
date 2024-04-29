# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read CSV File
# MAGIC 2. Apply schema for it
# MAGIC 3. Rename and Remove column based on the requirements

# COMMAND ----------

dbutils.widgets.text("source_name", "circuits")
source_name = dbutils.widgets.get("source_name")

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

input_schema = StructType(
    [
        StructField("circuitId", IntegerType()),
        StructField("circuitRef", StringType()),
        StructField("name", StringType()),
        StructField("location", StringType()),
        StructField("country", StringType()),
        StructField("lat", FloatType()),
        StructField("lng", FloatType()),
        StructField("alt", IntegerType()),
        StructField("url", StringType()),
    ]
)

# COMMAND ----------

df = create_csv_df(f"/mnt/formulaoneadb/raw/{source_name}.csv", input_schema)
df.display()

# COMMAND ----------

# Rename column and add New column
df = df.withColumnRenamed("circuitId", "circuit_id").withColumnRenamed("circuitRef", "circuit_ref").withColumn("ingest_dt", lit(current_date()))
df.display()

# COMMAND ----------

# df.write.mode('overwrite').parquet(f'/mnt/formulaoneadb/processed/{source_name}')

# COMMAND ----------

# dbutils.notebook.exit("success")

# COMMAND ----------

df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.circuits_tbl')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits_tbl;
