# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read CSV File
# MAGIC 2. Apply schema for it
# MAGIC 3. Rename and Remove column based on the requirements

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

input_schema = StructType(
    [
        StructField("constructorId", IntegerType()),
        StructField("constructorRef", StringType()),
        StructField("name", StringType()),
        StructField("nationality", StringType()),
        StructField("url", StringType()),  
    ]
)

# COMMAND ----------

df = spark.read.json("/mnt/formulaoneadb/raw/constructors.json", input_schema)
df.display()

# COMMAND ----------

# Rename column and add New column
df = df.withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("constructorRef", "constructor_ref").withColumn("ingest_dt", lit(current_date()))
df.display()

# COMMAND ----------

# df.write.mode('overwrite').parquet('/mnt/formulaoneadb/processed/constructors')

# COMMAND ----------

# dbutils.notebook.exit("success")

# COMMAND ----------

df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.constructors_tbl')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors_tbl;
