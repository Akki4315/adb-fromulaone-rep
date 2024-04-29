# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read JSON File
# MAGIC 2. Apply schema for it
# MAGIC 3. Rename and Remove column based on the requirements

# COMMAND ----------

dbutils.widgets.text("source_name", "")
source_name = dbutils.widgets.get("source_name")

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-21")
file_dt = dbutils.widgets.get("file_date")

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

df = spark.read.json(f"/mnt/raw/{file_dt}/{source_name}.json", input_schema)
df.display()

# COMMAND ----------

# Rename column and add New column
df = df.withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("constructorRef", "constructor_ref").withColumn("ingest_dt", lit(current_date())).withColumn("file_date", lit(file_dt))
df.display()

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructor_delta_tbl")

# COMMAND ----------

# increment_data(df, "constructor_id", "f1_processed", "constructors_tbl")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select constructor_id, count(1) from f1_processed.constructor_delta_tbl
# MAGIC group by constructor_id
# MAGIC order by constructor_id desc;
