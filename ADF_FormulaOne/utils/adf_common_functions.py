# Databricks notebook source
from pyspark.sql.functions import explode, col, current_date, lit, row_number, desc, monotonically_increasing_id, concat, dense_rank, sum, count, when, rank, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, ArrayType, MapType
from datetime import datetime
from pyspark.sql.window import Window

# COMMAND ----------

transformation_date = datetime.now().strftime('%Y%m%d')
dbutils.widgets.text('transformation_dt', transformation_date)
transformation_dt = dbutils.widgets.get('transformation_dt')

# COMMAND ----------

dbutils.widgets.text('run_dt', '')
run_dt = dbutils.widgets.get('run_dt')

# COMMAND ----------

from datetime import datetime

if run_dt:
    current_date = run_dt
else:
    run_dt = datetime.now().strftime("%Y%m%d")
