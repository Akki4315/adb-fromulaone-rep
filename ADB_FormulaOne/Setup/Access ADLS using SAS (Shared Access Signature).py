# Databricks notebook source
# MAGIC %md
# MAGIC ####Accessing ADLS from Databricks using SAS (Shared Access Signature)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### to generate sas token go to storage account-->container--> select Shared Access Tokens--> fill all the fileds and click on generate sas token and url. copy the sas token and paste it into below variable. (Note: don't close the sas window)

# COMMAND ----------

sas_token = dbutils.secrets.get("bwt session", "kv-formulaoneproject-SAS-Token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formulaoneproject1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formulaoneproject1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formulaoneproject1.dfs.core.windows.net",f"{sas_token}")

# COMMAND ----------

#dbutils.fs.ls("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>")
display(dbutils.fs.ls("abfss://test@formulaoneproject1.dfs.core.windows.net/input_data/"))

# COMMAND ----------


