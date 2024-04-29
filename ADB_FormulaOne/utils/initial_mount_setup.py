# Databricks notebook source
# MAGIC %md
# MAGIC ##### Create Mount Point ADLS to ADB

# COMMAND ----------

dbutils.widgets.text("layer_name","bronze")
layer_name = dbutils.widgets.get("layer_name")

# COMMAND ----------

print(layer_name)

# COMMAND ----------

dbutils.secrets.list("bwt session")

# COMMAND ----------

application_id = dbutils.secrets.get("bwt session","kv-formulaoneproject-application-id")
directory_id = dbutils.secrets.get("bwt session","kv-formulaoneproject-directory-id")
service_credential = dbutils.secrets.get("bwt session","kv-formulaoneproject-service-credential")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": service_credential,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{layer_name}@formulaoneproject1.dfs.core.windows.net/",
  mount_point = f"/mnt/{layer_name}",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())
