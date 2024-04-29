# Databricks notebook source
dbutils.secrets.list("key-vault-secrets")

# COMMAND ----------

application_id = dbutils.secrets.get("key-vault-secrets","kv-formulaoneproject-application-id")
directory_id = dbutils.secrets.get("key-vault-secrets","kv-formulaoneproject-directory-id")
service_credential = dbutils.secrets.get("key-vault-secrets","kv-formulaoneproject-service-credential")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": service_credential,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://formulaoneadb@f1adlsgen2sa.dfs.core.windows.net/",
  mount_point = "/mnt/formulaoneadb",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls('/mnt/formulaoneadb/raw'))
