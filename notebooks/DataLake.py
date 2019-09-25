# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC #### How to read (and write) data from Datalake using Databricks
# MAGIC 
# MAGIC Reference Links:
# MAGIC   1. Azure Data Lake Gen1: https://docs.databricks.com/spark/latest/data-sources/azure/azure-datalake.html
# MAGIC   2. Azure Data Lake Gen2: https://docs.databricks.com/spark/latest/data-sources/azure/azure-datalake-gen2.html
# MAGIC   3. Create Azure AD Application: https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Step 1: Create Azure AD Application:
# MAGIC 
# MAGIC 1. Go to Azure Portal (portal.azure.com)
# MAGIC 2. Go to Azure Active Directory:
# MAGIC 
# MAGIC   ![alt text](https://github.com/prtdomingo/DatabricksDatalake/raw/master/docs-assets/step1-1.png "")

# COMMAND ----------

