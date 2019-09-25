# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC #### How to read data from Datalake using Databricks
# MAGIC 
# MAGIC Reference Links:
# MAGIC   1. Azure Data Lake Gen1: https://docs.databricks.com/spark/latest/data-sources/azure/azure-datalake.html
# MAGIC   2. Azure Data Lake Gen2: https://docs.databricks.com/spark/latest/data-sources/azure/azure-datalake-gen2.html
# MAGIC   3. Create Azure AD Application: https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Prerequisite: Create Azure AD Application (you can use the reference provided above):
# MAGIC 
# MAGIC 1. Go to Azure Portal (portal.azure.com)
# MAGIC 2. Go to Azure Active Directory:
# MAGIC 
# MAGIC   ![alt text](https://github.com/prtdomingo/DatabricksDatalake/raw/master/docs-assets/step1-1.png "")
# MAGIC   
# MAGIC 3. Navigate to **App registrations**
# MAGIC 4. Click **Register an application**
# MAGIC   
# MAGIC   From the form, just fill up the **Name** and you can leave everything else as default, and proceed to **Register**
# MAGIC   ![alt text](https://github.com/prtdomingo/DatabricksDatalake/raw/master/docs-assets/step1-2.png "")
# MAGIC 
# MAGIC 5. Once the application is created, please take note of the value of both **Application (client) ID** and **Directory (tenant) ID**
# MAGIC 
# MAGIC   ![alt text](https://github.com/prtdomingo/DatabricksDatalake/raw/master/docs-assets/step1-3.png "")
# MAGIC   
# MAGIC 6. Navigate to **Certificates & secrets** and click **+ New client secret**
# MAGIC 7. Add a description for the client secret and click **Add**
# MAGIC 
# MAGIC   ![alt text](https://github.com/prtdomingo/DatabricksDatalake/raw/master/docs-assets/step1-4.png "")
# MAGIC   
# MAGIC 8. Take note of the **VALUE** that will be generated in Client secrets

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### Access Data Lake Gen1 Data from Databricks - Grant Access Control on the Active Directory Application to Azure Data Lake Gen1
# MAGIC 
# MAGIC 1. Before we proceed on the coding part on how to access Data Lake Gen1 in Databricks, from the Azure Portal, navigate to the Azure Data Lake Gen1 you provisioned
# MAGIC 2. Go to **Access Control (IAM)**
# MAGIC 3. Click **+ Add** and **Add role assignments**
# MAGIC 4. From the form, include the following details:
# MAGIC     - Role: Contributor
# MAGIC     - Assign acess to: leave it default
# MAGIC     - Select: type the **Active Directory Application** we've created on the previous step
# MAGIC     
# MAGIC     ![alt text](https://github.com/prtdomingo/DatabricksDatalake/raw/master/docs-assets/step2-1.png "")
# MAGIC     
# MAGIC 5. Press **Save**
# MAGIC 6. Once all is successfully setup, write the following code (**NOTE:** For this demo I'll be using a databricks parameter so I don't have to hardcode my secret keys on my source control):

# COMMAND ----------

dbutils.widgets.text("client_id", "", "Application (client) ID")
dbutils.widgets.text("client_secret", "", "Client Secret")
dbutils.widgets.text("directory_id", "", "Directory (tenant) ID")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###### Databricks Parameter
# MAGIC 
# MAGIC After running Cell #5, you should be able to see something similar in your Databricks Notebook view - this is where you will be pasting the values required for accessing Data Lake:
# MAGIC 
# MAGIC ![alt text](https://github.com/prtdomingo/DatabricksDatalake/raw/master/docs-assets/step2-2.png "")

# COMMAND ----------

# Configure Spark to use Service Credentials in Azure

client_id = dbutils.widgets.get("client_id")
client_secret = dbutils.widgets.get("client_secret")
directory_id = dbutils.widgets.get("directory_id")

spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", client_id)
spark.conf.set("dfs.adls.oauth2.credential", client_secret)
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/" + directory_id + "/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###### Check if you can successfully access your ADLS Gen1
# MAGIC 
# MAGIC Replace the value of **DATALAKE_GEN1_NAME** with your own Data Lake Gen1

# COMMAND ----------

# MAGIC %fs ls adl://DATALAKE_GEN1_NAME.azuredatalakestore.net/

# COMMAND ----------

# MAGIC %r 
# MAGIC 
# MAGIC library(SparkR)
# MAGIC 
# MAGIC df <- read.text("adl://DATALAKE_GEN1_NAME.azuredatalakestore.net/PATH_TO_TEXT_FILE")
# MAGIC 
# MAGIC ## Another Example on how to read a file from Data Lake with CSV  
# MAGIC 
# MAGIC #df <- read.df("adl://DATALAKE_GEN1_NAME.azuredatalakestore.net/PATH_TO_CSV", source = "csv", header="true", inferSchema = "true")
# MAGIC 
# MAGIC display(df)