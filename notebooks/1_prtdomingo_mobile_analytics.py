# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables

# COMMAND ----------

# File location and type
file_location = "FileStore/tables/Analytics_prtdomingo_blog_Devices-e2245.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

columns_to_drop = [ "_c" + str(column_name) for column_name in range(10,24)]

df = df.drop(*columns_to_drop) # remove unnecessary columns
df = df.na.drop(subset=["Mobile Device Info"]) # drop rows with null value

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "Analytics_prtdomingo_blog_Devices"

df.createOrReplaceTempView(temp_table_name)


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select `Mobile Device Info`, Users from `Analytics_prtdomingo_blog_Devices`

# COMMAND ----------

permanent_table_name = "analytics_prtdomingo_blog_devices_e2245_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)