# Databricks notebook source
# DBTITLE 1,Import Pandas
import pandas

# COMMAND ----------

# DBTITLE 1,Set Environment Variables
source_path = "/dbfs/FileStore/tables/"
target_db = "db_br_bronze"

# COMMAND ----------

# DBTITLE 1,Sales Charge
source_file1 = "Base2017.xlsx"
source_file2 = "Base2018.xlsx"
source_file3 = "Base2019.xlsx"
target_table = "sales"

pdf1 = pandas.read_excel(source_path + source_file1)
pdf2 = pandas.read_excel(source_path + source_file2)
pdf3 = pandas.read_excel(source_path + source_file3)

df_target_table = spark.createDataFrame(pdf1).union(spark.createDataFrame(pdf2)).union(spark.createDataFrame(pdf3))

df_target_table.write.format("delta").mode("overwrite").saveAsTable(target_db + '.' + target_table, mergeSchema=True, overwriteSchema=True)

#df_target_table.display()

# COMMAND ----------


