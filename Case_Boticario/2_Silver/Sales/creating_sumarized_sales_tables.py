# Databricks notebook source
# DBTITLE 1,Set Environment Variables
source_db = "db_br_bronze"
target_db = "db_br_silver"

# COMMAND ----------

# DBTITLE 1,Creation of sumarized sales table YEAR and MONTH
target_table = "sales_year_month"

df_target_table = spark.sql(f'''
  SELECT
      YEAR(DATA_VENDA) AS YEAR,
      MONTH(DATA_VENDA) AS MONTH,
      SUM(QTD_VENDA) QUANTITY_SOLD
    FROM {source_db}.sales
    GROUP BY 
      YEAR(DATA_VENDA),
      MONTH(DATA_VENDA)
  ''')

df_target_table.write.format("delta").mode("overwrite").saveAsTable(target_db + '.' + target_table, mergeSchema=True, overwriteSchema=True)

#display(df_target_table) 

# COMMAND ----------

# DBTITLE 1,Creation of sumarized sales table BRAND and LINE
target_table = "sales_brand_line"

df_target_table = spark.sql(f'''
  SELECT
      MARCA AS BRAND,
      LINHA AS LINE,
      SUM(QTD_VENDA) AS QUANTITY_SOLD
    FROM {source_db}.sales
    GROUP BY 
      MARCA,
      LINHA
  ''')

df_target_table.write.format("delta").mode("overwrite").saveAsTable(target_db + '.' + target_table, mergeSchema=True, overwriteSchema=True)

#display(df_target_table) 

# COMMAND ----------

# DBTITLE 1,Creation of sumarized sales table BRAND, YEAR and MONTH
target_table = "sales_brand_year_month"

df_target_table = spark.sql(f'''
  SELECT
      MARCA AS BRAND,
      YEAR(DATA_VENDA) AS YEAR,
      MONTH(DATA_VENDA) AS MONTH,
      SUM(QTD_VENDA) AS QUANTITY_SOLD
    FROM {source_db}.sales
    GROUP BY 
      MARCA,
      YEAR(DATA_VENDA),
      MONTH(DATA_VENDA)
  ''')

df_target_table.write.format("delta").mode("overwrite").saveAsTable(target_db + '.' + target_table, mergeSchema=True, overwriteSchema=True)

#display(df_target_table) 

# COMMAND ----------

# DBTITLE 1,Creation of sumarized sales table LINE, YEAR and MONTH
target_table = "sales_line_year_month"

df_target_table = spark.sql(f'''
  SELECT
      LINHA AS LINE,
      YEAR(DATA_VENDA) AS YEAR,
      MONTH(DATA_VENDA) AS MONTH,
      SUM(QTD_VENDA) AS QUANTITY_SOLD
    FROM {source_db}.sales
    GROUP BY 
      LINHA,
      YEAR(DATA_VENDA),
      MONTH(DATA_VENDA)
  ''')

df_target_table.write.format("delta").mode("overwrite").saveAsTable(target_db + '.' + target_table, mergeSchema=True, overwriteSchema=True)

#display(df_target_table) 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC       *
# MAGIC       
# MAGIC  FROM db_br_silver.sales_line_year_month
