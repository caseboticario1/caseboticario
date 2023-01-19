# Databricks notebook source
import matplotlib.pyplot as plt
import numpy as np
import pandas

# COMMAND ----------

df_target_table = spark.sql(f'''
  SELECT
      YEAR||'-'||MONTH YEAR_MONTH,
      QUANTITY_SOLD,
      (SELECT SUM(QUANTITY_SOLD) TOTAL FROM db_br_silver.sales_year_month WHERE YEAR=2019) TOTAL,
      QUANTITY_SOLD/(SELECT SUM(QUANTITY_SOLD) TOTAL FROM db_br_silver.sales_year_month WHERE YEAR=2019)*100 PERCENT
    FROM db_br_silver.sales_year_month
    WHERE YEAR=2019
    ORDER BY MONTH ASC
  ''')

pdf = df_target_table.toPandas()
pdf

# COMMAND ----------

names = pdf['YEAR_MONTH']
values = pdf['QUANTITY_SOLD']
percent = pdf['PERCENT']



plt.figure(figsize=(50, 5))

plt.subplot(131)
plt.bar(names, values)
plt.subplot(132)
plt.plot(names, percent)

plt.show()
