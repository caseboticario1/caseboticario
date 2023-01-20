-- Databricks notebook source
-- DBTITLE 1,Comparativo Mês Ano
SELECT
  YEAR Ano,
  MONTH Mes,
  QUANTITY_SOLD Quantidade_Vendida
FROM db_br_silver.sales_year_month
ORDER BY YEAR, MONTH

-- COMMAND ----------

-- DBTITLE 1,Quantidade Vendida Mês Por Linha
SELECT
  YEAR Ano,
  MONTH Mes,
  LINE Linha,
  QUANTITY_SOLD Quantidade_Vendida
FROM db_br_silver.sales_line_year_month
ORDER BY YEAR, MONTH

-- COMMAND ----------

SELECT
  YEAR Ano,
  MONTH Mes,
  BRAND Marca,
  QUANTITY_SOLD Quantidade_Vendida
FROM db_br_silver.sales_brand_year_month
ORDER BY YEAR, MONTH
