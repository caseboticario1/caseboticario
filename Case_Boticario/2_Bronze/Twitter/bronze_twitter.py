# Databricks notebook source
# DBTITLE 1,Import Pandas/Tweepy/Json
import pandas
import tweepy as tw
import json

# COMMAND ----------

# DBTITLE 1,Set Environment Variables and Load Token Twitter
target_db = "db_br_bronze"

f = open('/dbfs/FileStore/tables/token_twitter.json')
  
token_twitter = json.load(f)

# COMMAND ----------

# DBTITLE 1,API Authorization
authorization = tw.OAuth1UserHandler(token_twitter['twitter']['consumer_key'], token_twitter['twitter']['consumer_secret'])

authorization.set_access_token(token_twitter['twitter']['access_token'], token_twitter['twitter']['access_token_secret'])

tw_api = tw.API(authorization)

# COMMAND ----------

# DBTITLE 1,Set Query Keys
df_target_table = spark.sql(f'''
  SELECT
      LINE
    FROM db_br_silver.sales_line_year_month
    WHERE YEAR=2019 
      AND MONTH=12
    ORDER BY QUANTITY_SOLD DESC
    LIMIT 1
  ''')

pdf = df_target_table.toPandas()

query1 = "Boticário"
query2 = pdf['LINE'][0]
query3 = f'''{query1} OR {query2}'''

# COMMAND ----------

# DBTITLE 1,Twitter query
search1 = tw_api.search_tweets(q=query1, lang="pt-BR", tweet_mode="extended",count=50)
search2 = tw_api.search_tweets(q=query2, lang="pt-BR", tweet_mode="extended",count=50)
search3 = tw_api.search_tweets(q=query3, lang="pt-BR", tweet_mode="extended",count=50)

# COMMAND ----------

# DBTITLE 1,Set DataFrame Pandas
twitters = []

for results1 in search1:
    twitters.append([
        results1.id,
        results1.created_at,
        results1.user.screen_name,
        results1.user.name,
        results1.full_text,
        query1
    ])
    
for results2 in search2:
    twitters.append([
        results2.id,
        results2.created_at,
        results2.user.screen_name,
        results2.user.name,
        results2.full_text,
        query2
    ])
    
for results3 in search3:
    twitters.append([
        results3.id,
        results3.created_at,
        results3.user.screen_name,
        results3.user.name,
        results3.full_text,
        query3
    ])
    
pdf = pandas.DataFrame(twitters, columns=['id', 'created_at', 'screen_name', 'name', 'full_text', 'query_key'])

# COMMAND ----------

# DBTITLE 1,Storing twitters
target_table = "twitters_logs"

df_target_table = spark.createDataFrame(pdf)

df_target_table.write.format("delta").mode("overwrite").saveAsTable(target_db + '.' + target_table, mergeSchema=True, overwriteSchema=True)

#df_target_table.display()

# COMMAND ----------


