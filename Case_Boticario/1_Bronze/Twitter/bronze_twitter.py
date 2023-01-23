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
query4 = f'''{query1} AND {query2}'''

print(query1)
print(query2)
print(query3)
print(query4)

# COMMAND ----------

# DBTITLE 1,Twitter query
search1 = tw_api.search_tweets(q=query1, lang="pt-BR", tweet_mode="extended",count=50)
search2 = tw_api.search_tweets(q=query2, lang="pt-BR", tweet_mode="extended",count=50)
search3 = tw_api.search_tweets(q=query3, lang="pt-BR", tweet_mode="extended",count=50)
search4 = tw_api.search_tweets(q=query4, lang="pt-BR", tweet_mode="extended",count=50)

# COMMAND ----------

def append_twitters(search, twitters, query):
    for results in search:
        twitters.append([
            results.id,
            results.created_at,
            results.user.screen_name,
            results.user.name,
            results.full_text,
            query
        ])
    return twitters

# COMMAND ----------

# DBTITLE 1,Set DataFrame Pandas
twitters = []

twitters = append_twitters(search1, twitters, query1)
twitters = append_twitters(search2, twitters, query2)
twitters = append_twitters(search3, twitters, query3)
twitters = append_twitters(search4, twitters, query4)
    
pdf = pandas.DataFrame(twitters, columns=['id', 'created_at', 'screen_name', 'name', 'full_text', 'query_key'])

# COMMAND ----------

# DBTITLE 1,Storing twitters
target_table = "twitters_logs"

df_target_table = spark.createDataFrame(pdf)

df_target_table.write.format("delta").mode("overwrite").saveAsTable(target_db + '.' + target_table, mergeSchema=True, overwriteSchema=True)

#df_target_table.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM db_br_bronze.twitters_logs
