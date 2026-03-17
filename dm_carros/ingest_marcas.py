# Databricks notebook source
dbutils.widgets.text("catalog", "bronze_dev")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_carros")
used_schema = dbutils.widgets.get("schema")

tabela_destino = f"{used_catalog}.{used_schema}.raw_marcas"

import requests
import pandas as pd
from pyspark.sql.functions import col

url = "https://fipe.parallelum.com.br/api/v2/cars/brands"

response = requests.get(url, timeout=30)
response.raise_for_status()

dados = response.json()

df_pandas = pd.DataFrame(dados)
df_spark = spark.createDataFrame(df_pandas)

df_spark = df_spark.select(
    col("code").alias("id_marca"),
    col("name").alias("nome_marca")
)

display(df_spark)

# Realiza o append com overwriteschema true
df_spark.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable(tabela_destino)

print(f"Dados salvos com sucesso em: {tabela_destino}")
