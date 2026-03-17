# Databricks notebook source
dbutils.widgets.text("catalog", "bronze_dev")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_carros")
used_schema = dbutils.widgets.get("schema")

tabela_origem = f"{used_catalog}.{used_schema}.raw_marcas"
tabela_destino = f"{used_catalog}.{used_schema}.raw_modelos"

import requests
import pandas as pd

# Lê todas as marcas já gravadas
df_marcas = spark.table(tabela_origem)

marcas = df_marcas.select("id_marca", "nome_marca").collect()

lista_modelos = []

for marca in marcas:
    id_marca = marca["id_marca"]
    nome_marca = marca["nome_marca"]

    url = f"https://fipe.parallelum.com.br/api/v2/cars/brands/{id_marca}/models"

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    dados = response.json()

    for modelo in dados:
        lista_modelos.append({
            "id_marca": id_marca,
            "nome_marca": nome_marca,
            "id_modelo": modelo["code"],
            "nome_modelo": modelo["name"]
        })

df_pandas = pd.DataFrame(lista_modelos)
df_spark = spark.createDataFrame(df_pandas)

display(df_spark)

# Realiza o append com overwriteschema true
df_spark.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable(tabela_destino)

print(f"Dados salvos com sucesso em: {tabela_destino}")
