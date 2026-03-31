# Databricks notebook source
# Define onde a tabela será salva no Databricks.

dbutils.widgets.text("catalog", "bronze_dev")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_ibge")
used_schema = dbutils.widgets.get("schema")

tabela_destino = f"{used_catalog}.{used_schema}.raw_regioes_intermediarias"

print(f"Catálogo: {used_catalog}")
print(f"Schema: {used_schema}")
print(f"Tabela destino: {tabela_destino}")

import requests
import json

# Endpoint oficial de regiões intermediárias

url = "https://servicodados.ibge.gov.br/api/v1/localidades/regioes-intermediarias"

response = requests.get(url, timeout=120)
response.raise_for_status()

dados = response.json()

if not dados:
    raise ValueError("A API do IBGE retornou uma lista vazia para regiões intermediárias.")

# Aqui deixamos a bronze robusta:
# - campos simples ficam como estão
# - campos aninhados viram string JSON
#
# Isso evita erro de inferência no Spark e preserva
# o conteúdo bruto da API.

dados_tratados = []

for item in dados:
    linha = {}

    for chave, valor in item.items():
        if isinstance(valor, (dict, list)):
            linha[chave.replace("-", "_")] = json.dumps(valor, ensure_ascii=False)
        else:
            linha[chave.replace("-", "_")] = valor

    dados_tratados.append(linha)

df = spark.createDataFrame(dados_tratados)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {used_catalog}.{used_schema}")

# Como é uma carga histórica completa de dado de referência,
# usamos overwrite.

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(tabela_destino)

print(f"Tabela criada com sucesso: {tabela_destino}")
print(f"Quantidade de registros carregados: {df.count()}")

display(df)
