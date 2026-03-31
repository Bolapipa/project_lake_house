# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# Define onde a tabela será salva no Databricks.

dbutils.widgets.text("catalog", "bronze_dev")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_ibge")
used_schema = dbutils.widgets.get("schema")

tabela_destino = f"{used_catalog}.{used_schema}.raw_regioes"

print(f"Catálogo: {used_catalog}")
print(f"Schema: {used_schema}")
print(f"Tabela destino: {tabela_destino}")

import requests

# Endpoint oficial de regiões

url = "https://servicodados.ibge.gov.br/api/v1/localidades/regioes"

response = requests.get(url, timeout=60)
response.raise_for_status()

dados = response.json()

if not dados:
    raise ValueError("A API do IBGE retornou uma lista vazia para regiões.")

# Aqui transformamos o retorno da API em DataFrame Spark.
# Não usamos pandas.

df = spark.createDataFrame(dados)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {used_catalog}.{used_schema}")

# Como esta é uma carga histórica completa de um dado pequeno e estável,
# usamos overwrite para substituir o conteúdo da tabela.

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(tabela_destino)

print(f"Tabela criada com sucesso: {tabela_destino}")
print(f"Quantidade de registros carregados: {df.count()}")

display(df)
