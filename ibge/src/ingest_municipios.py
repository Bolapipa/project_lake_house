# Databricks notebook source
# DBTITLE 1,Cell 1
# Define onde a tabela será salva no Databricks.

dbutils.widgets.text("catalog", "bronze_dev")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_ibge")
used_schema = dbutils.widgets.get("schema")

tabela_destino = f"{used_catalog}.{used_schema}.raw_municipios"

print(f"Catálogo: {used_catalog}")
print(f"Schema: {used_schema}")
print(f"Tabela destino: {tabela_destino}")

import requests
import json

# Endpoint oficial de municípios

url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"

response = requests.get(url, timeout=120)
response.raise_for_status()

dados = response.json()

if not dados:
    raise ValueError("A API do IBGE retornou uma lista vazia para municípios.")

# CORRIGIDO:
# As colunas aninhadas estão sendo convertidas para string JSON.
# Isso evita erro de inferência de tipo no Spark
# e preserva o conteúdo bruto da API na bronze.

dados_tratados = []

for item in dados:
    dados_tratados.append({
        "id": item.get("id"),
        "nome": item.get("nome"),
        "microrregiao": json.dumps(item.get("microrregiao"), ensure_ascii=False),
        "regiao_imediata": json.dumps(item.get("regiao-imediata"), ensure_ascii=False)
    })

df = spark.createDataFrame(dados_tratados)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {used_catalog}.{used_schema}")


df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(tabela_destino)

print(f"Tabela criada com sucesso: {tabela_destino}")
print(f"Quantidade de registros carregados: {df.count()}")

display(df)
