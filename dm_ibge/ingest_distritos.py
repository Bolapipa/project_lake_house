# Databricks notebook source
# Define o destino da tabela

dbutils.widgets.text("catalog", "bronze_dev")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_ibge")
used_schema = dbutils.widgets.get("schema")

tabela_destino = f"{used_catalog}.{used_schema}.raw_distritos"

print(f"Catálogo: {used_catalog}")
print(f"Schema: {used_schema}")
print(f"Tabela destino: {tabela_destino}")

import requests
import json

# Endpoint oficial de distritos
url = "https://servicodados.ibge.gov.br/api/v1/localidades/distritos"

# Faz a chamada da API
response = requests.get(url, timeout=120)
response.raise_for_status()

dados = response.json()

# Valida se a API retornou dados
if not dados:
    raise ValueError("A API do IBGE retornou uma lista vazia para distritos.")

# Prepara os dados para a bronze
# Campos simples ficam normais
# Campos aninhados viram string JSON
dados_tratados = []

for item in dados:
    linha = {}

    for chave, valor in item.items():
        nome_coluna = chave.replace("-", "_")

        if isinstance(valor, (dict, list)):
            linha[nome_coluna] = json.dumps(valor, ensure_ascii=False)
        else:
            linha[nome_coluna] = valor

    dados_tratados.append(linha)

# Cria o DataFrame Spark
df = spark.createDataFrame(dados_tratados)

# Garante que o schema existe
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {used_catalog}.{used_schema}")

# Salva a tabela na bronze
df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(tabela_destino)

# Exibe o resultado
print(f"Tabela criada com sucesso: {tabela_destino}")
print(f"Quantidade de registros carregados: {df.count()}")

display(df)
