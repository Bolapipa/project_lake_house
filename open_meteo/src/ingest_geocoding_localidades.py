# Databricks notebook source
dbutils.widgets.text("catalog", "bronze_dev")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_open_meteo")
used_schema = dbutils.widgets.get("schema")

tabela_destino = f"{used_catalog}.{used_schema}.raw_geocoding_localidades"
tabela_controle = f"{used_catalog}.{used_schema}.tabela_controle"
tabela_localidades = f"{used_catalog}.{used_schema}.auxiliar_localidades"

import requests
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

localidades = spark.table(tabela_localidades).collect()

dados_finais = []

for localidade in localidades:
    capital = localidade["capital"]
    uf = localidade["uf"]

    url = "https://geocoding-api.open-meteo.com/v1/search"

    params = {
        "name": capital,
        "count": 1,
        "language": "pt",
        "format": "json"
    }

    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()

    dados = response.json()
    resultados = dados.get("results", [])

    if not resultados:
        continue

    resultado = resultados[0]

    linha = {
        "id": resultado.get("id"),
        "name": resultado.get("name"),
        "latitude": resultado.get("latitude"),
        "longitude": resultado.get("longitude"),
        "elevation": resultado.get("elevation"),
        "feature_code": resultado.get("feature_code"),
        "country_code": resultado.get("country_code"),
        "admin1_id": resultado.get("admin1_id"),
        "admin2_id": resultado.get("admin2_id"),
        "admin3_id": resultado.get("admin3_id"),
        "admin4_id": resultado.get("admin4_id"),
        "timezone": resultado.get("timezone"),
        "population": resultado.get("population"),
        "country_id": resultado.get("country_id"),
        "country": resultado.get("country"),
        "admin1": resultado.get("admin1"),
        "admin2": resultado.get("admin2"),
        "admin3": resultado.get("admin3"),
        "admin4": resultado.get("admin4"),
        "uf_consulta": uf,
        "capital_consulta": capital
    }

    dados_finais.append(linha)

if not dados_finais:
    raise ValueError("A API de geocoding retornou dados vazios.")

# Remove colunas que vieram totalmente nulas no lote atual
colunas_com_valor = [
    coluna
    for coluna in dados_finais[0].keys()
    if any(linha.get(coluna) is not None for linha in dados_finais)
]

dados_finais_tratados = [
    {coluna: linha.get(coluna) for coluna in colunas_com_valor}
    for linha in dados_finais
]

df_spark_novo = spark.createDataFrame(dados_finais_tratados).dropDuplicates(["id"])

display(df_spark_novo)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {used_catalog}.{used_schema}")

if not spark.catalog.tableExists(tabela_destino):
    df_spark_novo.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(tabela_destino)
else:
    delta_table = DeltaTable.forName(spark, tabela_destino)

    (
        delta_table.alias("destino")
        .merge(
            df_spark_novo.alias("origem"),
            "destino.id = origem.id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

data_atual = datetime.now()
qtd_registros = str(df_spark_novo.count())

schema_controle = StructType([
    StructField("processo", StringType(), True),
    StructField("ultima_execucao", TimestampType(), True),
    StructField("ultima_data_previsao", StringType(), True),
    StructField("qtd_registros", StringType(), True)
])

dados_controle = [
    (
        "open_meteo_geocoding_localidades",
        data_atual,
        None,
        qtd_registros
    )
]

controle_df = spark.createDataFrame(dados_controle, schema=schema_controle)

controle_df.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "processo = 'open_meteo_geocoding_localidades'") \
    .saveAsTable(tabela_controle)

print("Controle de ingestao atualizado com sucesso.")
