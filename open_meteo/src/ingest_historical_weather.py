# Databricks notebook source
dbutils.widgets.text("catalog", "bronze_dev")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_open_meteo")
used_schema = dbutils.widgets.get("schema")

tabela_destino = f"{used_catalog}.{used_schema}.raw_historical_weather"
tabela_controle = f"{used_catalog}.{used_schema}.tabela_controle"
tabela_localidades = f"{used_catalog}.{used_schema}.auxiliar_localidades"

import requests
from datetime import datetime, timedelta
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

data_fim = datetime.today().date()
data_inicio = data_fim - timedelta(days=30)

localidades = spark.table(tabela_localidades).collect()

dados_finais = []

for localidade in localidades:
    uf = localidade["uf"]
    capital = localidade["capital"]
    latitude = localidade["latitude"]
    longitude = localidade["longitude"]

    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": str(data_inicio),
        "end_date": str(data_fim),
        "daily": "temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,weather_code,wind_speed_10m_max",
        "timezone": "America/Sao_Paulo"
    }

    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()

    dados = response.json()
    daily = dados.get("daily", {})
    datas = daily.get("time", [])

    for i in range(len(datas)):
        linha = {
            "uf": uf,
            "capital": capital,
            "latitude": dados.get("latitude"),
            "longitude": dados.get("longitude"),
            "elevation": dados.get("elevation"),
            "timezone": dados.get("timezone"),
            "time": daily.get("time", [None] * len(datas))[i],
            "temperature_2m_max": daily.get("temperature_2m_max", [None] * len(datas))[i],
            "temperature_2m_min": daily.get("temperature_2m_min", [None] * len(datas))[i],
            "temperature_2m_mean": daily.get("temperature_2m_mean", [None] * len(datas))[i],
            "precipitation_sum": daily.get("precipitation_sum", [None] * len(datas))[i],
            "weather_code": daily.get("weather_code", [None] * len(datas))[i],
            "wind_speed_10m_max": daily.get("wind_speed_10m_max", [None] * len(datas))[i]
        }

        dados_finais.append(linha)

if not dados_finais:
    raise ValueError("A API historica retornou dados vazios.")

df_spark_novo = spark.createDataFrame(dados_finais).dropDuplicates(
    ["latitude", "longitude", "time"]
)

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
            """
            destino.latitude = origem.latitude
            AND destino.longitude = origem.longitude
            AND destino.time = origem.time
            """
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
        "open_meteo_historical_weather",
        data_atual,
        str(data_fim),
        qtd_registros
    )
]

controle_df = spark.createDataFrame(dados_controle, schema=schema_controle)

controle_df.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "processo = 'open_meteo_historical_weather'") \
    .saveAsTable(tabela_controle)

print("Controle de ingestao atualizado com sucesso.")
