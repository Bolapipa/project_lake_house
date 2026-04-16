# Databricks notebook source
dbutils.widgets.text("catalog", "bronze_dev")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_open_meteo")
used_schema = dbutils.widgets.get("schema")

tabela_destino = f"{used_catalog}.{used_schema}.raw_daily_forecast"
tabela_controle = f"{used_catalog}.{used_schema}.tabela_controle"
tabela_localidades = f"{used_catalog}.{used_schema}.auxiliar_localidades"

import requests
import pandas as pd
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Le as localidades auxiliares
localidades = spark.table(tabela_localidades).collect()

# Coleta os dados da API
dados_finais = []

for localidade in localidades:
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": localidade["latitude"],
        "longitude": localidade["longitude"],
        "daily": "temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,weather_code,wind_speed_10m_max,sunrise,sunset",
        "timezone": "America/Sao_Paulo",
        "forecast_days": 7
    }

    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()

    dados = response.json()
    daily = dados.get("daily", {})
    datas = daily.get("time", [])

    for i in range(len(datas)):
        linha = {
            "latitude": dados.get("latitude"),
            "longitude": dados.get("longitude"),
            "generationtime_ms": dados.get("generationtime_ms"),
            "utc_offset_seconds": dados.get("utc_offset_seconds"),
            "timezone": dados.get("timezone"),
            "timezone_abbreviation": dados.get("timezone_abbreviation"),
            "elevation": dados.get("elevation"),
            "time": daily.get("time", [None] * len(datas))[i],
            "temperature_2m_max": daily.get("temperature_2m_max", [None] * len(datas))[i],
            "temperature_2m_min": daily.get("temperature_2m_min", [None] * len(datas))[i],
            "temperature_2m_mean": daily.get("temperature_2m_mean", [None] * len(datas))[i],
            "precipitation_sum": daily.get("precipitation_sum", [None] * len(datas))[i],
            "weather_code": daily.get("weather_code", [None] * len(datas))[i],
            "wind_speed_10m_max": daily.get("wind_speed_10m_max", [None] * len(datas))[i],
            "sunrise": daily.get("sunrise", [None] * len(datas))[i],
            "sunset": daily.get("sunset", [None] * len(datas))[i]
        }

        dados_finais.append(linha)

if not dados_finais:
    raise ValueError("A API retornou dados vazios.")

# Cria o DataFrame da bronze
df_pandas = pd.DataFrame(dados_finais)
df_spark_novo = spark.createDataFrame(df_pandas).dropDuplicates(["latitude", "longitude", "time"])

display(df_spark_novo)

# Garante o schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {used_catalog}.{used_schema}")

# Faz carga inicial ou merge incremental
if not spark.catalog.tableExists(tabela_destino):
    df_spark_novo.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(tabela_destino)

    print(f"Tabela criada e carga inicial realizada em: {tabela_destino}")

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

    print(f"Carga incremental realizada com merge em: {tabela_destino}")

# Monta o DataFrame da tabela de controle
data_atual = datetime.now()
ultima_data_previsao = df_pandas["time"].max()
qtd_registros = len(df_pandas)

schema_controle = StructType([
    StructField("processo", StringType(), True),
    StructField("ultima_execucao", TimestampType(), True),
    StructField("ultima_data_previsao", StringType(), True),
    StructField("qtd_registros", StringType(), True)
])

dados_controle = [
    (
        "open_meteo_daily_forecast",
        data_atual,
        str(ultima_data_previsao),
        str(qtd_registros)
    )
]

controle_df = spark.createDataFrame(dados_controle, schema=schema_controle)

# Atualiza a tabela de controle
if not spark.catalog.tableExists(tabela_controle):
    controle_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(tabela_controle)

else:
    controle_df.write.format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", "processo = 'open_meteo_daily_forecast'") \
        .saveAsTable(tabela_controle)

print("Controle de ingestao atualizado com sucesso.")
