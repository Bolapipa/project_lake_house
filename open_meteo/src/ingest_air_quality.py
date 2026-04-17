# Databricks notebook source
dbutils.widgets.text("catalog", "bronze_dev")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_open_meteo")
used_schema = dbutils.widgets.get("schema")

tabela_destino = f"{used_catalog}.{used_schema}.raw_air_quality"
tabela_controle = f"{used_catalog}.{used_schema}.tabela_controle"
tabela_localidades = f"{used_catalog}.{used_schema}.auxiliar_localidades"

import requests
from datetime import datetime, timedelta
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Define o periodo de consulta
data_fim = datetime.today().date()
data_inicio = data_fim - timedelta(days=3)

# Le as localidades auxiliares
localidades = spark.table(tabela_localidades).collect()

# Coleta os dados da API
dados_finais = []

for localidade in localidades:
    uf = localidade["uf"]
    capital = localidade["capital"]
    latitude = localidade["latitude"]
    longitude = localidade["longitude"]

    url = "https://air-quality-api.open-meteo.com/v1/air-quality"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": str(data_inicio),
        "end_date": str(data_fim),
        "hourly": "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone,uv_index,european_aqi,us_aqi",
        "timezone": "America/Sao_Paulo"
    }

    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()

    dados = response.json()
    hourly = dados.get("hourly", {})
    datas = hourly.get("time", [])

    for i in range(len(datas)):
        linha = {
            "uf": uf,
            "capital": capital,
            "latitude": dados.get("latitude"),
            "longitude": dados.get("longitude"),
            "elevation": dados.get("elevation"),
            "timezone": dados.get("timezone"),
            "time": hourly.get("time", [None] * len(datas))[i],
            "pm10": hourly.get("pm10", [None] * len(datas))[i],
            "pm2_5": hourly.get("pm2_5", [None] * len(datas))[i],
            "carbon_monoxide": hourly.get("carbon_monoxide", [None] * len(datas))[i],
            "nitrogen_dioxide": hourly.get("nitrogen_dioxide", [None] * len(datas))[i],
            "sulphur_dioxide": hourly.get("sulphur_dioxide", [None] * len(datas))[i],
            "ozone": hourly.get("ozone", [None] * len(datas))[i],
            "uv_index": hourly.get("uv_index", [None] * len(datas))[i],
            "european_aqi": hourly.get("european_aqi", [None] * len(datas))[i],
            "us_aqi": hourly.get("us_aqi", [None] * len(datas))[i]
        }

        dados_finais.append(linha)

if not dados_finais:
    raise ValueError("A API de qualidade do ar retornou dados vazios.")

# Cria o DataFrame da bronze deixando o Spark inferir os tipos
df_spark_novo = spark.createDataFrame(dados_finais).dropDuplicates(
    ["latitude", "longitude", "time"]
)

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

# Atualiza a tabela de controle
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
        "open_meteo_air_quality",
        data_atual,
        str(data_fim),
        qtd_registros
    )
]

controle_df = spark.createDataFrame(dados_controle, schema=schema_controle)

controle_df.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "processo = 'open_meteo_air_quality'") \
    .saveAsTable(tabela_controle)

print("Controle de ingestao atualizado com sucesso.")
