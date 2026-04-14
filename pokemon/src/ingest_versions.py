# Databricks notebook source
import requests

# COMMAND ----------

dbutils.widgets.text("catalog", "bronze_prod")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_pokemon")
used_schema = dbutils.widgets.get("schema")

tabela_controle = f"{used_catalog}.{used_schema}.controle_ingestao"
tabela_destino = f"{used_catalog}.{used_schema}.raw_pokemon_versions"

print(used_catalog)
print(used_schema)
print(tabela_controle)
print(tabela_destino)

# COMMAND ----------

ctrl_id = spark.sql(
    f"SELECT raw_pokemon_versions FROM {tabela_controle} WHERE id = 1"
).collect()

if ctrl_id and ctrl_id[0][0] is not None:
    ultimo_id = int(ctrl_id[0][0])
else:
    ultimo_id = 0

print(f"Último ID salvo: {ultimo_id}")

# COMMAND ----------

import time

API_URL = "https://pokeapi.co/api/v2/version?limit=100"
all_rows = []

while API_URL:
    response = requests.get(API_URL)
    if response.status_code != 200 or not response.text:
        API_URL = None
        continue

    dados = response.json()

    for version in dados.get("results", []):
        version_url = version.get("url")
        if not version_url:
            continue

        version_resp = requests.get(version_url)
        if version_resp.status_code != 200 or not version_resp.text:
            continue

        version_data = version_resp.json()

        version_id = int(version_data.get("id"))

        # filtro incremental
        if version_id <= ultimo_id:
            continue

        row = (
            version_id,
            version_data.get("name"),
            version_data.get("version_group", {}).get("name") if version_data.get("version_group") else None
        )
        all_rows.append(row)

        time.sleep(0.1)

    API_URL = dados.get("next")

print(f"Total de novos registros: {len(all_rows)}")

# COMMAND ----------

if all_rows:
    df_versions = spark.createDataFrame(
        all_rows,
        ["id", "name", "version_group"]
    )

    display(df_versions)

    df_versions.write.mode("append").saveAsTable(tabela_destino)

    ultimo_id_ingestao = all_rows[-1][0]

    spark.sql(f"""
        UPDATE {tabela_controle}
        SET raw_pokemon_versions = {ultimo_id_ingestao}
        WHERE id = 1
    """)

    print(f"Último ID atualizado na tabela de controle: {ultimo_id_ingestao}")
else:
    print("Nenhum novo registro para inserir.")
