# Databricks notebook source
dbutils.widgets.text("catalog", "bronze_prod")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_locations")
used_schema = dbutils.widgets.get("schema")

tabela_controle = f"{used_catalog}.{used_schema}.controle_ingestao"
tabela_destino = f"{used_catalog}.{used_schema}.raw_locations"

print(used_catalog)
print(used_schema)
print(tabela_controle)
print(tabela_destino)

# COMMAND ----------

ctrl_id = spark.sql(
    f"SELECT raw_locations FROM {tabela_controle} WHERE id = 1"
).collect()

if ctrl_id and ctrl_id[0][0] is not None:
    ultimo_id = int(ctrl_id[0][0])
else:
    ultimo_id = 0

print(f"Último ID de location salvo: {ultimo_id}")

# COMMAND ----------

import requests

API_URL = "https://pokeapi.co/api/v2/location/"
all_rows_locations = []

while API_URL:
    response = requests.get(API_URL)
    
    if response.status_code != 200 or not response.text:
        API_URL = None
        continue

    dados = response.json()

    for location in dados.get("results", []):
        url = (location.get("url") or "").rstrip("/")
        if not url:
            continue

        partes = url.split("/")

        try:
            location_id = int(partes[-1])  # id da location na API
        except ValueError:
            continue

        # incremental em cima do id da location
        if location_id <= ultimo_id:
            continue

        row_location = (
            location_id,          # id incremental
            location.get("name")  # nome da location
        )

        all_rows_locations.append(row_location)

    API_URL = dados.get("next")

print(f"Total de novas locations: {len(all_rows_locations)}")

# COMMAND ----------

if all_rows_locations:
    df_locations = spark.createDataFrame(
        all_rows_locations,
        ["id_location", "name"]
    )

    display(df_locations)

    df_locations.write.mode("append").saveAsTable(tabela_destino)

    ultimo_id_ingestao = max([row[0] for row in all_rows_locations])

    spark.sql(f'''
        UPDATE {tabela_controle}
        SET raw_locations = {ultimo_id_ingestao}
        WHERE id = 1
    ''')

    print(f"Último ID de location atualizado na tabela de controle: {ultimo_id_ingestao}")
    
else:
    print("Nenhuma nova location para inserir.")
