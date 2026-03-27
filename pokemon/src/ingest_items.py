# Databricks notebook source
import requests

# COMMAND ----------

# Widgets de catálogo e schema (continua igual, só pra reaproveitar depois)
dbutils.widgets.text("catalog", "bronze_prod")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_items")
used_schema = dbutils.widgets.get("schema")

tabela_controle = f"{used_catalog}.{used_schema}.controle_ingestao"
tabela_destino = f"{used_catalog}.{used_schema}.raw_items"

print(used_catalog)
print(used_schema)
print(tabela_controle)
print(tabela_destino)

# COMMAND ----------

API_URL = "https://pokeapi.co/api/v2/item?limit=2000"
all_rows_items = []

while API_URL:
    response = requests.get(API_URL)
    if response.status_code != 200 or not response.text:
        print(f"Falha ao chamar {API_URL}: status {response.status_code}")
        API_URL = None
        continue

    dados = response.json()

    for item in dados.get("results", []):
        url = (item.get("url") or "").rstrip("/")
        if not url:
            continue

        partes = url.split("/")

        try:
            item_id = int(partes[-1])  # id do item na API
        except ValueError:
            continue

        row_item = (
            item_id,          # id do item
            item.get("name")  # nome do item (bruto da API)
        )

        all_rows_items.append(row_item)

    API_URL = dados.get("next")

print(f"Total de items retornados: {len(all_rows_items)}")

# COMMAND ----------

if all_rows_items:
    df_item = spark.createDataFrame(
        all_rows_items,
        ["id_item", "name"]
    )

    display(df_item)
else:
    print("Nenhum item retornado pela API.")

# COMMAND ----------

df_item.write.mode("append").saveAsTable(f"{used_catalog}.{used_schema}.raw_items")

# COMMAND ----------

# Atualiza o último ID ingerido na tabela de controle
if all_rows_items:
    ultimo_id_ingestao = all_rows_items[-1][0]  # pega o último ID da ingestão

    spark.sql(f"""
        UPDATE {tabela_controle}
        SET raw_items = {ultimo_id_ingestao}
        WHERE id = 1
    """)

    print(f"Último ID atualizado na tabela de controle: {ultimo_id_ingestao}")
