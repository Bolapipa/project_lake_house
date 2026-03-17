# Databricks notebook source
import requests

# COMMAND ----------

# Widgets de catálogo e schema (continua igual, só pra reaproveitar depois)
dbutils.widgets.text("catalog", "bronze_dev")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_pokemon_items")
used_schema = dbutils.widgets.get("schema")

tabela_controle = f"{used_catalog}.{used_schema}.controle_ingestao"
tabela_destino = f"{used_catalog}.{used_schema}.raw_pokemon_item_attributes"

print(used_catalog)
print(used_schema)
print(tabela_controle)
print(tabela_destino)

# COMMAND ----------

API_URL = "https://pokeapi.co/api/v2/item-attribute/?limit=2000"
all_rows_item_attributes = []

while API_URL:
    response = requests.get(API_URL)
    if response.status_code != 200 or not response.text:
        print(f"Falha ao chamar {API_URL}: status {response.status_code}")
        API_URL = None
        continue

    dados = response.json()

    for attr in dados.get("results", []):
        url = (attr.get("url") or "").rstrip("/")
        if not url:
            continue

        partes = url.split("/")

        try:
            attr_id = int(partes[-1])  # id do item-attribute na API
        except ValueError:
            continue

        row_attr = (
            attr_id,            # id do item-attribute
            attr.get("name")    # nome do item-attribute (bruto da API)
        )

        all_rows_item_attributes.append(row_attr)

    API_URL = dados.get("next")

print(f"Total de item-attributes retornados: {len(all_rows_item_attributes)}")

# COMMAND ----------

if all_rows_item_attributes:
    df_attr = spark.createDataFrame(
        all_rows_item_attributes,
        ["id_item_attribute", "name"]
    )

    display(df_attr)

    # Só escreve se tiver dado (evita erro de schema vazio)
    df_attr.write.mode("append").saveAsTable(tabela_destino)

    # Atualiza o último ID ingerido na tabela de controle
    ultimo_id_ingestao = all_rows_item_attributes[-1][0]

    spark.sql(f"""
        UPDATE {tabela_controle}
        SET raw_pokemon_item_attributes = {ultimo_id_ingestao}
        WHERE id = 1
    """)

    print(f"Último ID atualizado na tabela de controle: {ultimo_id_ingestao}")
else:
    print("Nenhum item-attribute retornado pela API. Não vou gravar tabela nem atualizar controle.")
