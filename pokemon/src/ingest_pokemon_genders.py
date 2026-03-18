# Databricks notebook source
# Databricks notebook source
import requests

# COMMAND ----------

dbutils.widgets.text("catalog", "bronze_prod")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_pokemon")
used_schema = dbutils.widgets.get("schema")

tabela_controle = f"{used_catalog}.{used_schema}.controle_ingestao"
tabela_destino = f"{used_catalog}.{used_schema}.raw_pokemon_genders"

print(used_catalog)
print(used_schema)
print(tabela_controle)
print(tabela_destino)

# COMMAND ----------

ctrl_id = spark.sql(
    f"SELECT raw_pokemon_genders FROM {tabela_controle} WHERE id = 1"
).collect()

if ctrl_id and ctrl_id[0][0] is not None:
    ultimo_id = int(ctrl_id[0][0])
else:
    ultimo_id = 0

print(f"Último ID salvo: {ultimo_id}")

# COMMAND ----------

API_URL = "https://pokeapi.co/api/v2/gender?limit=2000"
all_rows = []

while API_URL:
    response = requests.get(API_URL)
    if response.status_code != 200 or not response.text:
        API_URL = None
        continue

    dados = response.json()

    for item in dados.get("results", []):
        url = (item.get("url") or "").rstrip("/")
        if not url:
            continue

        partes = url.split("/")
        try:
            item_id = int(partes[-1])
        except ValueError:
            continue

        if item_id <= ultimo_id:
            continue

        row = (
            item_id,
            item.get("name")
        )

        all_rows.append(row)

    API_URL = dados.get("next")

print(f"Total de novos registros: {len(all_rows)}")

# COMMAND ----------

if all_rows:
    df = spark.createDataFrame(all_rows, ["id_gender", "name"])
    display(df)

    df.write.mode("append").saveAsTable(tabela_destino)

    ultimo_id_ingestao = all_rows[-1][0]

    spark.sql(f"""
        UPDATE {tabela_controle}
        SET raw_pokemon_genders = {ultimo_id_ingestao}
        WHERE id = 1
    """)

    print(f"Último ID atualizado na tabela de controle: {ultimo_id_ingestao}")
else:
    print("Nenhum novo registro para inserir.")

