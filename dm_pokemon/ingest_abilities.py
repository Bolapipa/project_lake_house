# Databricks notebook source
# Databricks notebook source
import requests

# COMMAND ----------

dbutils.widgets.text("catalog", "bronze_dev")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_pokemon")
used_schema = dbutils.widgets.get("schema")

tabela_controle = f"{used_catalog}.{used_schema}.controle_ingestao"
tabela_destino = f"{used_catalog}.{used_schema}.raw_pokemon_abilities"

print(used_catalog)
print(used_schema)
print(tabela_controle)
print(tabela_destino)

# COMMAND ----------

ctrl_id = spark.sql(
    f"SELECT raw_pokemon_abilities FROM {tabela_controle} WHERE id = 1"
).collect()

if ctrl_id and ctrl_id[0][0] is not None:
    ultimo_id = int(ctrl_id[0][0])
else:
    ultimo_id = 0

print(f"Último ID de ability salvo: {ultimo_id}")

# COMMAND ----------

API_URL = "https://pokeapi.co/api/v2/ability?limit=2000"
all_rows_abilities = []

while API_URL:
    response = requests.get(API_URL)
    if response.status_code != 200 or not response.text:
        API_URL = None
        continue

    dados = response.json()

    for ability in dados.get("results", []):
        url = (ability.get("url") or "").rstrip("/")
        if not url:
            continue

        partes = url.split("/")

        try:
            ability_id = int(partes[-1])  # id da ability na API
        except ValueError:
            continue

        # incremental em cima do id da ability
        if ability_id <= ultimo_id:
            continue

        row_ability = (
            ability_id,          # id incremental
            ability.get("name")  # nome da ability (bruto da API)
        )

        all_rows_abilities.append(row_ability)

    API_URL = dados.get("next")

print(f"Total de novas abilities: {len(all_rows_abilities)}")

# COMMAND ----------

if all_rows_abilities:
    df_abilities = spark.createDataFrame(
        all_rows_abilities,
        ["id_ability", "name"]
    )

    display(df_abilities)

    df_abilities.write.mode("append").saveAsTable(tabela_destino)

    ultimo_id_ingestao = all_rows_abilities[-1][0]

    spark.sql(f"""
        UPDATE {tabela_controle}
        SET raw_pokemon_abilities = {ultimo_id_ingestao}
        WHERE id = 1
    """)

    print(f"Último ID de ability atualizado na tabela de controle: {ultimo_id_ingestao}")
else:
    print("Nenhuma nova ability para inserir.")

