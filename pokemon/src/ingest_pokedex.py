# Databricks notebook source
# Databricks notebook source
import requests

# COMMAND ----------

dbutils.widgets.text("catalog", "bronze_prod")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_pokemon")
used_schema = dbutils.widgets.get("schema")

tabela_controle = f"{used_catalog}.{used_schema}.controle_ingestao"
tabela_destino = f"{used_catalog}.{used_schema}.raw_pokemon_number_dex"

print(used_catalog)
print(used_schema)
print(tabela_controle)
print(tabela_destino)

# COMMAND ----------

ctrl_id = spark.sql(
    f"SELECT raw_pokemon_number_dex FROM {tabela_controle} WHERE id = 1"
).collect()

if ctrl_id and ctrl_id[0][0] is not None:
    ultimo_id = int(ctrl_id[0][0])
else:
    ultimo_id = 0

print(f"Último número de Pokédex salvo: {ultimo_id}")

# COMMAND ----------

API_URL = "https://pokeapi.co/api/v2/pokemon-species?limit=2000"
all_rows_dex = []

while API_URL:
    response = requests.get(API_URL)
    if response.status_code != 200 or not response.text:
        API_URL = None
        continue

    dados = response.json()

    for species in dados.get("results", []):
        url = (species.get("url") or "").rstrip("/")
        if not url:
            continue

        partes = url.split("/")
        try:
            num_pokedex = int(partes[-1])  # número da Pokédex Nacional
        except ValueError:
            continue

        # filtro incremental usando o número da dex
        if num_pokedex <= ultimo_id:
            continue

        row_dex = (
            num_pokedex,          # número da dex (id incremental)
            species.get("name")   # nome do pokémon
        )

        all_rows_dex.append(row_dex)

    API_URL = dados.get("next")

print(f"Total de novos registros: {len(all_rows_dex)}")

# COMMAND ----------

if all_rows_dex:
    df_pokedex = spark.createDataFrame(
        all_rows_dex,
        ["num_pokedex", "name"]
    )

    display(df_pokedex)

    df_pokedex.write.mode("append").saveAsTable(tabela_destino)

    ultimo_id_ingestao = all_rows_dex[-1][0]

    spark.sql(f"""
        UPDATE {tabela_controle}
        SET raw_pokemon_number_dex = {ultimo_id_ingestao}
        WHERE id = 1
    """)

    print(f"Último número da Pokédex atualizado na tabela de controle: {ultimo_id_ingestao}")
else:
    print("Nenhum novo registro para inserir.")
