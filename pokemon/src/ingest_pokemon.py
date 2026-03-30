# Databricks notebook source
# MAGIC %pip install requests

# COMMAND ----------

import requests
import json
import time
from datetime import datetime
from pyspark.sql.functions import lit
import pytz

# COMMAND ----------

dbutils.widgets.text("catalog", "bronze_prod")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_pokemon")
used_schema = dbutils.widgets.get("schema")

tabela_controle = f"{used_catalog}.{used_schema}.controle_ingestao"

print(used_catalog)
print(used_schema)
print(tabela_controle)

# COMMAND ----------

# busca o ultimo id salvo (necessário para comparar IDs)
ctrl_id = spark.sql(f"SELECT raw_pokemon_name FROM {tabela_controle} WHERE id = 1").collect()

if ctrl_id and ctrl_id[0][0] is not None:
    ultimo_id = (ctrl_id[0][0])
else:
    ultimo_id = 0

print(f"Último id salvo: {ultimo_id}")


# COMMAND ----------

import requests
import time

API_URL = "https://pokeapi.co/api/v2/pokemon/?limit=20"
all_rows = []

ultimo_id = int(ultimo_id) if ultimo_id is not None else 0  # corrigido

while API_URL:
    response = requests.get(API_URL)
    if response.status_code != 200 or not response.text:
        API_URL = None
        continue

    dados = response.json()

    for pkm in dados.get("results", []):
        poke_resp = requests.get(pkm["url"])
        if poke_resp.status_code != 200 or not poke_resp.text:
            continue

        pokemon = poke_resp.json()

        pokemon_id = int(pokemon.get("id"))  # corrigido

        # filtro incremental
        if pokemon_id <= ultimo_id:  # corrigido
            continue

        row = (
            pokemon_id,  # corrigido
            pokemon.get("name")
        )
        all_rows.append(row)

        time.sleep(0.1)

    API_URL = dados["next"]

else:
    API_URL = None

# se não tem dados novos, não cria DF, não grava tabela, não atualiza controle
if len(all_rows) == 0:
    print("Sem dados novos para ingerir. Não vou atualizar tabela nem controle.")
else:
    df = spark.createDataFrame(all_rows, ["id", "name"])
    display(df)

    df.write.mode("append").saveAsTable(f"{used_catalog}.{used_schema}.raw_pokemon_name")

    # Atualiza tabela de controle
    novo_ultimo_id = all_rows[-1][0]
    spark.sql(f"""
        UPDATE {tabela_controle}
        SET raw_pokemon_name = {novo_ultimo_id}
        WHERE id = 1
    """)
    print(f"Último ID atualizado na tabela de controle: {novo_ultimo_id}")
