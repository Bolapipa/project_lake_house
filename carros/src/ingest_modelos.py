# Databricks notebook source
# DBTITLE 1,Ingestão robusta de modelos FIPE com retry para erro 429
import time
from requests.exceptions import HTTPError
from datetime import datetime

dbutils.widgets.text("catalog", "bronze_prod")
used_catalog = dbutils.widgets.get("catalog")
dbutils.widgets.text("schema", "ds_carros")
used_schema = dbutils.widgets.get("schema")

tabela_origem = f"{used_catalog}.{used_schema}.raw_marcas"
tabela_destino = f"{used_catalog}.{used_schema}.raw_modelos"
tabela_controle = f"{used_catalog}.{used_schema}.tabela_controle"

import requests
import pandas as pd

# Lê todas as marcas já gravadas
df_marcas = spark.table(tabela_origem)
marcas = df_marcas.select("id_marca", "nome_marca").collect()

lista_modelos = []

dados = []

for marca in marcas:
    id_marca = str(marca["id_marca"])  # Garante que id_marca seja sempre string
    nome_marca = marca["nome_marca"]
    url = f"https://fipe.parallelum.com.br/api/v2/cars/brands/{id_marca}/models"

    # NOVO: mostra qual marca está sendo processada
    print(f"Iniciando ingestão da marca {id_marca} - {nome_marca}")

    for tentativas in range(3):
        try:
            # NOVO: mostra tentativa
            print(f"Tentativa {tentativas + 1}/3 para marca {id_marca} - {nome_marca}")

            response = requests.get(url, timeout=30)
            response.raise_for_status()
            dados = response.json()

            # NOVO: mostra sucesso e quantidade de modelos retornados
            print(f"Sucesso na marca {id_marca} - {nome_marca}. Total de modelos retornados: {len(dados)}")
            break

        except HTTPError as e:
            if response.status_code == 429 and tentativas < 2:
                print(f"HTTP 429 detectado para marca {id_marca} - {nome_marca}, aguardando 2 segundos antes de tentar novamente...")
                time.sleep(2)
            else:
                print(f"Erro HTTP na marca {id_marca} - {nome_marca}: {e}, código: {response.status_code}")
                dados = []
                break

        except Exception as e:
            print(f"Erro geral ao pedir modelos para marca {id_marca} - {nome_marca}: {e}")
            dados = []
            break

    for modelo in dados:
        lista_modelos.append({
            "id_marca": id_marca,
            "nome_marca": nome_marca,
            "id_modelo": modelo["code"],
            "nome_modelo": modelo["name"]
        })

df_pandas = pd.DataFrame(lista_modelos)
df_pandas["id_marca"] = df_pandas["id_marca"].astype(str)  # Garante que id_marca seja string no DataFrame
df_spark = spark.createDataFrame(df_pandas)
df_spark = df_spark.dropDuplicates()  # Remove duplicatas
display(df_spark)

# Realiza o append com overwriteschema true
df_spark.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable(tabela_destino)
print(f"Dados salvos com sucesso em: {tabela_destino}")

# Controle incremental por último id_modelo - save como string
from pyspark.sql.functions import max as spark_max
ultimo_id = df_spark.agg(spark_max("id_modelo").alias("ultimo_id")).collect()[0]["ultimo_id"]
data_atual = datetime.now()

controle_df = spark.createDataFrame([
    {"processo": "modelos", "ultimo_id": str(ultimo_id), "ultima_execucao": data_atual}
])

controle_df.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "processo = 'modelos'") \
    .saveAsTable(tabela_controle)

print("Controle incremental atualizado para modelos.")
