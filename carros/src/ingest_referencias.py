# Databricks notebook source
# DBTITLE 1,Cell 1: Ajuste do controle incremental para STRING
dbutils.widgets.text("catalog", "bronze_prod")
used_catalog = dbutils.widgets.get("catalog")
dbutils.widgets.text("schema", "ds_carros")
used_schema = dbutils.widgets.get("schema")
tabela_destino = f"{used_catalog}.{used_schema}.raw_referencias"
tabela_controle = f"{used_catalog}.{used_schema}.tabela_controle"

import requests
import pandas as pd
from datetime import datetime

token_api = dbutils.secrets.get(scope="carros", key="fipe-token")

headers = {}
if token_api:
    headers["X-Subscription-Token"] = token_api

# LÊ O CONTROLE INCREMENTAL
ctrl = spark.sql(f"""
SELECT ultimo_id
FROM {tabela_controle}
WHERE processo = 'referencias'
""").collect()

if ctrl and ctrl[0]["ultimo_id"] is not None:
    ultimo_id_processado = str(ctrl[0]["ultimo_id"])
else:
    ultimo_id_processado = "0"

print(f"Último id_referencia processado: {ultimo_id_processado}")

# CONSULTA A API
url = "https://fipe.parallelum.com.br/api/v2/references"
response = requests.get(url, headers=headers, timeout=30)
response.raise_for_status()
dados = response.json()

# MONTA A LISTA E FILTRA SÓ NOVOS IDS
lista_referencias = []
for linha in dados:
    id_referencia = str(linha["code"])
    mes_referencia = linha["month"]
    if id_referencia > ultimo_id_processado:
        lista_referencias.append({
            "id_referencia": id_referencia,
            "mes_referencia": mes_referencia
        })

# SALVA SOMENTE SE HOUVER NOVOS DADOS
if lista_referencias:
    df_pandas = pd.DataFrame(lista_referencias)
    df_spark = spark.createDataFrame(df_pandas)
    display(df_spark)
    df_spark.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("append") \
        .saveAsTable(tabela_destino)
    print(f"Dados salvos com sucesso em: {tabela_destino}")
    # ATUALIZA O CONTROLE INCREMENTAL
    ultimo_id = max(item["id_referencia"] for item in lista_referencias)
    data_atual = datetime.now()
    spark.sql(f"""
    UPDATE {tabela_controle}
    SET ultimo_id = '{ultimo_id}', ultima_execucao = current_timestamp()
    WHERE processo = 'referencias'
    """)
    print("Controle incremental atualizado para referencias.")
else:
    print("Nenhuma referência nova encontrada.")
