# Databricks notebook source
# DBTITLE 1,Ingestão FIPE – controle id_ano tipo string
dbutils.widgets.text("catalog", "bronze_prod")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_carros")
used_schema = dbutils.widgets.get("schema")

tabela_origem = f"{used_catalog}.{used_schema}.raw_anos"
tabela_referencias = f"{used_catalog}.{used_schema}.raw_referencias"
tabela_destino = f"{used_catalog}.{used_schema}.raw_fipe"
tabela_controle = f"{used_catalog}.{used_schema}.tabela_controle"

import requests
import pandas as pd
import time
from datetime import datetime
from pyspark.sql.functions import col, max as spark_max

token_api = dbutils.secrets.get(scope="carros", key="fipe-token")

headers = {}
if token_api:
    headers["X-Subscription-Token"] = token_api

# LÊ O CONTROLE INCREMENTAL
ctrl = spark.sql(f"""
SELECT ultimo_id
FROM {tabela_controle}
WHERE processo = 'fipe'
""").collect()

if ctrl and ctrl[0]["ultimo_id"] is not None:
    ultimo_id_processado = str(ctrl[0]["ultimo_id"])
else:
    ultimo_id_processado = '0'

print(f"Último id_ano processado: {ultimo_id_processado}")

# PEGA A REFERÊNCIA MAIS RECENTE
ref_row = (
    spark.table(tabela_referencias)
    .select(
        col("id_referencia").cast("int").alias("id_referencia"),
        col("mes_referencia")
    )
    .orderBy(col("id_referencia").desc())
    .limit(1)
    .collect()[0]
)

id_referencia = int(ref_row["id_referencia"])
mes_referencia = ref_row["mes_referencia"]

print(f"Referência utilizada: {id_referencia} - {mes_referencia}")

# LÊ APENAS OS REGISTROS NOVOS
# Garante que id_ano usado seja string, igual à tabela controle
df_anos = (
    spark.table(tabela_origem)
    .filter(col("id_ano") > ultimo_id_processado)
)

anos = df_anos.select(
    "id_marca",
    "nome_marca",
    "id_modelo",
    "nome_modelo",
    "id_ano",
    "nome_ano"
).collect()

print(f"Total de combinações novas para consultar: {len(anos)}")

lista_fipe = []
lista_erros = []

# CONSULTA A API
for i, registro in enumerate(anos, start=1):
    id_marca = registro["id_marca"]
    nome_marca = registro["nome_marca"]
    id_modelo = registro["id_modelo"]
    nome_modelo = registro["nome_modelo"]
    id_ano = registro["id_ano"]
    nome_ano = registro["nome_ano"]

    url = f"https://fipe.parallelum.com.br/api/v2/cars/brands/{id_marca}/models/{id_modelo}/years/{id_ano}"

    tentativas = 0
    sucesso = False

    while tentativas < 3 and not sucesso:
        try:
            response = requests.get(
                url,
                headers=headers,
                params={"reference": id_referencia},
                timeout=30
            )

            if response.status_code == 429:
                tentativas += 1
                espera = 5 * tentativas
                print(f"[{i}/{len(anos)}] 429 para marca {id_marca}, modelo {id_modelo}, ano {id_ano}. Esperando {espera}s...")
                time.sleep(espera)
                continue

            response.raise_for_status()
            dado = response.json()

            lista_fipe.append({
                "id_referencia": id_referencia,
                "mes_referencia": mes_referencia,
                "id_marca": id_marca,
                "nome_marca": nome_marca,
                "id_modelo": id_modelo,
                "nome_modelo": nome_modelo,
                "id_ano": id_ano,
                "nome_ano": nome_ano,
                "valor": dado.get("price"),
                "modelo_fipe": dado.get("model"),
                "ano_modelo": dado.get("modelYear"),
                "combustivel": dado.get("fuel"),
                "codigo_fipe": dado.get("codeFipe"),
                "mes_referencia_fipe": dado.get("referenceMonth")
            })

            sucesso = True
            time.sleep(0.5)

        except Exception as e:
            tentativas += 1

            if tentativas >= 3:
                lista_erros.append({
                    "id_referencia": id_referencia,
                    "id_marca": id_marca,
                    "id_modelo": id_modelo,
                    "id_ano": id_ano,
                    "erro": str(e)
                })
                print(f"[{i}/{len(anos)}] Falha definitiva: marca {id_marca}, modelo {id_modelo}, ano {id_ano} -> {e}")
            else:
                time.sleep(3)

# SALVA NA TABELA DESTINO COM APPEND
if lista_fipe:
    df_pandas = pd.DataFrame(lista_fipe)
    df_spark = spark.createDataFrame(df_pandas)

    display(df_spark)

    df_spark.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("append") \
        .saveAsTable(tabela_destino)

    print(f"Dados salvos com sucesso em: {tabela_destino}")

    # ATUALIZA O CONTROLE INCREMENTAL
    # Sempre como string
    ultimo_id = df_spark.agg(spark_max("id_ano").alias("ultimo_id")).collect()[0]["ultimo_id"]
    data_atual = datetime.now()

    controle_df = spark.createDataFrame([
        {
            "processo": "fipe",
            "ultimo_id": str(ultimo_id),
            "ultima_execucao": data_atual
        }
    ])

    controle_df.write.format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", "processo = 'fipe'") \
        .saveAsTable(tabela_controle)

    print("Controle incremental atualizado para fipe.")
else:
    print("Nenhum dado novo encontrado para ingestão.")

print(f"Total de erros: {len(lista_erros)}")

if lista_erros:
    df_erros = spark.createDataFrame(pd.DataFrame(lista_erros))
    display(df_erros)
