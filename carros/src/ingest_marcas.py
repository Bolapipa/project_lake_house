# Databricks notebook source
# DBTITLE 1,Ingestão e controle automático de marcas FIPE
dbutils.widgets.text("catalog", "bronze_prod")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_carros")
used_schema = dbutils.widgets.get("schema")

tabela_destino = f"{used_catalog}.{used_schema}.raw_marcas"
tabela_controle = f"{used_catalog}.{used_schema}.tabela_controle"

import requests
import pandas as pd
from pyspark.sql.functions import col, max as spark_max
from datetime import datetime

# ==============================
# 1) CHAMA A API
# ==============================
url = "https://fipe.parallelum.com.br/api/v2/cars/brands"
response = requests.get(url, timeout=30)
response.raise_for_status()
dados = response.json()

# ==============================
# 2) MONTA DATAFRAME NOVO
# ==============================
df_pandas = pd.DataFrame(dados)

df_spark_novo = spark.createDataFrame(df_pandas).select(
    col("code").cast("int").alias("id_marca"),
    col("name").alias("nome_marca")
).dropDuplicates()

display(df_spark_novo)

# ==============================
# 3) SE A TABELA NÃO EXISTE, CRIA
# ==============================
if not spark.catalog.tableExists(tabela_destino):
    df_spark_novo.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(tabela_destino)

    print(f"Tabela criada e carga inicial realizada em: {tabela_destino}")

# ==============================
# 4) SE A TABELA JÁ EXISTE, INSERE SÓ NOVOS REGISTROS
# ==============================
else:
    df_existente = spark.table(tabela_destino).select("id_marca")

    df_somente_novos = df_spark_novo.join(
        df_existente,
        on="id_marca",
        how="left_anti"
    )

    qtd_novos = df_somente_novos.count()

    if qtd_novos > 0:
        df_somente_novos.write.format("delta") \
            .mode("append") \
            .saveAsTable(tabela_destino)

        print(f"{qtd_novos} novas marcas adicionadas em: {tabela_destino}")
    else:
        print("Nenhuma nova marca nova encontrada para inserir.")

# ==============================
# 5) ATUALIZA CONTROLE
# ==============================
ultimo_id = spark.table(tabela_destino) \
    .agg(spark_max("id_marca").alias("ultimo_id")) \
    .collect()[0]["ultimo_id"]

data_atual = datetime.now()

controle_df = spark.createDataFrame([
    {
        "processo": "marcas",
        "ultimo_id": str(ultimo_id),
        "ultima_execucao": data_atual
    }
])

# Se a tabela de controle não existir, cria
if not spark.catalog.tableExists(tabela_controle):
    controle_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(tabela_controle)

# Se já existir, substitui apenas o processo 'marcas'
else:
    controle_df.write.format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", "processo = 'marcas'") \
        .saveAsTable(tabela_controle)

print("Controle incremental atualizado para marcas.")
