# Databricks notebook source
# DBTITLE 1,Cell 1: Corrige comparação de tipos
dbutils.widgets.text("catalog", "bronze_dev")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_carros")
used_schema = dbutils.widgets.get("schema")

tabela_origem = f"{used_catalog}.{used_schema}.raw_modelos"
tabela_destino = f"{used_catalog}.{used_schema}.raw_anos"
tabela_controle = f"{used_catalog}.{used_schema}.controle_anos"

import requests
import pandas as pd
import time
from datetime import datetime
from pyspark.sql.functions import col, max as spark_max

# le tabela controle
ctrl = spark.sql(f"""
SELECT ultimo_id
FROM {tabela_controle}
WHERE processo = 'ingest_anos'
""").collect()[0]

marca_inicio = int(ctrl["ultimo_id"])
tamanho_lote = 5  # Ajuste conforme desejado
marca_fim = marca_inicio + tamanho_lote - 1

print(f"Próximo lote automático: marcas de {marca_inicio} até {marca_fim}")

# DESCOBRE O MAIOR id_marca EXISTENTE
max_id_marca = (
    spark.table(tabela_origem)
    .agg(spark_max("id_marca").alias("max_id_marca"))
    .collect()[0]["max_id_marca"]
)
max_id_marca = int(max_id_marca)

if max_id_marca is None:
    raise Exception(f"Nenhum dado encontrado em {tabela_origem}")

print(f"Maior id_marca encontrado: {max_id_marca}")

# SE JÁ TERMINOU A HISTÓRICA, ENCERRA
if marca_inicio > max_id_marca:
    print("Carga histórica de anos concluída. Não há mais lotes para processar.")

    spark.sql(f"""
    UPDATE {tabela_controle}
    SET
        ultima_execucao = current_timestamp(),
        status = 'CONCLUIDO'
    WHERE processo = 'ingest_anos'
    """)

else:
    if marca_fim > max_id_marca:
        marca_fim = max_id_marca

    print(f"Lote ajustado para processar marcas de {marca_inicio} até {marca_fim}")

    df_modelos = (
        spark.table(tabela_origem)
        .filter((col("id_marca") >= marca_inicio) & (col("id_marca") <= marca_fim))
    )

    modelos = df_modelos.select(
        "id_marca",
        "nome_marca",
        "id_modelo",
        "nome_modelo"
    ).collect()

    print(f"Total de modelos no lote: {len(modelos)}")

    lista_anos = []
    lista_erros = []

    token_api = None
    headers = {}
    if token_api:
        headers["X-Subscription-Token"] = token_api

    for i, modelo in enumerate(modelos, start=1):
        id_marca = modelo["id_marca"]
        nome_marca = modelo["nome_marca"]
        id_modelo = modelo["id_modelo"]
        nome_modelo = modelo["nome_modelo"]
        url = f"https://fipe.parallelum.com.br/api/v2/cars/brands/{id_marca}/models/{id_modelo}/years"
        tentativas = 0
        sucesso = False
        while tentativas < 3 and not sucesso:
            try:
                response = requests.get(url, headers=headers, timeout=30)
                if response.status_code == 429:
                    tentativas += 1
                    espera = 5 * tentativas
                    print(f"[{i}/{len(modelos)}] 429 para marca {id_marca}, modelo {id_modelo}. Esperando {espera}s...")
                    time.sleep(espera)
                    continue
                response.raise_for_status()
                dados = response.json()
                for ano in dados:
                    lista_anos.append({
                        "id_marca": id_marca,
                        "nome_marca": nome_marca,
                        "id_modelo": id_modelo,
                        "nome_modelo": nome_modelo,
                        "id_ano": ano["code"],
                        "nome_ano": ano["name"],
                        "data_ingestao": datetime.now()
                    })
                sucesso = True
                time.sleep(0.5)
            except Exception as e:
                tentativas += 1
                if tentativas >= 3:
                    lista_erros.append({
                        "id_marca": id_marca,
                        "nome_marca": nome_marca,
                        "id_modelo": id_modelo,
                        "nome_modelo": nome_modelo,
                        "erro": str(e),
                        "data_erro": datetime.now()
                    })
                    print(f"[{i}/{len(modelos)}] Falha definitiva: marca {id_marca}, modelo {id_modelo} -> {e}")
                else:
                    time.sleep(3)
    if lista_anos:
        df_pandas = pd.DataFrame(lista_anos)
        df_spark = spark.createDataFrame(df_pandas)
        display(df_spark)
        df_spark.write.format("delta") \
            .option("mergeSchema", "true") \
            .mode("append") \
            .saveAsTable(tabela_destino)
        print(f"Dados salvos com sucesso em: {tabela_destino}")
        print(f"Total de linhas inseridas neste lote: {len(lista_anos)}")
    else:
        print("Nenhum dado retornado para este lote.")
    print(f"Total de erros: {len(lista_erros)}")
    proxima_marca = marca_fim + 1
    novo_status = "EM_ANDAMENTO"
    if proxima_marca > max_id_marca:
        novo_status = "CONCLUIDO"
    spark.sql(f"""
    UPDATE {tabela_controle}
    SET
        ultimo_id = {proxima_marca},
        ultima_execucao = current_timestamp(),
        status = '{novo_status}'
    WHERE processo = 'ingest_anos'
    """)
    print(f"Controle atualizado. Próxima execução começará em: {proxima_marca}")
    if lista_erros:
        df_erros = spark.createDataFrame(pd.DataFrame(lista_erros))
        display(df_erros)
