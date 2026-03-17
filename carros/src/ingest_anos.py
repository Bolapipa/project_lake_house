# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "4"
# ///
# DBTITLE 1,Cell 1: Corrige comparação de tipos
# ==============================
# WIDGETS
# ==============================
dbutils.widgets.text("catalog", "bronze_dev")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_carros")
used_schema = dbutils.widgets.get("schema")

dbutils.widgets.text("batch_marcas", "5")
batch_marcas = int(dbutils.widgets.get("batch_marcas"))

tabela_modelos = f"{used_catalog}.{used_schema}.raw_modelos"
tabela_destino = f"{used_catalog}.{used_schema}.raw_anos"
tabela_controle = f"{used_catalog}.{used_schema}.controle_anos"

print("Catálogo:", used_catalog)
print("Schema:", used_schema)
print("Tabela origem:", tabela_modelos)
print("Tabela destino:", tabela_destino)
print("Tabela controle:", tabela_controle)
print("Tamanho do lote de marcas:", batch_marcas)

# ==============================
# IMPORTS
# ==============================
import requests
import pandas as pd
import time
from pyspark.sql.functions import current_timestamp, col

# ==============================
# TOKEN
# ==============================
token_api = dbutils.secrets.get(scope="carros", key="fipe-token")

headers = {
    "accept": "application/json",
    "X-Subscription-Token": token_api
}

session = requests.Session()
session.headers.update(headers)

# ==============================
# GARANTE LINHA NA TABELA CONTROLE
# ==============================
ctrl_exist = spark.sql(f"""
    SELECT COUNT(*) AS qtd
    FROM {tabela_controle}
    WHERE processo = 'ingest_anos'
""").collect()[0]["qtd"]

if ctrl_exist == 0:
    spark.sql(f"""
        INSERT INTO {tabela_controle} (processo, ultimo_id, ultima_execucao, status)
        VALUES ('ingest_anos', 0, current_timestamp(), 'PENDENTE')
    """)
    print("Linha inicial criada na tabela controle.")

# ==============================
# LÊ O CONTROLE
# ==============================
ctrl = spark.sql(f"""
    SELECT ultimo_id, status
    FROM {tabela_controle}
    WHERE processo = 'ingest_anos'
""").collect()

if ctrl and ctrl[0]["ultimo_id"] is not None:
    ultimo_id_processado = int(ctrl[0]["ultimo_id"])
    status_atual = ctrl[0]["status"]
else:
    ultimo_id_processado = 0
    status_atual = None

print(f"Último id processado no controle: {ultimo_id_processado}")
print(f"Status atual no controle: {status_atual}")

# ==============================
# DEFINE FILTRO DAS MARCAS
# ==============================
if status_atual in ("RATE_LIMIT", "ERRO"):
    filtro_marcas = col("id_marca") >= ultimo_id_processado
else:
    filtro_marcas = col("id_marca") > ultimo_id_processado

# ==============================
# BUSCA MARCAS A PROCESSAR
# ==============================
df_marcas = (
    spark.table(tabela_modelos)
    .select("id_marca", "nome_marca")
    .distinct()
    .filter(filtro_marcas)
    .orderBy("id_marca")
)

marcas_disponiveis = df_marcas.collect()

if not marcas_disponiveis:
    print("Nenhuma marca pendente para processar.")

    spark.sql(f"""
        UPDATE {tabela_controle}
        SET
            ultima_execucao = current_timestamp(),
            status = 'SEM_DADOS'
        WHERE processo = 'ingest_anos'
    """)

    print("Tabela de controle atualizada com status SEM_DADOS.")

else:
    marcas_lote = marcas_disponiveis[:batch_marcas]
    marca_inicio = min([row["id_marca"] for row in marcas_lote])
    marca_fim = max([row["id_marca"] for row in marcas_lote])

    print(f"Processando marcas de {marca_inicio} até {marca_fim}")

    # ==============================
    # MODELOS DA(S) MARCA(S) DO LOTE
    # ==============================
    df_modelos_lote = (
        spark.table(tabela_modelos)
        .select("id_marca", "id_modelo", "nome_marca", "nome_modelo")
        .distinct()
        .filter(col("id_marca").isin([row["id_marca"] for row in marcas_lote]))
        .orderBy("id_marca", "id_modelo")
    )

    modelos_lote = df_modelos_lote.collect()

    print(f"Total de modelos no lote: {len(modelos_lote)}")

    all_rows = []
    houve_rate_limit = False
    houve_erro = False
    interromper_lote = False

    # ==============================
    # PROCESSA CADA MODELO
    # ==============================
    for i, modelo in enumerate(modelos_lote, start=1):
        if interromper_lote:
            break

        id_marca = int(modelo["id_marca"])
        nome_marca = modelo["nome_marca"]
        id_modelo = int(modelo["id_modelo"])
        nome_modelo = modelo["nome_modelo"]

        url_years_modelo = f"https://fipe.parallelum.com.br/api/v2/cars/brands/{id_marca}/models/{id_modelo}/years"

        print(f"[{i}/{len(modelos_lote)}] Consultando anos da marca {id_marca} - {nome_marca}, modelo {id_modelo} - {nome_modelo}")

        max_tentativas = 3
        sucesso = False

        for tentativa in range(1, max_tentativas + 1):
            try:
                resp_years = session.get(url_years_modelo, timeout=30)

                if resp_years.status_code == 429:
                    print(f"[{i}/{len(modelos_lote)}] 429 ao consultar anos do modelo {id_modelo}. Tentativa {tentativa}/{max_tentativas}")
                    time.sleep(5)

                    if tentativa == max_tentativas:
                        houve_rate_limit = True
                        interromper_lote = True

                    continue

                if resp_years.status_code != 200:
                    print(f"[{i}/{len(modelos_lote)}] Erro {resp_years.status_code} ao consultar anos do modelo {id_modelo}")
                    print(resp_years.text)
                    houve_erro = True
                    break

                anos_modelo = resp_years.json()

                for ano in anos_modelo:
                    all_rows.append({
                        "id_marca": id_marca,
                        "nome_marca": nome_marca,
                        "id_modelo": id_modelo,
                        "nome_modelo": nome_modelo,
                        "id_ano": ano.get("code"),
                        "nome_ano": ano.get("name")
                    })

                sucesso = True
                time.sleep(1)
                break

            except Exception as e:
                print(f"[{i}/{len(modelos_lote)}] Erro ao consultar anos do modelo {id_modelo}: {str(e)}")
                time.sleep(3)

                if tentativa == max_tentativas:
                    houve_erro = True

        if not sucesso and houve_rate_limit:
            print("Rate limit detectado. Interrompendo o lote para retomar depois.")
            break

    # ==============================
    # SALVA RESULTADO
    # ==============================
    if all_rows:
        pdf = pd.DataFrame(all_rows)

        df_final = (
            spark.createDataFrame(pdf)
            .dropDuplicates(["id_marca", "id_modelo", "id_ano"])
            .withColumn("dt_ingestao", current_timestamp())
        )

        print(f"Total de linhas retornadas no lote: {df_final.count()}")
        display(df_final)

        if spark.catalog.tableExists(tabela_destino):
            df_final.write.mode("append").saveAsTable(tabela_destino)
        else:
            df_final.write.mode("overwrite").saveAsTable(tabela_destino)

        print(f"Dados salvos com sucesso em {tabela_destino}")
    else:
        print("Nenhum dado retornado neste lote.")

    # ==============================
    # STATUS FINAL
    # ==============================
    if houve_rate_limit:
        novo_status = "RATE_LIMIT"
        novo_ultimo_id = marca_inicio
    elif houve_erro:
        novo_status = "ERRO"
        novo_ultimo_id = marca_inicio
    else:
        novo_status = "SUCESSO"
        novo_ultimo_id = marca_fim

    print(f"Novo status: {novo_status}")
    print(f"Novo ultimo_id para controle: {novo_ultimo_id}")

    # ==============================
    # ATUALIZA CONTROLE
    # ==============================
    spark.sql(f"""
        UPDATE {tabela_controle}
        SET
            ultimo_id = {novo_ultimo_id},
            ultima_execucao = current_timestamp(),
            status = '{novo_status}'
        WHERE processo = 'ingest_anos'
    """)

    print("Tabela de controle atualizada com sucesso.")
