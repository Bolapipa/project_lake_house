# Databricks notebook source
# Define o destino da tabela

dbutils.widgets.text("catalog", "bronze_dev")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_ibge")
used_schema = dbutils.widgets.get("schema")

tabela_destino = f"{used_catalog}.{used_schema}.raw_distritos"

print(f"Catálogo: {used_catalog}")
print(f"Schema: {used_schema}")
print(f"Tabela destino: {tabela_destino}")

import requests
import json
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# Endpoint oficial de distritos
url = "https://servicodados.ibge.gov.br/api/v1/localidades/distritos"

# Faz a chamada da API
response = requests.get(url, timeout=120)
response.raise_for_status()

dados = response.json()

# Valida se a API retornou dados
if not dados:
    raise ValueError("A API do IBGE retornou uma lista vazia para distritos.")

# Prepara os dados para a bronze
# Campos simples ficam normais
# Campos aninhados viram string JSON
dados_tratados = []

for item in dados:
    linha = {}

    for chave, valor in item.items():
        nome_coluna = chave.replace("-", "_")

        if isinstance(valor, (dict, list)):
            linha[nome_coluna] = json.dumps(valor, ensure_ascii=False)
        else:
            linha[nome_coluna] = valor

    dados_tratados.append(linha)

# Cria o DataFrame Spark com os dados novos
df_novos = spark.createDataFrame(dados_tratados)

# Adiciona timestamp e hash para cada registro
df_novos = df_novos.withColumn("data_ingestao", F.lit(datetime.now()))

# Cria hash de todos os campos exceto id para comparação
cols_para_hash = [col for col in df_novos.columns if col not in ["id", "data_ingestao"]]
df_novos = df_novos.withColumn("hash_registro", F.sha2(F.concat_ws("|", *cols_para_hash), 256))

# Garante que o schema existe
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {used_catalog}.{used_schema}")

# Verifica se a tabela já existe
if spark.catalog.tableExists(tabela_destino):
    df_existente = spark.table(tabela_destino)
    
    # Pega a última versão de cada registro (por ID)
    window = Window.partitionBy("id").orderBy(F.desc("data_ingestao"))
    df_ultima_versao = df_existente.withColumn("rank", F.row_number().over(window)) \
        .filter("rank = 1") \
        .select("id", "hash_registro")
    
    # Identifica registros novos ou modificados (hash diferente ou ID novo)
    df_comparacao = df_novos.alias("novo").join(
        df_ultima_versao.alias("antigo"),
        on="id",
        how="left"
    )
    
    # Filtra apenas registros novos (sem match) ou modificados (hash diferente)
    df_para_inserir = df_comparacao.filter(
        (F.col("antigo.hash_registro").isNull()) | 
        (F.col("novo.hash_registro") != F.col("antigo.hash_registro"))
    ).select("novo.*")
    
    # Conta registros para inserir
    count_inserir = df_para_inserir.count()
    
    if count_inserir == 0:
        print("✓ Nenhuma mudança detectada. Tabela não foi atualizada.")
        print(f"Total de registros na tabela: {df_existente.select('id').distinct().count()}")
    else:
        # Faz append dos registros novos/modificados
        df_para_inserir.write.format("delta").mode("append").saveAsTable(tabela_destino)
        print(f"✓ {count_inserir} registros novos ou atualizados inseridos.")
        print(f"Tabela atualizada: {tabela_destino}")
        
        # Mostra resumo
        df_final = spark.table(tabela_destino)
        print(f"Total de registros históricos: {df_final.count()}")
        print(f"Total de IDs únicos: {df_final.select('id').distinct().count()}")
else:
    # Primeira carga - insere tudo
    df_novos.write.format("delta").mode("overwrite").saveAsTable(tabela_destino)
    print(f"✓ Tabela criada: {tabela_destino}")
    print(f"Quantidade de registros carregados: {df_novos.count()}")

# Exibe a última versão de cada registro
df_display = spark.table(tabela_destino)
window_display = Window.partitionBy("id").orderBy(F.desc("data_ingestao"))
df_ultima = df_display.withColumn("rank", F.row_number().over(window_display)) \
    .filter("rank = 1") \
    .drop("rank", "hash_registro") \
    .orderBy("id")

print("\nÚltima versão de cada registro:")
display(df_ultima)
