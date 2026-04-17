# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão de Deputados - Câmara dos Deputados
# MAGIC
# MAGIC Ingestão incremental de dados de deputados da API de Dados Abertos da Câmara dos Deputados.
# MAGIC
# MAGIC **Fonte**: https://dadosabertos.camara.leg.br/api/v2/deputados

# COMMAND ----------

import requests
import json
import time
from datetime import datetime

# COMMAND ----------

# Configuração de parâmetros via widgets
dbutils.widgets.text("catalog", "bronze_dev")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_camara_deputados")
used_schema = dbutils.widgets.get("schema")

tabela_controle = f"{used_catalog}.{used_schema}.controle_ingestao"
tabela_destino = f"{used_catalog}.{used_schema}.raw_deputados"

print(f"Catálogo: {used_catalog}")
print(f"Schema: {used_schema}")
print(f"Tabela controle: {tabela_controle}")
print(f"Tabela destino: {tabela_destino}")

# COMMAND ----------

# Busca o último ID salvo (necessário para comparar IDs)
ctrl_id = spark.sql(
    f"SELECT raw_deputados FROM {tabela_controle} WHERE id = 1"
).collect()

if ctrl_id and ctrl_id[0][0] is not None:
    ultimo_id = int(ctrl_id[0][0])
else:
    ultimo_id = 0

print(f"Último ID salvo: {ultimo_id}")

# COMMAND ----------

# Ingestão de dados da API
API_URL = "https://dadosabertos.camara.leg.br/api/v2/deputados?ordem=ASC&ordenarPor=id&itens=1000"
all_rows = []

print("Iniciando ingestão...")
response = requests.get(API_URL)

if response.status_code == 200:
    dados = response.json()
    
    for deputado in dados.get("dados", []):
        deputado_id = int(deputado.get("id"))
        
        # Filtro incremental
        if deputado_id <= ultimo_id:
            continue
        
        row = (
            deputado_id,
            deputado.get("uri"),
            deputado.get("nome"),
            deputado.get("siglaPartido"),
            deputado.get("uriPartido"),
            deputado.get("siglaUf"),
            deputado.get("idLegislatura"),
            deputado.get("urlFoto"),
            deputado.get("email")
        )
        all_rows.append(row)
    
    print(f"Total de novos registros: {len(all_rows)}")
else:
    print(f"Erro ao acessar API: {response.status_code}")

# COMMAND ----------

# Gravação dos dados e atualização do controle
if all_rows:
    df_deputados = spark.createDataFrame(
        all_rows,
        [
            "id",
            "uri",
            "nome",
            "sigla_partido",
            "uri_partido",
            "sigla_uf",
            "id_legislatura",
            "url_foto",
            "email"
        ]
    )
    
    display(df_deputados)
    
    df_deputados.write.mode("append").option("mergeSchema", "true").saveAsTable(tabela_destino)
    
    novo_ultimo_id = all_rows[-1][0]
    
    spark.sql(f"""
        UPDATE {tabela_controle}
        SET raw_deputados = {novo_ultimo_id}
        WHERE id = 1
    """)
    
    print(f"Último ID atualizado na tabela de controle: {novo_ultimo_id}")
    print(f"Total de registros inseridos: {len(all_rows)}")
else:
    print("Nenhum novo registro para inserir.")
