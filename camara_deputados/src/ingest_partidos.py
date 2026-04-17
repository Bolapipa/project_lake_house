# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão de Partidos - Câmara dos Deputados
# MAGIC
# MAGIC Ingestão completa de dados de partidos políticos da API de Dados Abertos da Câmara dos Deputados.
# MAGIC Esta ingestão captura todos os partidos registrados e suas informações básicas.
# MAGIC
# MAGIC **Fonte**: https://dadosabertos.camara.leg.br/api/v2/partidos
# MAGIC **Tipo**: Ingestão completa (dimensão)
# MAGIC **Controle**: Baseado no último ID de partido processado

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

# Definição das tabelas
tabela_controle = f"{used_catalog}.{used_schema}.controle_ingestao"
tabela_destino = f"{used_catalog}.{used_schema}.raw_partidos"

print(f"Catálogo: {used_catalog}")
print(f"Schema: {used_schema}")
print(f"Tabela controle: {tabela_controle}")
print(f"Tabela destino: {tabela_destino}")

# COMMAND ----------

# Busca o último ID de partido processado
ctrl_id = spark.sql(
    f"SELECT raw_partidos FROM {tabela_controle} WHERE id = 1"
).collect()

if ctrl_id and ctrl_id[0][0] is not None:
    ultimo_id = int(ctrl_id[0][0])
else:
    ultimo_id = 0

print(f"Último ID de partido processado: {ultimo_id}")

# COMMAND ----------

# Ingestão de dados da API de Partidos
API_URL = "https://dadosabertos.camara.leg.br/api/v2/partidos?itens=100&ordem=ASC&ordenarPor=id"
all_rows = []

print("Iniciando ingestão de partidos...")
response = requests.get(API_URL)

if response.status_code == 200:
    dados = response.json()
    
    for partido in dados.get("dados", []):
        partido_id = int(partido.get("id"))
        
        # Filtro incremental - só processa partidos novos
        if partido_id <= ultimo_id:
            continue
        
        # Extrair dados relevantes
        row = (
            partido_id,                              # id
            partido.get("sigla"),                    # sigla
            partido.get("nome"),                     # nome
            partido.get("uri"),                      # uri
            json.dumps(partido.get("status"))        # status (JSON)
        )
        all_rows.append(row)
    
    print(f"Total de novos partidos encontrados: {len(all_rows)}")
else:
    print(f"Erro ao acessar API: {response.status_code}")
    raise Exception(f"Falha na requisição: {response.status_code}")

# COMMAND ----------

# Gravação dos dados e atualização do controle
if all_rows:
    # Criar DataFrame com schema definido
    df_partidos = spark.createDataFrame(
        all_rows,
        [
            "id",
            "sigla",
            "nome",
            "uri",
            "status"
        ]
    )
    
    # Exibir preview dos dados
    display(df_partidos)
    
    # Salvar na tabela Bronze
    df_partidos.write.mode("append").option("mergeSchema", "true").saveAsTable(tabela_destino)
    
    # Atualizar tabela de controle com o último ID processado
    novo_ultimo_id = all_rows[-1][0]
    
    spark.sql(f"""
        UPDATE {tabela_controle}
        SET raw_partidos = {novo_ultimo_id}
        WHERE id = 1
    """)
    
    print(f"Ingestão concluída com sucesso!")
    print(f"Ultimo ID atualizado: {novo_ultimo_id}")
    print(f"Total de partidos inseridos: {len(all_rows)}")
else:
    print("Nenhum novo partido para inserir.")
