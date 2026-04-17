# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão de Votações - Câmara dos Deputados
# MAGIC
# MAGIC Ingestão incremental de votações do Plenário e Comissões da Câmara dos Deputados.
# MAGIC Esta ingestão captura votações nominais com os votos de cada deputado.
# MAGIC
# MAGIC **Fonte**: https://dadosabertos.camara.leg.br/api/v2/votacoes
# MAGIC **Tipo**: Ingestão incremental por data/hora
# MAGIC **Controle**: Baseado na última data/hora de votação processada
# MAGIC **Campos**: id, data, data_hora_registro, sigla_orgao, id_orgao, uri_orgao, uri_evento, proposicao_cod_tipo, proposicao_numero, proposicao_ano, uri_proposicao_votada, aprovacao, objeto_votacao

# COMMAND ----------

import requests
import json
import time
from datetime import datetime, timedelta

# COMMAND ----------

# Configuração de parâmetros via widgets
dbutils.widgets.text("catalog", "bronze_dev")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_camara_deputados")
used_schema = dbutils.widgets.get("schema")

# Número de dias retroativos para buscar votações (padrão: 30)
dbutils.widgets.text("dias_retroativos", "30")
dias_retroativos = int(dbutils.widgets.get("dias_retroativos"))

# Definição das tabelas
tabela_controle = f"{used_catalog}.{used_schema}.controle_ingestao"
tabela_destino = f"{used_catalog}.{used_schema}.raw_votacoes"

print(f"Catálogo: {used_catalog}")
print(f"Schema: {used_schema}")
print(f"Dias retroativos: {dias_retroativos}")
print(f"Tabela controle: {tabela_controle}")
print(f"Tabela destino: {tabela_destino}")

# COMMAND ----------

# Busca a última data/hora de votação processada
ctrl_data = spark.sql(
    f"SELECT raw_votacoes FROM {tabela_controle} WHERE id = 1"
).collect()

if ctrl_data and ctrl_data[0][0] is not None:
    ultima_data_str = ctrl_data[0][0]
    try:
        ultima_data = datetime.strptime(ultima_data_str, "%Y-%m-%d %H:%M:%S")
    except:
        ultima_data = datetime.strptime(ultima_data_str, "%Y-%m-%d")
else:
    # Se não há controle, pega N dias atrás
    ultima_data = datetime.now() - timedelta(days=dias_retroativos)

# Calcular data inicial e final para busca
data_inicial = ultima_data.strftime("%Y-%m-%d")
data_final = datetime.now().strftime("%Y-%m-%d")

print(f"Última data/hora processada: {ultima_data.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Período de busca: {data_inicial} até {data_final}")

# COMMAND ----------

# Ingestão de votações da API com paginação automática
API_URL = "https://dadosabertos.camara.leg.br/api/v2/votacoes"
all_rows = []
pagina = 1
total_paginas = 1

print("Iniciando ingestão de votações...")

while pagina <= total_paginas:
    try:
        params = {
            "dataInicio": data_inicial,
            "dataFim": data_final,
            "ordem": "ASC",
            "ordenarPor": "dataHoraRegistro",
            "pagina": pagina,
            "itens": 100
        }
        
        response = requests.get(API_URL, params=params, timeout=15)
        
        if response.status_code == 200:
            dados_json = response.json()
            dados = dados_json.get("dados", [])
            
            # Atualizar total de páginas (apenas na primeira iteração)
            if pagina == 1:
                links = dados_json.get("links", [])
                for link in links:
                    if link.get("rel") == "last":
                        href = link.get("href", "")
                        if "pagina=" in href:
                            total_paginas = int(href.split("pagina=")[1].split("&")[0])
                
                print(f"Total de páginas: {total_paginas}")
            
            print(f"Processando página {pagina}/{total_paginas} - {len(dados)} votações")
            
            for votacao in dados:
                data_hora_registro = votacao.get("dataHoraRegistro", "")
                
                # Filtro incremental por data/hora
                if data_hora_registro:
                    try:
                        data_hora_vot = datetime.strptime(data_hora_registro, "%Y-%m-%dT%H:%M")
                        if data_hora_vot <= ultima_data:
                            continue
                    except:
                        pass
                
                # Extrair informações da proposição votada
                proposicao = votacao.get("proposicaoObjeto", None)
                proposicao_uri = votacao.get("uriProposicaoObjeto", "")
                
                if proposicao:
                    try:
                        partes = proposicao.split()
                        prop_cod_tipo = partes[0] if len(partes) > 0 else None
                        prop_numero = int(partes[1]) if len(partes) > 1 else 0
                        prop_ano = int(partes[2].replace("/", "")) if len(partes) > 2 else 0
                    except:
                        prop_cod_tipo = None
                        prop_numero = 0
                        prop_ano = 0
                else:
                    prop_cod_tipo = None
                    prop_numero = 0
                    prop_ano = 0
                
                # Extrair dados relevantes
                row = (
                    votacao.get("id"),                         # id
                    votacao.get("data"),                       # data
                    votacao.get("dataHoraRegistro"),           # data_hora_registro
                    votacao.get("siglaOrgao"),                 # sigla_orgao
                    votacao.get("idOrgao"),                    # id_orgao
                    votacao.get("uriOrgao"),                   # uri_orgao
                    votacao.get("uriEvento"),                  # uri_evento
                    prop_cod_tipo,                             # proposicao_cod_tipo
                    prop_numero,                               # proposicao_numero
                    prop_ano,                                  # proposicao_ano
                    proposicao_uri,                            # uri_proposicao_votada
                    votacao.get("aprovacao") or 0,             # aprovacao (int: 1=aprovado, 0=rejeitado)
                    votacao.get("descricao")                   # objeto_votacao
                )
                all_rows.append(row)
            
            # Próxima página
            pagina += 1
            time.sleep(0.2)  # Rate limiting
            
        else:
            print(f"Erro HTTP {response.status_code} na página {pagina}")
            break
            
    except Exception as e:
        print(f"Erro ao processar página {pagina}: {str(e)}")
        break

print(f"\nIngestão concluída!")
print(f"Total de votações coletadas: {len(all_rows)}")
print(f"Total de páginas processadas: {pagina - 1}")

# COMMAND ----------

# Gravação dos dados e atualização do controle
if all_rows:
    # Criar DataFrame com schema definido
    df_votacoes = spark.createDataFrame(
        all_rows,
        [
            "id",
            "data",
            "data_hora_registro",
            "sigla_orgao",
            "id_orgao",
            "uri_orgao",
            "uri_evento",
            "proposicao_cod_tipo",
            "proposicao_numero",
            "proposicao_ano",
            "uri_proposicao_votada",
            "aprovacao",
            "objeto_votacao"
        ]
    )
    
    # Exibir preview dos dados
    print("\nPreview das votações coletadas:")
    display(df_votacoes)
    
    # Salvar na tabela Bronze
    df_votacoes.write.mode("append").option("mergeSchema", "true").saveAsTable(tabela_destino)
    
    # Atualizar tabela de controle com a data/hora mais recente
    # Buscar a última data/hora das votações coletadas
    votacoes_ordenadas = sorted(all_rows, key=lambda x: x[2] if x[2] else "", reverse=True)
    if votacoes_ordenadas and votacoes_ordenadas[0][2]:
        nova_data_hora = votacoes_ordenadas[0][2]
        # Converter de ISO para formato padrão
        nova_data_hora = nova_data_hora.replace("T", " ")[:19]
    else:
        nova_data_hora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    spark.sql(f"""
        UPDATE {tabela_controle}
        SET raw_votacoes = '{nova_data_hora}'
        WHERE id = 1
    """)
    
    print(f"\nDados gravados com sucesso!")
    print(f"Ultima data/hora atualizada: {nova_data_hora}")
    print(f"Total de votações inseridas: {len(all_rows)}")
    
    # Estatísticas
    print("\nEstatísticas:")
    print(f"Votações aprovadas: {sum(1 for r in all_rows if r[11] == 1)}")
    print(f"Votações rejeitadas: {sum(1 for r in all_rows if r[11] == 0)}")
    df_votacoes.groupBy("sigla_orgao").count().orderBy("count", ascending=False).show(10)
    
else:
    print("Nenhuma votação nova para inserir.")
