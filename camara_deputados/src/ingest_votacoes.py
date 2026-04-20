# Databricks notebook source
# DBTITLE 1,Descrição da Ingestão
# MAGIC %md
# MAGIC # Ingestão de Votações - Câmara dos Deputados
# MAGIC
# MAGIC Ingestão incremental de votações do Plenário e Comissões da Câmara dos Deputados.
# MAGIC Esta ingestão captura votações nominais com os votos de cada deputado.
# MAGIC
# MAGIC **Fonte**: https://dadosabertos.camara.leg.br/api/v2/votacoes
# MAGIC **Tipo**: Ingestão incremental por data/hora
# MAGIC **Controle**: Baseado na última data/hora de votação processada

# COMMAND ----------

# DBTITLE 1,Imports
import requests
import time
from datetime import datetime, timedelta

# COMMAND ----------

# DBTITLE 1,Parâmetros
# Parâmetros
dbutils.widgets.text("catalog", "bronze_dev")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_camara_deputados")
used_schema = dbutils.widgets.get("schema")

dbutils.widgets.text("dias_retroativos", "30")
dias_retroativos = int(dbutils.widgets.get("dias_retroativos"))

tabela_controle = f"{used_catalog}.{used_schema}.controle_ingestao"
tabela_destino = f"{used_catalog}.{used_schema}.raw_votacoes"

print(f"Catálogo: {used_catalog}")
print(f"Schema: {used_schema}")
print(f"Dias retroativos: {dias_retroativos}")
print(f"Tabela controle: {tabela_controle}")
print(f"Tabela destino: {tabela_destino}")

# COMMAND ----------

# DBTITLE 1,Busca Última Data Processada
# Busca a última data/hora processada
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
    ultima_data = datetime.now() - timedelta(days=dias_retroativos)

data_inicial = ultima_data.strftime("%Y-%m-%d")
data_final = datetime.now().strftime("%Y-%m-%d")

print(f"Última data/hora processada: {ultima_data.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Período de busca: {data_inicial} até {data_final}")

# COMMAND ----------

# DBTITLE 1,Ingestão de Votações
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

        if response.status_code != 200:
            print(f"Erro HTTP {response.status_code} na página {pagina}")
            break

        dados_json = response.json()
        dados = dados_json.get("dados", [])

        if pagina == 1:
            links = dados_json.get("links", [])

            for link in links:
                if link.get("rel") == "last":
                    href = link.get("href", "")

                    if "pagina=" in href:
                        total_paginas = int(href.split("pagina=")[1].split("&")[0])
                        break

            print(f"Total de páginas: {total_paginas}")

        print(f"Processando página {pagina}/{total_paginas} - {len(dados)} votações")

        for votacao in dados:
            data_hora_registro = votacao.get("dataHoraRegistro")

            # Filtro incremental
            if data_hora_registro:
                try:
                    data_hora_vot = datetime.strptime(data_hora_registro, "%Y-%m-%dT%H:%M")
                    if data_hora_vot <= ultima_data:
                        continue
                except:
                    pass

            # Trata proposição
            proposicao = votacao.get("proposicaoObjeto")
            proposicao_uri = votacao.get("uriProposicaoObjeto")

            prop_cod_tipo = None
            prop_numero = None
            prop_ano = None

            if proposicao:
                try:
                    partes = proposicao.split()

                    if len(partes) > 0:
                        prop_cod_tipo = partes[0]

                    if len(partes) > 1:
                        prop_numero = int(partes[1])

                    if len(partes) > 2:
                        prop_ano = int(partes[2].replace("/", ""))
                except:
                    prop_cod_tipo = None
                    prop_numero = None
                    prop_ano = None

            # Monta a linha como dicionário
            row = {
                "id": int(votacao["id"]) if votacao.get("id") is not None else None,
                "data": str(votacao["data"]) if votacao.get("data") else None,
                "data_hora_registro": str(data_hora_registro) if data_hora_registro else None,
                "sigla_orgao": str(votacao["siglaOrgao"]) if votacao.get("siglaOrgao") else None,
                "id_orgao": int(votacao["idOrgao"]) if votacao.get("idOrgao") is not None else None,
                "uri_orgao": str(votacao["uriOrgao"]) if votacao.get("uriOrgao") else None,
                "uri_evento": str(votacao["uriEvento"]) if votacao.get("uriEvento") else None,
                "proposicao_cod_tipo": str(prop_cod_tipo) if prop_cod_tipo else None,
                "proposicao_numero": int(prop_numero) if prop_numero is not None else None,
                "proposicao_ano": int(prop_ano) if prop_ano is not None else None,
                "uri_proposicao_votada": str(proposicao_uri) if proposicao_uri else None,
                "aprovacao": int(votacao["aprovacao"]) if votacao.get("aprovacao") is not None else 0,
                "objeto_votacao": str(votacao["descricao"]) if votacao.get("descricao") else None
            }

            all_rows.append(row)

        pagina += 1
        time.sleep(0.2)

    except Exception as e:
        print(f"Erro ao processar página {pagina}: {str(e)}")
        break

print("\nIngestão concluída!")
print(f"Total de votações coletadas: {len(all_rows)}")
print(f"Total de páginas processadas: {pagina - 1}")

# COMMAND ----------

# DBTITLE 1,Gravação dos Dados
if all_rows:
    # Deixa o Spark inferir automaticamente
    df_votacoes = spark.createDataFrame(all_rows)

    print("\nPreview das votações coletadas:")
    display(df_votacoes)

    df_votacoes.write \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(tabela_destino)

    # Atualiza controle com a data/hora mais recente
    votacoes_com_data = [r for r in all_rows if r["data_hora_registro"]]

    if votacoes_com_data:
        votacoes_ordenadas = sorted(
            votacoes_com_data,
            key=lambda x: x["data_hora_registro"],
            reverse=True
        )
        nova_data_hora = votacoes_ordenadas[0]["data_hora_registro"].replace("T", " ")[:19]
    else:
        nova_data_hora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    spark.sql(f"""
        UPDATE {tabela_controle}
        SET raw_votacoes = '{nova_data_hora}'
        WHERE id = 1
    """)

    print("\nDados gravados com sucesso!")
    print(f"Última data/hora atualizada: {nova_data_hora}")
    print(f"Total de votações inseridas: {len(all_rows)}")

    print("\nEstatísticas:")
    print(f"Votações aprovadas: {sum(1 for r in all_rows if r['aprovacao'] == 1)}")
    print(f"Votações rejeitadas: {sum(1 for r in all_rows if r['aprovacao'] == 0)}")

    df_votacoes.groupBy("sigla_orgao") \
        .count() \
        .orderBy("count", ascending=False) \
        .show(10)

else:
    print("Nenhuma votação nova para inserir.")
