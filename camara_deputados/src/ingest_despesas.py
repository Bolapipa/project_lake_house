# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão de Despesas dos Deputados - Câmara dos Deputados
# MAGIC
# MAGIC Ingestão incremental de despesas da Cota Parlamentar dos deputados.
# MAGIC Esta ingestão itera por todos os deputados e busca suas despesas a partir da última data processada.
# MAGIC
# MAGIC **Fonte**: https://dadosabertos.camara.leg.br/api/v2/deputados/{id}/despesas
# MAGIC **Tipo**: Ingestão incremental por data
# MAGIC **Controle**: Baseado na última data de despesa processada
# MAGIC **Categorias**: Combustível, Passagens, Hospedagem, Alimentação, Consultoria, etc
# MAGIC **Campos**: id_deputado, ano, mes, tipo_despesa, cod_documento, tipo_documento, data_documento, num_documento, valor_documento, url_documento, nome_fornecedor, cnpj_cpf_fornecedor, valor_liquido, valor_glosa

# COMMAND ----------

import requests
import json
import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# Configuração de parâmetros via widgets
dbutils.widgets.text("catalog", "bronze_dev")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_camara_deputados")
used_schema = dbutils.widgets.get("schema")

# Período de ingestão (meses retroativos a partir de hoje)
dbutils.widgets.text("meses_retroativos", "3")
meses_retroativos = int(dbutils.widgets.get("meses_retroativos"))

# Definição das tabelas
tabela_controle = f"{used_catalog}.{used_schema}.controle_ingestao"
tabela_deputados = f"{used_catalog}.{used_schema}.raw_deputados"
tabela_destino = f"{used_catalog}.{used_schema}.raw_despesas"

print(f"Catálogo: {used_catalog}")
print(f"Schema: {used_schema}")
print(f"Meses retroativos: {meses_retroativos}")
print(f"Tabela controle: {tabela_controle}")
print(f"Tabela deputados: {tabela_deputados}")
print(f"Tabela destino: {tabela_destino}")

# COMMAND ----------

# Busca a última data de despesa processada
ctrl_data = spark.sql(
    f"SELECT raw_despesas FROM {tabela_controle} WHERE id = 1"
).collect()

if ctrl_data and ctrl_data[0][0] is not None:
    ultima_data_str = ctrl_data[0][0]
    ultima_data = datetime.strptime(ultima_data_str, "%Y-%m-%d")
else:
    # Se não há controle, pega N meses atrás
    ultima_data = datetime.now() - relativedelta(months=meses_retroativos)

# Buscar lista de deputados
deputados = spark.sql(f"SELECT id FROM {tabela_deputados}").collect()
lista_deputados = [row.id for row in deputados]

print(f"Última data processada: {ultima_data.strftime('%Y-%m-%d')}")
print(f"Total de deputados: {len(lista_deputados)}")

# Calcular período de ingestão (da última data até hoje)
data_inicial = ultima_data
data_final = datetime.now()

# Gerar lista de meses a processar
meses_processar = []
current = data_inicial
while current <= data_final:
    meses_processar.append((current.year, current.month))
    current += relativedelta(months=1)

print(f"Meses a processar: {len(meses_processar)}")
for ano, mes in meses_processar:
    print(f"  - {ano}-{mes:02d}")

# COMMAND ----------

# Ingestão de despesas por deputado e por mês
all_rows = []
total_deputados = len(lista_deputados)
total_meses = len(meses_processar)

print(f"\nIniciando ingestão de despesas...")
print(f"Total de requisições: {total_deputados * total_meses}")

contador = 0
erros = 0

for deputado_id in lista_deputados:
    for ano, mes in meses_processar:
        contador += 1
        
        # Log de progresso a cada 50 requisições
        if contador % 50 == 0:
            print(f"Progresso: {contador}/{total_deputados * total_meses} requisições ({len(all_rows)} despesas coletadas, {erros} erros)")
        
        try:
            url = f"https://dadosabertos.camara.leg.br/api/v2/deputados/{deputado_id}/despesas"
            params = {
                "ano": ano,
                "mes": mes,
                "itens": 100,
                "ordem": "ASC"
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                dados = response.json()
                
                for despesa in dados.get("dados", []):
                    # Extrair dados relevantes
                    row = (
                        deputado_id,                                  # id_deputado
                        ano,                                          # ano
                        mes,                                          # mes
                        despesa.get("tipoDespesa"),                   # tipo_despesa
                        despesa.get("codDocumento"),                  # cod_documento
                        despesa.get("tipoDocumento"),                 # tipo_documento
                        despesa.get("codTipoDocumento"),              # cod_tipo_documento
                        despesa.get("dataDocumento"),                 # data_documento
                        despesa.get("numDocumento"),                  # num_documento
                        float(despesa.get("valorDocumento", 0)),      # valor_documento
                        despesa.get("urlDocumento"),                  # url_documento
                        despesa.get("nomeFornecedor"),                # nome_fornecedor
                        despesa.get("cnpjCpfFornecedor"),             # cnpj_cpf_fornecedor
                        float(despesa.get("valorLiquido", 0)),        # valor_liquido
                        float(despesa.get("valorGlosa", 0)),          # valor_glosa
                        despesa.get("numRessarcimento"),              # num_ressarcimento
                        despesa.get("codLote"),                       # cod_lote
                        despesa.get("parcela")                        # parcela
                    )
                    all_rows.append(row)
            
            elif response.status_code == 404:
                # Deputado sem despesas neste período (normal)
                pass
            else:
                erros += 1
                if erros <= 10:  # Log apenas dos primeiros 10 erros
                    print(f"Erro {response.status_code} para deputado {deputado_id}, {ano}-{mes:02d}")
            
            # Rate limiting: pausa pequena para não sobrecarregar a API
            time.sleep(0.1)
            
        except Exception as e:
            erros += 1
            if erros <= 10:
                print(f"Exceção para deputado {deputado_id}, {ano}-{mes:02d}: {str(e)}")
            continue

print(f"\n✓ Ingestão concluída!")
print(f"Total de despesas coletadas: {len(all_rows)}")
print(f"Total de erros: {erros}")

# COMMAND ----------

# Gravação dos dados e atualização do controle
if all_rows:
    # Criar DataFrame com schema definido
    df_despesas = spark.createDataFrame(
        all_rows,
        [
            "id_deputado",
            "ano",
            "mes",
            "tipo_despesa",
            "cod_documento",
            "tipo_documento",
            "cod_tipo_documento",
            "data_documento",
            "num_documento",
            "valor_documento",
            "url_documento",
            "nome_fornecedor",
            "cnpj_cpf_fornecedor",
            "valor_liquido",
            "valor_glosa",
            "num_ressarcimento",
            "cod_lote",
            "parcela"
        ]
    )
    
    # Exibir preview dos dados
    print("\nPreview das despesas coletadas:")
    display(df_despesas.limit(100))
    
    # Salvar na tabela Bronze
    df_despesas.write.mode("append").saveAsTable(tabela_destino)
    
    # Atualizar tabela de controle com a data de hoje
    nova_data = datetime.now().strftime("%Y-%m-%d")
    
    spark.sql(f"""
        UPDATE {tabela_controle}
        SET raw_despesas = '{nova_data}'
        WHERE id = 1
    """)
    
    print(f"\n✓ Dados gravados com sucesso!")
    print(f"✓ Última data atualizada: {nova_data}")
    print(f"✓ Total de despesas inseridas: {len(all_rows)}")
    
    # Estatísticas
    print("\n📊 Estatísticas:")
    df_despesas.groupBy("tipo_despesa").count().orderBy("count", ascending=False).show(20, truncate=False)
    
else:
    print("Nenhuma despesa nova para inserir.")
