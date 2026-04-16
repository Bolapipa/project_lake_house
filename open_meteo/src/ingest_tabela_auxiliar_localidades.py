# Databricks notebook source
dbutils.widgets.text("catalog", "bronze_dev")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_open_meteo")
used_schema = dbutils.widgets.get("schema")

tabela_destino = f"{used_catalog}.{used_schema}.auxiliar_localidades"

# Localidades de referência
localidades = [
    {"uf": "AC", "capital": "Rio Branco", "latitude": -9.97499, "longitude": -67.8243},
    {"uf": "AL", "capital": "Maceio", "latitude": -9.66599, "longitude": -35.7350},
    {"uf": "AP", "capital": "Macapa", "latitude": 0.03493, "longitude": -51.0694},
    {"uf": "AM", "capital": "Manaus", "latitude": -3.11903, "longitude": -60.0217},
    {"uf": "BA", "capital": "Salvador", "latitude": -12.9714, "longitude": -38.5014},
    {"uf": "CE", "capital": "Fortaleza", "latitude": -3.73186, "longitude": -38.5267},
    {"uf": "DF", "capital": "Brasilia", "latitude": -15.7939, "longitude": -47.8828},
    {"uf": "ES", "capital": "Vitoria", "latitude": -20.3155, "longitude": -40.3128},
    {"uf": "GO", "capital": "Goiania", "latitude": -16.6869, "longitude": -49.2648},
    {"uf": "MA", "capital": "Sao Luis", "latitude": -2.53073, "longitude": -44.3068},
    {"uf": "MT", "capital": "Cuiaba", "latitude": -15.6010, "longitude": -56.0974},
    {"uf": "MS", "capital": "Campo Grande", "latitude": -20.4697, "longitude": -54.6201},
    {"uf": "MG", "capital": "Belo Horizonte", "latitude": -19.9167, "longitude": -43.9345},
    {"uf": "PA", "capital": "Belem", "latitude": -1.45583, "longitude": -48.5044},
    {"uf": "PB", "capital": "Joao Pessoa", "latitude": -7.1195, "longitude": -34.8450},
    {"uf": "PR", "capital": "Curitiba", "latitude": -25.4284, "longitude": -49.2733},
    {"uf": "PE", "capital": "Recife", "latitude": -8.04756, "longitude": -34.8770},
    {"uf": "PI", "capital": "Teresina", "latitude": -5.0892, "longitude": -42.8016},
    {"uf": "RJ", "capital": "Rio de Janeiro", "latitude": -22.9068, "longitude": -43.1729},
    {"uf": "RN", "capital": "Natal", "latitude": -5.79448, "longitude": -35.2110},
    {"uf": "RS", "capital": "Porto Alegre", "latitude": -30.0346, "longitude": -51.2177},
    {"uf": "RO", "capital": "Porto Velho", "latitude": -8.76077, "longitude": -63.8999},
    {"uf": "RR", "capital": "Boa Vista", "latitude": 2.82384, "longitude": -60.6753},
    {"uf": "SC", "capital": "Florianopolis", "latitude": -27.5949, "longitude": -48.5482},
    {"uf": "SP", "capital": "Sao Paulo", "latitude": -23.5505, "longitude": -46.6333},
    {"uf": "SE", "capital": "Aracaju", "latitude": -10.9472, "longitude": -37.0731},
    {"uf": "TO", "capital": "Palmas", "latitude": -10.1840, "longitude": -48.3336}
]

# Cria o DataFrame
df_localidades = spark.createDataFrame(localidades)

# Garante o schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {used_catalog}.{used_schema}")

# Salva a tabela auxiliar
df_localidades.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(tabela_destino)

print(f"Tabela auxiliar criada com sucesso: {tabela_destino}")
print(f"Quantidade de localidades carregadas: {df_localidades.count()}")

display(df_localidades)
