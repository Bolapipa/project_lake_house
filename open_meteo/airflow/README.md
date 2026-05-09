# Airflow - Orquestracao Open Meteo

Esta pasta contem a orquestracao externa do bundle `open_meteo` via Apache Airflow.

## Conteudo
- `dags/open_meteo_databricks_prod.py`: DAG de producao (ativa)
- `dags/open_meteo_databricks_dev.py`: DAG de desenvolvimento (manter pausada)
- `dags/open_meteo_primeira_dag.py`: DAG de coleta direta Open-Meteo (bronze local)
- `docker-compose.yaml`: stack local do Airflow 3.2 (CeleryExecutor)
- `Dockerfile.airflow`: imagem custom com provider Databricks instalado
- `.env.example`: variaveis de ambiente de referencia

## Decisoes aplicadas
- Provider Databricks fixado na imagem para evitar instalacao dinamica em runtime.
- `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` configurado para o Airflow 3.
- `AIRFLOW__API_AUTH__JWT_SECRET` compartilhado entre os servicos.
- DAG `open_meteo_databricks_prod` validada com sucesso.
- DAG `open_meteo_databricks_dev` deve permanecer pausada quando nao utilizada.

## Como executar
1. Copie `.env.example` para `.env` e ajuste valores.
2. Suba os servicos:
   `docker compose up -d`
3. Acesse a UI:
   `http://localhost:8080`
4. Confirme o estado das DAGs:
   - `open_meteo_databricks_prod`: unpaused
   - `open_meteo_databricks_dev`: paused

## Observacoes de seguranca
- Nao commitar `.env` com segredos reais.
- Rotacionar periodicamente o token do `databricks_default`.
- Definir `AIRFLOW__CORE__FERNET_KEY` em ambiente produtivo.
