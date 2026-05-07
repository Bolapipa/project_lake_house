# Template de Post para LinkedIn - Open Meteo + Airflow + Databricks

## Opcao de titulo
"Da orquestracao ao deploy: como estruturei um pipeline Open Meteo com Airflow, Databricks e CI/CD no GitHub"

## Texto sugerido (pronto para copiar)
Hoje finalizei uma evolucao importante no projeto **project_lake_house** para o dominio **open_meteo**.

O foco foi tornar a orquestracao mais estavel e previsivel entre Airflow e Databricks, com padronizacao de configuracao por ambiente (DEV/PROD) e trilha completa de deploy no GitHub.

Principais pontos:
- DAG de producao validada com execucoes de sucesso.
- DAG de dev mantida pausada para evitar disparos indevidos.
- Stack Airflow isolada em `open_meteo/airflow`.
- Provider Databricks fixado na imagem para reduzir risco operacional.
- Pipeline CI/CD rastreavel via GitHub Actions.

Resultado: uma base mais robusta para evoluir o pipeline e manter confiabilidade de execucao.

## Ordem sugerida para carrossel de imagens
1. Visao do repositorio
![GitHub Repository](images/linkedin/github_repo.png)

2. Airflow home (saude da plataforma)
![Airflow Home](images/linkedin/airflow_home.png)

3. DAG de producao (overview)
![Airflow DAG Overview](images/linkedin/airflow_dag_prod.png)

4. Historico de runs da DAG de producao
![Airflow DAG Runs](images/linkedin/airflow_dag_runs_prod.png)

5. PR tecnico com mudancas
![GitHub Pull Request](images/linkedin/github_pr_33.png)

6. CI/CD no GitHub Actions
![GitHub Actions](images/linkedin/github_actions_open_meteo.png)

## Hashtags sugeridas
#DataEngineering #ApacheAirflow #Databricks #GitHubActions #DataPipeline #AnalyticsEngineering #Lakehouse #OpenMeteo
