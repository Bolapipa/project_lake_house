# Databricks - Snapshot Visual (Open Meteo)

Este material foi criado para documentar o estado operacional do Open Meteo no Databricks de forma visual e didatica.

## Arquivo principal
- `open_meteo/docs/databricks_visual_report.html`

## Imagem para README / LinkedIn
- `open_meteo/docs/images/linkedin/databricks_visual_report.png`

## O que esta representado
- Quantidade total de jobs no workspace e recorte de jobs Open Meteo.
- Taxa de sucesso recente de runs para PROD e DEV.
- Arquitetura de orquestracao:
  - Airflow -> Databricks Jobs -> DLT Pipeline
  - Bronze -> Silver -> Analytics/Consumo
- Tabela com IDs de jobs Open Meteo e concorrencia.
- Tabela com pipelines Open Meteo e ultimo update.
- Historico recente de runs de PROD e DEV.

## Observacao importante
A captura direta da interface web do Databricks depende de sessao autenticada no navegador.
Para nao bloquear a entrega, o snapshot foi gerado com dados reais da API Databricks via CLI autenticado.
