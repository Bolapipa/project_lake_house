# carros – Asset Bundle FIPE

## Contexto e Objetivo
Este asset bundle foi criado para automatizar, organizar e rastrear a ingestão incremental dos dados FIPE de marcas, modelos e anos de veículos brasileiros. A estrutura e os notebooks foram desenhados para facilitar consultas, análises, transformações e controles de integridade.

## Estrutura dos Dados
### Hierarquia
- **Marcas (`raw_marcas`)**: Fabricantes de veículos, cada um com um identificador único (`id_marca`).
- **Modelos (`raw_modelos`)**: Modelos de cada marca, identificados por `id_modelo` e ligados ao `id_marca`.
- **Anos (`raw_anos`)**: Cada modelo pode ter múltiplos anos de fabricação (`id_ano`). A hierarquia completa é: marca → modelo → ano.

## Notebooks de Ingestão
- **ingest_marcas**: Consulta a API FIPE e salva as marcas.
- **ingest_modelos**: Para cada marca, consulta os modelos e registra na tabela.
- **ingest_anos**: Para cada marca/modelo, consulta os anos disponíveis e grava na tabela de anos.

### Estrutura dos notebooks
Cada notebook pode ser rodado manualmente, por job, ou por pipeline. As células principais executam as etapas de consulta, transformação, controle incremental e atualização das tabelas.

## Controle Incremental e Tabela de Controle
### Por que controlamos por marca?
- O fluxo de dados FIPE é sempre marca → modelo → ano, pois a API exige primeiro uma marca para buscar modelos, depois um modelo para buscar anos.
- O controle incremental ocorre pela coluna `ultimo_id` (id_marca) da tabela controle, garantindo processamento ordenado, sem duplicidade e com rastreabilidade.

### Campos principais da tabela controle
- `processo`: Identifica qual etapa está sendo controlada ('marcas', 'modelos', 'anos').
- `ultimo_id`: Maior id_marca processado; é de onde começa o próximo lote incremental.
- `id_ano`: Maior id_ano processado, útil para rastreamento, não para controle incremental.
- `ultima_execucao`: Timestamp da última atualização.

### Lógica incremental
- **Primeira execução**: Identifica que não há controle registrado e processa todos os registros disponíveis (carga histórica).
- **Execuções seguintes**: Processa sempre do próximo id_marca após `ultimo_id`, podendo ser um ou vários (ajuste via TAMANHO_LOTE).
- **Atualização**: Ao final de cada lote, salva o maior `id_marca` processado em `ultimo_id` e o maior `id_ano` em `id_ano`.

## Fluxo técnico dos códigos
- Consulta a tabela controle para definir estratégia de execução (histórica ou incremental).
- Busca dados e transforma em DataFrames.
- Usa `.dropDuplicates()` para garantir integridade e evitar registros repetidos.
- Grava dados nas tabelas FIPE.
- Atualiza a tabela controle para garantir o ponto de reinício e rastreio.

## Recomendações práticas
- Ajuste TAMANHO_LOTE conforme volume de marcas e limites de API (tipicamente entre 3 e 10 por execução).
- Sempre monitore a tabela controle antes e depois das execuções.
- Nunca controle incremental pelo ano, pois o fluxo depende de marca/modelo.
- Use jobs/pipelines para agendamento automatizado e rastreabilidade.
- Consulte o campo `id_ano` apenas para saber qual foi o último ano processado; não é utilizado para controle incremental.

## Troubleshooting e Rastreabilidade
- Se houver duplicidade de dados, verifique se dropDuplicates está sendo aplicado.
- Caso um job falhe, reexecute a mesma rodada; o controle incremental evita duplicidade.
- Se novas marcas forem adicionadas na fonte, elas serão processadas nas próximas execuções automáticas.
- O campo ultima_execucao permite auditar a frequência e o status da ingestão.

## Expansão e Adaptabilidade
- Novos notebooks ou etapas podem ser integrados facilmente, seguindo a estrutura incremental e de controle.
- Alterações na estrutura das tabelas FIPE ou de controle devem sempre ser documentadas para manter rastreabilidade.
- Adaptações de TAMANHO_LOTE ou parâmetros de processamento são feitas nos notebooks de ingestão.

## Como Deployar, Agendar e Manter
- Utilize o painel de Deployments para ativar jobs e pipelines.
- Use agendamento (schedule) para criar rotinas recorrentes.
- Consulte logs dos notebooks e jobs para diagnosticar execuções e eventuais erros.
- Todos os scripts e estruturas deste asset bundle foram pensados para facilitar manutenção, auditoria e escalabilidade.

## Referências
- [Documentação Databricks Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-bundles)
- [Referência de Asset Bundles YAML](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)
- API FIPE e documentação oficial

---

> Qualquer dúvida técnica, melhoria, auditoria ou adaptação, consulte este README, a tabela de controle, os notebooks e as tabelas FIPE. O asset bundle carros foi desenhado para ser robusto, transparente e fácil de expandir.
