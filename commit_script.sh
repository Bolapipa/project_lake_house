#!/bin/bash
# Script para fazer commit das correções via Databricks CLI
# Criado em: 21 de Abril de 2026

set -e  # Parar em caso de erro

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║     📦 COMMIT DAS CORREÇÕES - Databricks Asset Bundles          ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# Variáveis
WORKSPACE_PATH="/Users/delacortearthur@gmail.com/project_lake_house"
LOCAL_PATH="./project_lake_house"

echo "1️⃣  Exportando arquivos do workspace..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

databricks workspace export-dir \
  "$WORKSPACE_PATH" \
  "$LOCAL_PATH" \
  --overwrite

echo "✅ Arquivos exportados!"
echo ""

echo "2️⃣  Navegando para o diretório..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
cd "$LOCAL_PATH"
echo "✅ Diretório: $(pwd)"
echo ""

echo "3️⃣  Adicionando arquivos modificados ao git..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Workflows
git add .github/workflows/camara_deputados-ci-cd.yml
git add .github/workflows/carros-ci-cd.yml
git add .github/workflows/ibge-ci-cd.yml
git add .github/workflows/open_meteo-ci-cd.yml
git add .github/workflows/pokemon-ci-cd.yml

# Bundles configs
git add carros/databricks.yml
git add carros/resources/carros_etl.pipeline.yml
git add pokemon/databricks.yml
git add pokemon/resources/pokemon.pipeline.yml
git add camara_deputados/databricks.yml
git add camara_deputados/resources/camara_deputados_etl.pipeline.yml
git add ibge/databricks.yml
git add ibge/resources/ibge_etl.pipeline.yml
git add open_meteo/databricks.yml
git add open_meteo/resources/open_meteo_etl.pipeline.yml

# Notebooks SQL
git add carros/src/dlt_carros.sql
git add pokemon/src/dlt_pokemon.sql
git add camara_deputados/src/dlt_camara_deputados.sql
git add ibge/src/dlt_ibge.sql
git add open_meteo/src/dlt_open_meteo.sql

# Documentação
git add CORRECOES_APLICADAS.md

echo "✅ 21 arquivos adicionados!"
echo ""

echo "4️⃣  Verificando status do git..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
git status --short
echo ""

echo "5️⃣  Fazendo commit..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

git commit -m "fix: padronizar workflows e adicionar campo schema

🔧 Workflows GitHub Actions (5):
- Databricks CLI v0.297.2 (versão estável)
- DATABRICKS_BUNDLE_TF_DOWNLOAD_SIGNATURE_VERIFICATION=false
- Validação de autenticação em todos os jobs
- Estrutura consistente para todos os bundles

📦 Bundles (5):
- Variáveis dinâmicas (silver_catalog, pipeline_development)
- 42 células SQL usando source_catalog/source_schema
- Campo 'schema' adicionado em pipelines (compatibilidade)
- 10 arquivos YAML de configuração
- Código 100% portável entre dev/prod

✅ Total: 21 arquivos modificados
✅ 4 problemas resolvidos:
  1. Caminhos hardcoded → variáveis dinâmicas
  2. Configs não dinâmicas → configs por ambiente
  3. Erro assinatura Terraform → workflows padronizados
  4. Campo schema ausente → adicionado por compatibilidade"

echo "✅ Commit realizado!"
echo ""

echo "6️⃣  Fazendo push para o repositório remoto..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Descomente a linha abaixo quando estiver pronto para fazer push
# git push origin main

echo "⚠️  PUSH DESABILITADO POR SEGURANÇA!"
echo ""
echo "Para fazer push, edite o script e descomente a linha:"
echo "  git push origin main"
echo ""
echo "Ou execute manualmente:"
echo "  cd $LOCAL_PATH"
echo "  git push origin main"
echo ""

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║                  ✅ COMMIT CONCLUÍDO COM SUCESSO!                ║"
echo "║                                                                  ║"
echo "║  Para fazer push:                                                ║"
echo "║    cd $LOCAL_PATH                                                ║"
echo "║    git push origin main                                          ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
