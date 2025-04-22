# Guia de CI/CD para Pipeline de Dados SaudeBliss

Este guia documenta a implementação de CI/CD (Integração Contínua/Entrega Contínua) para o pipeline de dados de inadimplência da SaudeBliss.

## Visão Geral

A arquitetura de CI/CD implementada utiliza Cloud Build do GCP como ferramenta principal, com integrações de GitHub, Artifact Registry, Cloud Composer (Airflow), Airbyte e outros serviços do GCP para permitir um fluxo completo de desenvolvimento até produção.

### Fluxo de Trabalho GitFlow

O projeto adota um modelo simplificado do GitFlow:

```
┌──────────────┐   ┌─────────────┐   ┌─────────────┐
│  Feature     │──▶│  Develop    │──▶│    Main     │
│  Branches    │   │  (Staging)  │   │  (Produção) │
└──────────────┘   └─────────────┘   └─────────────┘
       │                 │                  │
       ▼                 ▼                  ▼
┌──────────────┐   ┌─────────────┐   ┌─────────────┐
│  Validação   │   │  Build +    │   │ Release Tag │
│  de PR       │   │  Deploy Dev │   │ Deploy Prod │
└──────────────┘   └─────────────┘   └─────────────┘
```

- **Feature branches**: Desenvolvimento de novos recursos
- **Develop**: Branch de integração (ambiente de desenvolvimento)
- **Main**: Branch de produção (só recebe código estável)
- **Tags**: Marcam versões específicas para lançamentos em produção

## Pipelines Implementados

### 1. Pipeline de Validação de PR (`pr-validation.yaml`)

Executado automaticamente quando um Pull Request é aberto contra as branches `develop` ou `main`.

**Etapas:**
- Verificação de linters (flake8, black, isort)
- Execução de testes unitários
- Análise de segurança (bandit)
- Validação de DAGs do Airflow
- Validação do Terraform (sem apply)

**Comando para execução local:**
```bash
gcloud builds submit --config=.cloudbuild/pr-validation.yaml --substitutions=_ARTIFACT_BUCKET="seu-bucket-de-artefatos" .
```

### 2. Pipeline Principal (`cloudbuild.yaml`)

Executado em commits nas branches `develop` ou `main`, com comportamento diferente para cada ambiente.

**Etapas:**
1. Validação de código e testes
2. Build de pacotes Python
3. Publicação no Artifact Registry
4. Build e push de imagens Docker
5. Aplicação do Terraform baseado na branch
   - `develop`: Deploy em dev
   - `main`: Deploy em prod se for uma tag
6. Atualização de DAGs no Cloud Composer
7. Configuração do Airbyte
8. Configuração do Great Expectations
9. Notificação de conclusão

**Comandos para execução local:**
```bash
# Para develop
gcloud builds submit --config=.cloudbuild/cloudbuild.yaml --substitutions=_REGION="us-central1",_ARTIFACT_REPO="data-pipeline-artifacts" --branch=develop .

# Para main
gcloud builds submit --config=.cloudbuild/cloudbuild.yaml --substitutions=_REGION="us-central1",_ARTIFACT_REPO="data-pipeline-artifacts" --branch=main .
```

### 3. Pipeline de Release (`release.yaml`)

Executado quando uma tag com o formato `v0.0.0` é criada no repositório.

**Etapas:**
1. Validação da tag
2. Build de pacotes Python com versão definida
3. Testes completos
4. Build das imagens Docker (tagged release + latest)
5. Deploy para ambiente de produção
6. Criação de release no GitHub
7. Notificação de release completa

**Comando para execução local:**
```bash
# Substitua v1.0.0 pela sua versão
gcloud builds submit --config=.cloudbuild/release.yaml --substitutions=_REGION="us-central1",_ARTIFACT_REPO="data-pipeline-artifacts",TAG_NAME="v1.0.0" .
```

## Infraestrutura de CI/CD

A infraestrutura de CI/CD é gerenciada pelo Terraform em `infra/terraform/cicd.tf`, incluindo:

- **Cloud Build Triggers**: Configurados para PRs, branches e tags
- **Service Accounts**: Com permissões necessárias para os pipelines
- **Artifact Registry**: Para armazenamento de imagens Docker e pacotes Python
- **Secret Manager**: Para armazenamento de credenciais
- **Pub/Sub**: Para notificações de status dos pipelines
- **Monitoring**: Para alertar sobre falhas nos pipelines

## Imagens Docker

O pipeline usa imagens Docker para os seguintes componentes:

1. **Processamento Spark**: Imagem baseada em Apache Spark com pré-requisitos instalados
   - Localização: `/spark/Dockerfile`
   - Tags: Versão (v1.0.0) e latest

## Scripts de Integração

### Airbyte

Script para configurar fontes, destinos e conexões no Airbyte via API:
- Localização: `/airbyte/scripts/apply-configs.sh`
- Uso: `./apply-configs.sh <AIRBYTE_HOST> <AIRBYTE_API_KEY>`

## Guia de Uso para Desenvolvedores

### Fluxo de Trabalho Diário

1. **Início de novo recurso**:
   ```bash
   git checkout develop
   git pull
   git checkout -b feature/meu-novo-recurso
   ```

2. **Desenvolvimento e testes locais**:
   ```bash
   # Executar testes unitários
   cd spark
   pytest tests/
   
   # Verificar qualidade de código
   flake8 .
   black --check .
   ```

3. **Commit e Push**:
   ```bash
   git add .
   git commit -m "Descrição da alteração"
   git push origin feature/meu-novo-recurso
   ```

4. **Criar Pull Request**:
   - Abra PR contra a branch `develop`
   - Aguarde validação automática do CI
   - Solicite revisão de código
   - Faça merge após aprovação

5. **Promoção para Produção**:
   ```bash
   # Após testar em desenvolvimento
   git checkout main
   git merge develop
   git tag -a v1.0.0 -m "Versão 1.0.0"
   git push origin main --tags
   ```

### Solução de Problemas

#### PR Falhou na Validação

1. Verifique os logs no Cloud Build
2. Corrija os problemas identificados
3. Faça commit das correções
4. O PR será automaticamente reavaliado

#### Falha no Deploy para Ambiente de Desenvolvimento

1. Verifique logs do Cloud Build para a branch `develop`
2. Corrija problemas na infraestrutura ou código
3. Faça commit das correções na branch `develop`

## Métricas e Monitoramento

Todas as execuções do CI/CD são monitoradas através de:

- Dashboard do Cloud Build
- Dashboard personalizado do Cloud Monitoring
- Alertas configurados para falhas de builds
- Logs retidos por 30 dias para auditoria

## Segurança

A implementação segue as melhores práticas de segurança:

1. **Gestão de Segredos**:
   - Todas as credenciais armazenadas no Secret Manager
   - Nenhuma credencial hardcoded nos scripts ou Dockerfiles

2. **Princípio do privilégio mínimo**:
   - Service accounts com permissões limitadas ao necessário
   - Autenticação e autorização em todos os serviços

3. **Controle de Acesso ao Código**:
   - Proteções de branch no GitHub
   - Aprovação obrigatória em PRs para branches principais

## Implantação Gradual

A implementação de CI/CD seguiu estas fases:

1. **Fase 1** (Concluída):
   - Configuração do GitHub
   - Pipeline de validação básica
   - Proteção de branches

2. **Fase 2** (Concluída):
   - Configuração do Artifact Registry
   - Automação de build e testes
   - Publicação de artefatos

3. **Fase 3** (Concluída):
   - Automatização de deploy para ambiente de desenvolvimento
   - Integração com Terraform
   - Configuração de Airbyte/Airflow

4. **Fase 4** (Concluída):
   - Pipeline completo de release
   - Ambientes separados (Dev/Prod)
   - Monitoramento integrado

## Referências

- [Documentação do Cloud Build](https://cloud.google.com/build/docs)
- [Documentação do Artifact Registry](https://cloud.google.com/artifact-registry/docs)
- [Guia de GitFlow](https://nvie.com/posts/a-successful-git-branching-model/)
- [Documentação do Terraform para GCP](https://registry.terraform.io/providers/hashicorp/google/latest/docs)