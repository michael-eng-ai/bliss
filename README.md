# Pipeline de Dados para Análise de Inadimplência

Este repositório contém a implementação de um pipeline de dados completo para análise de inadimplência de clientes, utilizando o Google Cloud Platform (GCP) e seguindo uma arquitetura moderna de Lakehouse.

## Visão Geral da Arquitetura

A solução segue o padrão "bronze → silver → gold" para processamento de dados:

1. **Camada Bronze**: Dados brutos ingeridos via Airbyte, armazenados em formato JSON/Iceberg
2. **Camada Silver**: Dados limpos e normalizados via Apache Spark, armazenados em formato Parquet 
3. **Camada Gold**: Dados com regras de negócio aplicadas, armazenados em tabelas Iceberg
4. **Visualização**: Dados expostos via BigQuery BigLake para análise em Looker Studio e Grafana

![Arquitetura](/architecture/diagrams/architecture_diagram.png)

## Tabelas de Controle e Monitoramento

A plataforma implementa um sistema robusto de rastreabilidade e monitoramento através de tabelas de controle:

1. **Tabela de Processos**: Armazena metadados detalhados de cada execução
   - Identificador único do processo
   - Nome da entidade e do processo
   - Horários de início e fim
   - Contagens de registros processados
   - Status de execução (SUCESSO/ERRO)
   - Caminho dos arquivos de origem e destino
   - Tempos de execução de cada fase (carregamento, transformação, escrita)

2. **Tabela de Erros**: Registra erros e anomalias encontrados durante o processamento
   - Identificador único do erro
   - Tipo de erro (validação, transformação, escrita)
   - Mensagem de erro detalhada
   - Entidade e caminho do arquivo afetado
   - Conteúdo do registro com problema
   - Timestamp do erro

Este sistema oferece rastreabilidade completa para diagnóstico e auditoria de cada etapa do pipeline.

## Stack Tecnológico

- **Ingestão**: Airbyte, Google Cloud Functions
- **Armazenamento**: Google Cloud Storage (GCS), Apache Iceberg
- **Processamento**: Dataproc Serverless, Apache Spark
- **Orquestração**: Cloud Composer (Airflow)
- **Governança**: Data Catalog, OpenLineage, Great Expectations
- **Monitoramento**: Prometheus, Grafana, BigQuery, Looker Studio
- **CI/CD**: Google Cloud Build, Artifact Registry, Secret Manager, Cloud Deploy

## Estrutura do Repositório

```
saudebliss/
├── README.md                     # Este arquivo
├── code_challenge.md             # Descrição do desafio
├── payments.json                 # Dados de pagamentos
├── transactions.json             # Dados de transações
├── airbyte/                      # Configurações do Airbyte
│   ├── connections/              # Configurações de fontes e conexões
│   └── scripts/                  # Scripts para automação do Airbyte
├── airflow/                      # DAGs e plugins do Airflow
├── architecture/                 # Documentação da arquitetura
├── docs/                         # Documentação adicional
│   └── cicd_guide.md             # Guia completo de CI/CD
├── governance/                   # Configurações de governança
├── infra/                        # Infraestrutura como código
│   ├── cloud_functions/          # Cloud Functions para detecção de arquivos
│   ├── terraform/                # Configurações Terraform
│   │   ├── cicd.tf               # Infraestrutura para CI/CD
│   │   ├── main.tf               # Recursos principais
│   │   └── environments/         # Variáveis por ambiente
├── monitoring/                   # Configurações de monitoramento
│   ├── grafana/                  # Dashboards Grafana
│   ├── great_expectations/       # Validação de qualidade de dados
│   └── prometheus/               # Configuração do Prometheus
└── spark/                        # Scripts PySpark para transformações
    ├── Dockerfile                # Containerização para jobs Spark
    ├── scripts/                  # Scripts de suporte
    ├── tests/                    # Testes unitários
    └── utils/                    # Módulos utilitários
```

## Como Iniciar

### Pré-requisitos

- Conta GCP com privilégios para criar recursos
- gcloud CLI configurado
- Terraform instalado (v1.0+)
- Python 3.8+
- Docker e Docker Compose (para ambiente local)
- Git configurado (para CI/CD)

### Implantação da Infraestrutura

```bash
# Clone do repositório
git clone https://github.com/saudebliss/data-pipeline-inadimplencia.git
cd data-pipeline-inadimplencia

# Configuração inicial
cd infra/terraform
terraform init
terraform apply -var-file=environments/dev.tfvars
```

### Configuração do CI/CD

O projeto utiliza o Google Cloud Build para CI/CD. Siga os passos abaixo para iniciar:

```bash
# Criar os secrets necessários
gcloud secrets create github-token --replication-policy="automatic"
echo -n "seu-token-aqui" | gcloud secrets versions add github-token --data-file=-

gcloud secrets create airbyte-api-key --replication-policy="automatic"
echo -n "sua-api-key-aqui" | gcloud secrets versions add airbyte-api-key --data-file=-

# Inicializar os triggers do Cloud Build
cd infra/terraform
terraform apply -var-file=environments/dev.tfvars -target=google_cloudbuild_trigger.develop_branch_trigger
```

Para mais detalhes sobre o CI/CD, consulte o [Guia de CI/CD](/docs/cicd_guide.md).

### Configuração do Airbyte

Consulte as instruções em `airbyte/setup.md` para configurar as conexões.

### Implantação dos DAGs do Airflow

Os DAGs serão automaticamente carregados pelo Cloud Composer após a implantação da infraestrutura e execução do pipeline de CI/CD.

## Fluxo de Trabalho de Desenvolvimento

O projeto segue um modelo GitFlow simplificado:

1. Desenvolva em branches de feature (`feature/nome-do-recurso`)
2. Abra PRs para a branch `develop` (ambiente de dev)
3. Após testes, promova o código para `main` com tags de versão (`v1.0.0`)

O pipeline de CI/CD automaticamente:
- Valida PRs (linting, testes, segurança)
- Implanta código na branch `develop` para ambiente de desenvolvimento
- Publica releases completos para produção ao criar tags

## Regras de Negócio para Inadimplência

Um cliente é considerado inadimplente se atender a um dos critérios:
- **Critério 1**: Atraso em pagamento por mais de 30 dias, após um período de 3 meses
- **Critério 2**: Qualquer atraso superior a 15 dias em um período de 6 meses

## Monitoramento e Alertas

Os dashboards Grafana estão disponíveis em `monitoring/grafana/` e podem ser importados após a implantação.

O monitoramento inclui:
- Métricas operacionais do pipeline (sucesso/falha, duração)
- Métricas de negócio (taxas de inadimplência, tendências)
- Qualidade de dados via Great Expectations
- Alertas para falhas em jobs e anomalias nos dados

## Conformidade com LGPD

Este pipeline implementa:
- Mascaramento de dados PII
- Controle de acesso granular
- Rastreamento de linhagem de dados
- Políticas de retenção

## Contribuições

Este projeto segue as boas práticas de desenvolvimento, incluindo:
- Padrão PEP 8 para código Python
- Design patterns para melhor manutenção
- Tratamento adequado de erros
- Idempotência nos processos
- CI/CD automatizado para validação e implantação

# Decisões de Arquitetura e Escolha de Ferramentas

## Cenário e Desafios

Este projeto foi desenvolvido para uma startup em crescimento que utiliza GCP como sua plataforma de nuvem principal, com uma equipe técnica enxuta (um engenheiro de dados sênior e um júnior). Diante deste cenário, as escolhas tecnológicas foram guiadas por princípios fundamentais:

1. **Escalabilidade**: Infraestrutura que cresce com o negócio sem redesenho completo
2. **Simplicidade operacional**: Minimizar overhead de gerenciamento para uma equipe pequena
3. **Open Source**: Evitar vendor lock-in, permitindo portabilidade entre ambientes
4. **Custo-eficiência**: Maximizar uso de recursos, usando componentes serverless quando possível
5. **Conformidade com LGPD**: Incorporar governança de dados desde a concepção

A arquitetura foi projetada como um moderno data lakehouse seguindo o paradigma "bronze-silver-gold" para proporcionar tanto a flexibilidade de um data lake quanto a estrutura de um data warehouse.

## Escolhas Tecnológicas e Justificativas

### 1. Ingestão de Dados: Airbyte

**Por que escolhemos**: O Airbyte foi selecionado como ferramenta de ingestão por sua combinação única de facilidade de uso e robustez técnica.

**Principais vantagens**:
- **Interface visual intuitiva**: Permite que mesmo um engenheiro júnior configure novos conectores
- **Mais de 300 conectores pré-construídos**: Evita desenvolvimento de conectores customizados
- **Suporte nativo a CDC (Change Data Capture)**: Facilita integração futura com bancos de dados
- **Comunidade ativa e open-source**: Reduz riscos de dependência de fornecedor específico
- **Implantação simples no GCP**: Funciona bem com Kubernetes ou Cloud Run

**Alternativas consideradas**:
- **Apache NiFi/Cloud Data Fusion**: Mais robusto, mas com curva de aprendizado maior e custo mais elevado
- **Desenvolvimento interno**: Descartado pelo custo de manutenção para equipe pequena
- **Fivetran**: Solução proprietária com custo mais alto, embora com excelente suporte

### 2. Formato de Dados: Apache Iceberg

**Por que escolhemos**: O Apache Iceberg oferece capacidades de tabela ACID sobre arquivos em cloud storage, essencial para garantir confiabilidade dos dados.

**Principais vantagens**:
- **Transações ACID**: Garante consistência de dados mesmo com múltiplas gravações concorrentes
- **Evolução de schema**: Permite alterações de schema sem migrações complexas
- **Time travel**: Possibilita consultas em snapshots históricos, útil para auditoria e recuperação
- **Integração com BigQuery**: Com BigLake, permite consultas SQL sem mover dados
- **Portabilidade**: Funciona em qualquer nuvem ou ambiente on-premise

**Alternativas consideradas**:
- **Delta Lake**: Excelente, mas melhor integrado ao ecossistema Databricks/Azure
- **Apache Hudi**: Bom para upserts, mas menos maduro no ecossistema GCP
- **Parquet simples**: Não oferece garantias ACID necessárias para dados financeiros

### 3. Processamento: Dataproc Serverless para Apache Spark

**Por que escolhemos**: Processamento distribuído sob demanda sem necessidade de gerenciar clusters.

**Principais vantagens**:
- **Serverless**: Paga apenas pelo tempo de execução do job, ideal para processamento batch
- **Escalabilidade automática**: Dimensiona recursos conforme necessidade do job
- **100% compatível com Apache Spark**: Sem lock-in, código portável para qualquer ambiente Spark
- **Integrado com IAM do GCP**: Simplifica governança e segurança
- **Baixo overhead operacional**: Sem necessidade de provisionar ou manter clusters

**Alternativas consideradas**:
- **Dataproc tradicional**: Maior controle, mas custos fixos mesmo quando ocioso
- **Dataflow (Apache Beam)**: Excelente para streaming, mas menos flexível para processamento batch
- **BigQuery**: Ótimo para consultas ad-hoc, mas menos flexível para ETL complexo

### 4. Orquestração: Cloud Composer (Apache Airflow)

**Por que escolhemos**: Orquestração robusta com ampla adoção na comunidade de dados.

**Principais vantagens**:
- **Modelo de programação em Python**: Familiar para a equipe de engenharia
- **DAGs declarativos**: Expressam claramente dependências entre tarefas
- **Integração nativa com GCP**: Operadores prontos para serviços Google Cloud
- **Extensível**: Ampla biblioteca de operadores e hooks, incluindo para Airbyte
- **Interface visual para monitoramento**: Facilita depuração e acompanhamento

**Alternativas consideradas**:
- **Cloud Workflows**: Mais simples, mas menos flexível para pipelines complexos
- **Prefect**: Promissor, mas com menos integrações maduras para GCP
- **Desenvolvimento interno**: Complexidade desnecessária para uma equipe pequena

### 5. Governança e Metadados: Data Catalog + OpenLineage

**Por que escolhemos**: Combinação de serviço gerenciado do GCP com framework open-source.

**Principais vantagens**:
- **Catalogação automática**: O Data Catalog indexa datasets no GCS e BigQuery
- **Controle de acesso integrado**: Usa IAM do GCP para gerenciar permissões
- **Rastreamento de linhagem**: OpenLineage captura dependências entre dados
- **Baixa manutenção**: Data Catalog é serverless e não requer infraestrutura
- **Conformidade LGPD**: Facilita classificação de dados sensíveis e políticas de acesso

**Alternativas consideradas**:
- **DataHub**: Mais completo, mas requer mais recursos para implantação e manutenção
- **Amundsen**: Bom para descoberta, mas menos integrado com GCP
- **Apache Atlas**: Poderoso, mas complexo para uma equipe pequena

### 6. Monitoramento: Prometheus + Grafana + Great Expectations

**Por que escolhemos**: Stack de monitoramento completo cobrindo infraestrutura e qualidade de dados.

**Principais vantagens**:
- **Prometheus**: Coleta de métricas robusta e escalável
- **Grafana**: Visualizações flexíveis e alertas configuráveis
- **Great Expectations**: Framework Python para validação de dados
- **Integração com Cloud Monitoring**: Complementa ferramentas open-source com alertas nativos
- **Visualização unificada**: Um único painel para métricas técnicas e de negócio

**Alternativas consideradas**:
- **DataDog**: Excelente, mas com custo adicional significativo
- **Apenas Cloud Monitoring**: Menos flexível para customizações
- **Monte Carlo/Bigeye**: Soluções SaaS específicas para qualidade, mas com custo alto

### 7. CI/CD: Cloud Build + Artifact Registry

**Por que escolhemos**: Solução nativa do GCP para automação de builds e implantações.

**Principais vantagens**:
- **Integração nativa com GCP**: Permissões e recursos simplificados
- **Pay-per-use**: Paga apenas pelo tempo de build, sem custos fixos
- **Suporte multi-linguagem**: Python, Terraform, Docker em um único pipeline
- **Artifact Registry**: Armazenamento centralizado para pacotes e imagens
- **Triggers configuráveis**: Automação baseada em eventos do Git

**Alternativas consideradas**:
- **Jenkins**: Mais controle, mas exige manutenção de infraestrutura
- **GitHub Actions**: Excelente, mas com integrações menos profundas com GCP
- **GitLab CI**: Bom, mas exigiria migração de repositório ou ferramentas adicionais

## Sinergia do Ecossistema

A arquitetura foi planejada para que as ferramentas se complementem de forma coesa:

1. **Airbyte → GCS (Bronze)**: Ingestão simples com metadados preservados
2. **Dataproc + Spark → GCS (Silver/Gold)**: Transformação escalável com Apache Iceberg
3. **BigQuery + BigLake → Visualização**: Consulta eficiente sem mover dados
4. **Composer → Orquestração E2E**: Coordenação de todo o fluxo
5. **Data Catalog + OpenLineage → Governança**: Visibilidade e conformidade

Essa combinação oferece um equilíbrio entre componentes gerenciados (reduzindo overhead operacional) e ferramentas open-source (garantindo portabilidade), ideal para uma startup com equipe enxuta mas com necessidade de escalar.

## Considerações de Custo

A arquitetura prioriza componentes serverless e pay-per-use, particularmente:

- **Dataproc Serverless**: Custo zero quando não está processando
- **GCS + Iceberg**: Armazenamento de baixo custo com capacidades avançadas
- **BigLake**: Consultas sem custo de duplicação ou movimentação de dados
- **Airbyte OSS**: Implantação em containers reduz custo de licenciamento

Esta abordagem proporciona uma base tecnológica robusta que pode escalar com o crescimento da empresa, sem investimentos iniciais desproporcionais.