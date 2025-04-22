# Configuração do Airbyte

Este documento fornece instruções para configurar o Airbyte como ferramenta de ingestão para o pipeline de dados de inadimplência.

## Pré-requisitos

- Acesso à instância do Airbyte (self-hosted ou cloud)
- Credenciais de acesso ao GCP (Google Cloud Platform)
- Service Account com permissões adequadas para buckets GCS

## Implantação do Airbyte

### Opção 1: Airbyte Cloud

Para uma implantação rápida sem preocupações de infraestrutura, use o [Airbyte Cloud](https://airbyte.com/).

### Opção 2: Airbyte OSS no GCP

Para uma implantação self-managed no GCP:

1. Crie uma VM no GCP:

```bash
gcloud compute instances create airbyte-server \
  --machine-type=e2-standard-4 \
  --boot-disk-size=50GB \
  --image-family=ubuntu-2004-lts \
  --image-project=ubuntu-os-cloud \
  --tags=airbyte,http-server,https-server \
  --service-account=[SERVICE_ACCOUNT_EMAIL]
```

2. Conecte-se à VM e instale o Docker:

```bash
sudo apt-get update
sudo apt-get install -y docker.io docker-compose
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker $USER
```

3. Clone o repositório Airbyte e inicie os containers:

```bash
git clone https://github.com/airbytehq/airbyte.git
cd airbyte
docker-compose up -d
```

4. Acesse a interface web em http://[VM_IP]:8000

## Fontes de Dados

### 1. Transações (JSON)

As transações são ingeridas a partir de arquivos JSON armazenados em um bucket GCS de landing.

#### Configuração da Fonte:

1. Na interface do Airbyte, vá para **Sources > Add new source**
2. Selecione **File** como tipo de fonte
3. Configure os seguintes parâmetros:
   - **Name**: `json_transactions`
   - **Dataset Name**: `transactions`
   - **Format**: JSONL
   - **Storage Provider**: GCS
   - **GCS Bucket Name**: `${PROJECT_ID}-landing-${ENV}`
   - **GCS Path Prefix**: `/transactions/`
   - **Service Account Key**: Carregue o arquivo JSON da Service Account
4. Clique em **Set up source**

### 2. Pagamentos (JSON)

Os pagamentos são ingeridos a partir de arquivos JSON armazenados em um bucket GCS de landing.

#### Configuração da Fonte:

1. Na interface do Airbyte, vá para **Sources > Add new source**
2. Selecione **File** como tipo de fonte
3. Configure os seguintes parâmetros:
   - **Name**: `json_payments`
   - **Dataset Name**: `payments`
   - **Format**: JSONL
   - **Storage Provider**: GCS
   - **GCS Bucket Name**: `${PROJECT_ID}-landing-${ENV}`
   - **GCS Path Prefix**: `/payments/`
   - **Service Account Key**: Carregue o arquivo JSON da Service Account
4. Clique em **Set up source**

## Destinos

### GCS Bronze

Os dados são destinados à camada Bronze no GCS.

#### Configuração do Destino:

1. Na interface do Airbyte, vá para **Destinations > Add new destination**
2. Selecione **GCS** como tipo de destino
3. Configure os seguintes parâmetros:
   - **Name**: `gcs_bronze`
   - **GCS Bucket Name**: `${PROJECT_ID}-bronze-${ENV}`
   - **GCS Path Prefix**: `/`
   - **Service Account Key**: Carregue o arquivo JSON da Service Account
   - **Format**: JSON Lines (JSONL)
4. Clique em **Set up destination**

## Conexões

### 1. Transações para Bronze

1. Na interface do Airbyte, vá para **Connections > Add a new connection**
2. Selecione a fonte `json_transactions`
3. Selecione o destino `gcs_bronze`
4. Configure os seguintes parâmetros:
   - **Connection name**: `transactions_to_bronze`
   - **Replication frequency**: Manual (será acionado via Cloud Function/Airflow)
   - **Destination namespace**: `transactions`
   - **Destination stream prefix**: ` ` (deixar em branco)
5. Clique em **Set up connection**

### 2. Pagamentos para Bronze

1. Na interface do Airbyte, vá para **Connections > Add a new connection**
2. Selecione a fonte `json_payments`
3. Selecione o destino `gcs_bronze`
4. Configure os seguintes parâmetros:
   - **Connection name**: `payments_to_bronze`
   - **Replication frequency**: Manual (será acionado via Cloud Function/Airflow)
   - **Destination namespace**: `payments`
   - **Destination stream prefix**: ` ` (deixar em branco)
5. Clique em **Set up connection**

## Integração com Airflow

Para integrar com o Airflow (Cloud Composer), os seguintes passos são necessários:

1. Obtenha os IDs de conexão no Airbyte:
   - Na interface do Airbyte, vá para cada conexão e note o ID da conexão 
   - Geralmente tem o formato `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`

2. Armazene esses IDs como variáveis no Airflow:
   - No Cloud Composer, vá para **Admin > Variables**
   - Adicione as seguintes variáveis:
     - `AIRBYTE_TRANSACTIONS_CONNECTION_ID`: ID da conexão de transações
     - `AIRBYTE_PAYMENTS_CONNECTION_ID`: ID da conexão de pagamentos

3. Certifique-se de que o Airflow tenha o provider do Airbyte instalado:
   ```bash
   pip install apache-airflow-providers-airbyte
   ```

4. Configure a conexão do Airbyte no Airflow:
   - No Cloud Composer, vá para **Admin > Connections**
   - Adicione uma nova conexão:
     - **Conn Id**: `airbyte_conn`
     - **Conn Type**: `Airbyte`
     - **Host**: URL da sua instância Airbyte
     - **Port**: 8000 (ou porta configurada)
     - **Login**: usuário (se aplicável)
     - **Password**: senha (se aplicável)

## Monitoramento

Para monitorar as sincronizações do Airbyte:

1. Logs de sincronização estão disponíveis na interface do Airbyte
2. Para monitoramento via Prometheus, configure a integração Airbyte-Prometheus:
   - Adicione a seguinte configuração no arquivo `.env` do Airbyte:
   ```
   MICROMETER_METRICS_ENABLED=true
   MICROMETER_METRICS_EXPORT_PROMETHEUS_ENABLED=true
   ```
   - Reinicie o servidor Airbyte
   - Adicione o endpoint de métricas no Prometheus (geralmente `http://airbyte-server:9001/metrics`)

## Próximos Passos

1. **Implementar CDC**: Para fontes que suportam CDC (Como bancos de dados)
2. **Adicionar validações**: Regras básicas de validação na ingestão
3. **Integrar Data Catalog**: Registrar metadados das fontes no Data Catalog

## Referências

- [Documentação oficial do Airbyte](https://docs.airbyte.com/)
- [Conectores do Airbyte](https://docs.airbyte.com/integrations/)
- [Airbyte API](https://docs.airbyte.com/api-documentation/)