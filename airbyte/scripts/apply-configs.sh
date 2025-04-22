#!/bin/bash
# Script para aplicar configurações do Airbyte via API

set -e

# Parâmetros
AIRBYTE_HOST=${1:-"http://localhost:8000"}
AIRBYTE_API_KEY=${2:-"default_key"}

echo "Aplicando configurações do Airbyte em: $AIRBYTE_HOST"

# Funções auxiliares
function call_airbyte_api() {
    local endpoint=$1
    local payload=$2
    
    curl -s -X POST "$AIRBYTE_HOST/api/v1/$endpoint" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $AIRBYTE_API_KEY" \
        -d "$payload"
}

function get_workspace_id() {
    local response=$(call_airbyte_api "workspaces/list" "{}")
    echo $(echo $response | jq -r '.workspaces[0].workspaceId')
}

function create_or_update_source() {
    local name=$1
    local config_file=$2
    local workspace_id=$3
    
    echo "Criando/atualizando fonte: $name"
    
    # Carregar configuração
    local config=$(cat "$config_file")
    
    # Verificar se a fonte já existe
    local list_payload="{\"workspaceId\": \"$workspace_id\"}"
    local sources_response=$(call_airbyte_api "sources/list" "$list_payload")
    local source_id=$(echo $sources_response | jq -r ".sources[] | select(.name == \"$name\") | .sourceId")
    
    # Extrair valores do arquivo de configuração
    local source_def_id=$(echo $config | jq -r '.source.sourceDefinitionId')
    local connection_config=$(echo $config | jq -r '.source.connectionConfiguration')
    
    if [ -z "$source_id" ] || [ "$source_id" == "null" ]; then
        echo "Criando nova fonte: $name"
        
        # Preparar payload para criar fonte
        local create_payload=$(cat <<EOF
{
  "sourceDefinitionId": "$source_def_id",
  "connectionConfiguration": $connection_config,
  "name": "$name",
  "workspaceId": "$workspace_id"
}
EOF
)
        # Chamar API para criar fonte
        local response=$(call_airbyte_api "sources/create" "$create_payload")
        source_id=$(echo $response | jq -r '.sourceId')
        echo "Fonte criada com ID: $source_id"
    else
        echo "Atualizando fonte existente: $name (ID: $source_id)"
        
        # Preparar payload para atualizar fonte
        local update_payload=$(cat <<EOF
{
  "sourceId": "$source_id",
  "connectionConfiguration": $connection_config
}
EOF
)
        # Chamar API para atualizar fonte
        local response=$(call_airbyte_api "sources/update_connection_configuration" "$update_payload")
        echo "Fonte atualizada"
    fi
    
    echo $source_id
}

function create_or_update_destination() {
    local name=$1
    local destination_def_id=$2
    local config_json=$3
    local workspace_id=$4
    
    echo "Criando/atualizando destino: $name"
    
    # Verificar se o destino já existe
    local list_payload="{\"workspaceId\": \"$workspace_id\"}"
    local destinations_response=$(call_airbyte_api "destinations/list" "$list_payload")
    local destination_id=$(echo $destinations_response | jq -r ".destinations[] | select(.name == \"$name\") | .destinationId")
    
    if [ -z "$destination_id" ] || [ "$destination_id" == "null" ]; then
        echo "Criando novo destino: $name"
        
        # Preparar payload para criar destino
        local create_payload=$(cat <<EOF
{
  "destinationDefinitionId": "$destination_def_id",
  "connectionConfiguration": $config_json,
  "name": "$name",
  "workspaceId": "$workspace_id"
}
EOF
)
        # Chamar API para criar destino
        local response=$(call_airbyte_api "destinations/create" "$create_payload")
        destination_id=$(echo $response | jq -r '.destinationId')
        echo "Destino criado com ID: $destination_id"
    else
        echo "Atualizando destino existente: $name (ID: $destination_id)"
        
        # Preparar payload para atualizar destino
        local update_payload=$(cat <<EOF
{
  "destinationId": "$destination_id",
  "connectionConfiguration": $config_json
}
EOF
)
        # Chamar API para atualizar destino
        local response=$(call_airbyte_api "destinations/update_connection_configuration" "$update_payload")
        echo "Destino atualizado"
    fi
    
    echo $destination_id
}

function create_or_update_connection() {
    local name=$1
    local source_id=$2
    local destination_id=$3
    local workspace_id=$4
    local namespace=$5
    
    echo "Criando/atualizando conexão: $name"
    
    # Verificar se a conexão já existe
    local list_payload="{\"workspaceId\": \"$workspace_id\"}"
    local connections_response=$(call_airbyte_api "connections/list" "$list_payload")
    local connection_id=$(echo $connections_response | jq -r ".connections[] | select(.name == \"$name\") | .connectionId")
    
    # Configuração básica de sincronização
    local sync_config=$(cat <<EOF
{
  "syncMode": "full_refresh_overwrite",
  "destinationSyncMode": "overwrite"
}
EOF
)
    
    if [ -z "$connection_id" ] || [ "$connection_id" == "null" ]; then
        echo "Criando nova conexão: $name"
        
        # Preparar payload para criar conexão
        local create_payload=$(cat <<EOF
{
  "sourceId": "$source_id",
  "destinationId": "$destination_id",
  "name": "$name",
  "namespaceDefinition": "destination",
  "namespaceFormat": "${namespace}",
  "prefix": "",
  "status": "active",
  "scheduleType": "manual",
  "syncCatalog": {
    "streams": []
  },
  "operationIds": []
}
EOF
)
        # Chamar API para criar conexão
        local response=$(call_airbyte_api "connections/create" "$create_payload")
        connection_id=$(echo $response | jq -r '.connectionId')
        echo "Conexão criada com ID: $connection_id"
    else
        echo "Atualizando conexão existente: $name (ID: $connection_id)"
        
        # Preparar payload para atualizar conexão
        local update_payload=$(cat <<EOF
{
  "connectionId": "$connection_id",
  "status": "active",
  "scheduleType": "manual",
  "namespaceDefinition": "destination",
  "namespaceFormat": "${namespace}",
  "prefix": ""
}
EOF
)
        # Chamar API para atualizar conexão
        local response=$(call_airbyte_api "connections/update" "$update_payload")
        echo "Conexão atualizada"
    fi
    
    echo $connection_id
}

# Obter workspace ID
WORKSPACE_ID=$(get_workspace_id)
echo "ID do Workspace: $WORKSPACE_ID"

# Definições
GCS_DESTINATION_DEF_ID="10942577-ce01-47b6-90f7-f72926f79d31" # GCS
FILE_SOURCE_DEF_ID="778daa7c-feaf-4db6-96f3-70fd645acc77"      # File Source

# Criar fontes
TRANSACTIONS_SOURCE_ID=$(create_or_update_source "json_transactions" "airbyte/connections/transactions_source.yaml" "$WORKSPACE_ID")
PAYMENTS_SOURCE_ID=$(create_or_update_source "json_payments" "airbyte/connections/payments_source.yaml" "$WORKSPACE_ID")

# Criar destino GCS Bronze
GCS_CONFIG=$(cat <<EOF
{
  "gcs_bucket_name": "${PROJECT_ID}-bronze-${ENV}",
  "gcs_bucket_path": "/",
  "gcs_bucket_region": "us-central1",
  "format": {
    "format_type": "JSONL"
  }
}
EOF
)

BRONZE_DESTINATION_ID=$(create_or_update_destination "gcs_bronze" "$GCS_DESTINATION_DEF_ID" "$GCS_CONFIG" "$WORKSPACE_ID")

# Criar conexões
TRANSACTIONS_CONNECTION_ID=$(create_or_update_connection "transactions_to_bronze" "$TRANSACTIONS_SOURCE_ID" "$BRONZE_DESTINATION_ID" "$WORKSPACE_ID" "transactions")
PAYMENTS_CONNECTION_ID=$(create_or_update_connection "payments_to_bronze" "$PAYMENTS_SOURCE_ID" "$BRONZE_DESTINATION_ID" "$WORKSPACE_ID" "payments")

echo "Configuração do Airbyte concluída com sucesso!"
echo "ID da conexão de transações: $TRANSACTIONS_CONNECTION_ID"
echo "ID da conexão de pagamentos: $PAYMENTS_CONNECTION_ID"

# Salvar IDs em arquivo para uso posterior
echo "AIRBYTE_TRANSACTIONS_CONNECTION_ID=$TRANSACTIONS_CONNECTION_ID" > airbyte_connection_ids.env
echo "AIRBYTE_PAYMENTS_CONNECTION_ID=$PAYMENTS_CONNECTION_ID" >> airbyte_connection_ids.env

echo "IDs salvos em airbyte_connection_ids.env"