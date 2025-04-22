import base64
import json
import os
import logging
from google.cloud import storage
from google.cloud import airbyte_v1

# Configurar logging
logging.basicConfig(level=logging.INFO)

# Configurações
PROJECT_ID = os.environ.get('PROJECT_ID')
REGION = os.environ.get('REGION')
BRONZE_BUCKET = os.environ.get('BRONZE_BUCKET')
TRANSACTIONS_CONNECTION_ID = os.environ.get('AIRBYTE_TRANSACTIONS_CONNECTION_ID')
PAYMENTS_CONNECTION_ID = os.environ.get('AIRBYTE_PAYMENTS_CONNECTION_ID')

# Clientes
storage_client = storage.Client()
airbyte_client = airbyte_v1.AirbyteClient()

def process_file(cloud_event):
    """
    Processa arquivo quando carregado no bucket de landing.
    
    Args:
        cloud_event (CloudEvent): Evento do Cloud Functions com dados do novo arquivo
        
    Returns:
        str: Mensagem de sucesso
    """
    # Extrair informações do evento
    bucket_name = cloud_event.data["bucket"]
    file_path = cloud_event.data["name"]
    file_name = os.path.basename(file_path)
    
    logging.info(f"Detectado novo arquivo: {bucket_name}/{file_path}")
    
    # Verificar tipo de arquivo
    if "transaction" in file_path.lower():
        file_type = "transactions"
        connection_id = TRANSACTIONS_CONNECTION_ID
    elif "payment" in file_path.lower():
        file_type = "payments"
        connection_id = PAYMENTS_CONNECTION_ID
    else:
        logging.warning(f"Tipo de arquivo não reconhecido: {file_path}")
        return "Arquivo ignorado: tipo não reconhecido"
    
    # Validar arquivo
    if not validate_file(bucket_name, file_path):
        logging.error(f"Validação do arquivo falhou: {file_path}")
        return "Processo interrompido: validação falhou"
    
    # Copiar para bronze
    bronze_path = f"{file_type}/{file_name}"
    copy_to_bronze(bucket_name, file_path, BRONZE_BUCKET, bronze_path)
    
    # Iniciar sinronização Airbyte
    if connection_id:
        trigger_airbyte_sync(connection_id)
    
    return f"Arquivo processado com sucesso: {file_path}"

def validate_file(bucket_name, file_path):
    """Valida estrutura e conteúdo do arquivo."""
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        content = blob.download_as_text()
        
        # Verificar se é um JSON válido
        json_data = json.loads(content)
        
        # Adicionar validações específicas aqui
        return True
    except Exception as e:
        logging.error(f"Erro na validação: {str(e)}")
        return False

def copy_to_bronze(source_bucket, source_path, dest_bucket, dest_path):
    """Copia arquivo para camada bronze."""
    try:
        source = storage_client.bucket(source_bucket).blob(source_path)
        destination = storage_client.bucket(dest_bucket).blob(dest_path)
        
        # Copiar blob
        token = None
        source.reload()
        rewrite_token = None
        
        while True:
            rewrite_token, bytes_rewritten, bytes_to_rewrite = destination.rewrite(
                source, token=rewrite_token
            )
            if not rewrite_token:
                break
        
        logging.info(f"Arquivo copiado para bronze: {dest_bucket}/{dest_path}")
        return True
    except Exception as e:
        logging.error(f"Erro ao copiar para bronze: {str(e)}")
        return False

def trigger_airbyte_sync(connection_id):
    """Inicia sincronização Airbyte."""
    try:
        # Preparar requisição
        request = airbyte_v1.CreateConnectionSyncRequest()
        request.connection_id = connection_id
        
        # Enviar requisição
        operation = airbyte_client.create_connection_sync(request)
        logging.info(f"Sync Airbyte iniciado: {operation.name}")
        return True
    except Exception as e:
        logging.error(f"Erro ao iniciar sync Airbyte: {str(e)}")
        return False