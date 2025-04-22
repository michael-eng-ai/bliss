"""
DAG de Inadimplência SaudeBliss - Processamento ETL Completo

Este DAG implementa o fluxo completo de processamento de dados de inadimplência:
1. Detecção de novos arquivos do Airbyte
2. Processamento Landing -> Bronze (conversão Parquet para Iceberg)
3. Processamento Bronze -> Silver (limpeza e tipagem)
4. Processamento Silver -> Gold (aplicação de regras de negócio)
5. Validação e monitoramento de qualidade dos dados
6. Geração de métricas e alertas

Inclui tratamento de falhas, registro de execuções e dependências.

Autor: Time de Engenharia de Dados SaudeBliss
Data: Abril/2025
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.models import Variable
import os
import json
import logging

# Configurações
default_args = {
    'owner': 'SaudeBliss DataEng',
    'depends_on_past': False,
    'email': ['data-alerts@saudebliss.com.br'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Parâmetros do DAG
BUCKET_NAME = "{{ var.value.saudebliss_bucket }}"
PROJECT_ID = "{{ var.value.project_id }}"
REGION = "us-central1"
CLUSTER_NAME = "spark-inadimplencia-{{ ds_nodash }}"
PYSPARK_URI = f"gs://{BUCKET_NAME}/spark"

# Configuração do cluster Dataproc
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_size_gb": 500},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_size_gb": 500},
    },
    "software_config": {
        "image_version": "2.0-debian10",
        "properties": {
            "spark:spark.default.parallelism": "16",
            "spark:spark.dynamicAllocation.enabled": "true",
            "spark:spark.executor.memory": "4g",
            "spark:spark.driver.memory": "4g",
            "spark:spark.sql.adaptive.enabled": "true",
            "spark:spark.sql.iceberg.enabled": "true"
        }
    },
    "gce_cluster_config": {
        "service_account_scopes": [
            "https://www.googleapis.com/auth/cloud-platform"
        ],
    }
}

# Definição do DAG
dag = DAG(
    'etl_inadimplencia',
    default_args=default_args,
    description='Pipeline de processamento de inadimplência SaudeBliss',
    schedule_interval='0 5 * * *',  # Diariamente às 5am
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['saudebliss', 'inadimplencia', 'etl'],
    max_active_runs=1,
)

# ----- Funções auxiliares -----

def check_new_files(**kwargs):
    """Verifica se há novos arquivos do Airbyte para processar."""
    gcs_hook = GCSHook()
    
    # Verifica arquivos nos prefixos de pagamentos e transações
    payments_files = gcs_hook.list(
        bucket_name=BUCKET_NAME,
        prefix="landing/payments",
        delimiter=".parquet"
    )
    
    transactions_files = gcs_hook.list(
        bucket_name=BUCKET_NAME,
        prefix="landing/transactions",
        delimiter=".parquet"
    )
    
    # Verifica se há arquivos para processar
    has_files = len(payments_files) > 0 or len(transactions_files) > 0
    
    if has_files:
        logging.info(f"Arquivos encontrados: {len(payments_files)} pagamentos, {len(transactions_files)} transações")
    else:
        logging.info("Nenhum arquivo novo encontrado para processamento")
    
    # Registrar as informações no XCom para que outras tasks possam acessar
    kwargs['ti'].xcom_push(key='payment_files_count', value=len(payments_files))
    kwargs['ti'].xcom_push(key='transaction_files_count', value=len(transactions_files))
    
    return has_files


def check_process_logs(entity, **kwargs):
    """
    Verifica o registro de processos para determinar o status da execução.
    Essa função busca informações na tabela de processos e determina se o 
    processamento foi bem-sucedido.
    """
    from google.cloud import bigquery
    import pandas as pd
    
    execution_date = kwargs['execution_date'].strftime("%Y-%m-%d")
    
    # Criar cliente BigQuery
    client = bigquery.Client(project=PROJECT_ID)
    
    # Consultar os logs de processo para a entidade e data
    query = f"""
    SELECT 
        status, error_message, 
        total_records_processed, total_records_error,
        TIMESTAMP_DIFF(TIMESTAMP(end_time), TIMESTAMP(start_time), SECOND) as duration_seconds
    FROM `{PROJECT_ID}.control.process_logs`
    WHERE process_name LIKE '%{entity}%'
    AND DATE(start_time) = '{execution_date}'
    ORDER BY start_time DESC
    LIMIT 1
    """
    
    try:
        results = client.query(query).to_dataframe()
        
        if results.empty:
            logging.warning(f"Nenhum log de processo encontrado para {entity} em {execution_date}")
            kwargs['ti'].xcom_push(key=f'{entity}_status', value="NOT_FOUND")
            return True  # Continuar o pipeline mesmo se não encontrar logs
        
        status = results['status'].iloc[0]
        records_processed = int(results['total_records_processed'].iloc[0])
        records_error = int(results['total_records_error'].iloc[0])
        duration = int(results['duration_seconds'].iloc[0])
        
        # Registrar informações no XCom para uso em outras tasks
        kwargs['ti'].xcom_push(key=f'{entity}_status', value=status)
        kwargs['ti'].xcom_push(key=f'{entity}_records_processed', value=records_processed)
        kwargs['ti'].xcom_push(key=f'{entity}_records_error', value=records_error)
        kwargs['ti'].xcom_push(key=f'{entity}_duration', value=duration)
        
        if records_error > 0:
            # Buscar alguns exemplos de erros para análise
            error_query = f"""
            SELECT entity_name, error_type, error_message, error_time
            FROM `{PROJECT_ID}.control.error_records`
            WHERE process_id = (
                SELECT process_id 
                FROM `{PROJECT_ID}.control.process_logs`
                WHERE process_name LIKE '%{entity}%'
                AND DATE(start_time) = '{execution_date}'
                ORDER BY start_time DESC
                LIMIT 1
            )
            LIMIT 10
            """
            
            error_examples = client.query(error_query).to_dataframe()
            
            if not error_examples.empty:
                errors_summary = error_examples.to_dict('records')
                kwargs['ti'].xcom_push(key=f'{entity}_error_samples', value=errors_summary)
                
                # Log alguns exemplos de erro
                logging.warning(f"Exemplos de erros em {entity}:")
                for _, row in error_examples.iterrows():
                    logging.warning(f"  - {row['error_type']}: {row['error_message']}")
        
        # Decidir se continuar baseado no status
        success = status == "SUCESSO"
        error_threshold = kwargs.get('error_threshold', 0.1)  # Máximo 10% de erros por padrão
        
        if records_processed > 0:
            error_rate = records_error / records_processed
            logging.info(f"{entity} - Taxa de erro: {error_rate:.2%}")
            
            # Continuar se:
            # 1. Status é SUCESSO, OU
            # 2. Tem erros mas estão abaixo do limiar aceitável
            return success or (error_rate <= error_threshold)
        
        return success
        
    except Exception as e:
        logging.error(f"Erro ao verificar logs de processo: {str(e)}")
        return False


def send_slack_notification(**kwargs):
    """
    Prepara e envia notificação para o Slack com resultados do pipeline.
    """
    execution_date = kwargs['execution_date'].strftime("%Y-%m-%d")
    
    # Recuperar informações registradas pelas tasks anteriores
    ti = kwargs['ti']
    
    # Status das camadas
    landing_to_bronze_status = ti.xcom_pull(key='landing_to_bronze_status') or "DESCONHECIDO"
    bronze_to_silver_status = ti.xcom_pull(key='bronze_to_silver_status') or "DESCONHECIDO"  
    silver_to_gold_status = ti.xcom_pull(key='silver_to_gold_status') or "DESCONHECIDO"
    
    # Métricas
    payment_files = ti.xcom_pull(key='payment_files_count') or 0
    transaction_files = ti.xcom_pull(key='transaction_files_count') or 0
    
    landing_to_bronze_records = ti.xcom_pull(key='landing_to_bronze_records_processed') or 0
    bronze_to_silver_records = ti.xcom_pull(key='bronze_to_silver_records_processed') or 0
    silver_to_gold_records = ti.xcom_pull(key='silver_to_gold_records_processed') or 0
    
    landing_to_bronze_errors = ti.xcom_pull(key='landing_to_bronze_records_error') or 0
    bronze_to_silver_errors = ti.xcom_pull(key='bronze_to_silver_records_error') or 0
    silver_to_gold_errors = ti.xcom_pull(key='silver_to_gold_records_error') or 0
    
    # Status geral
    if all(status == "SUCESSO" for status in [landing_to_bronze_status, bronze_to_silver_status, silver_to_gold_status]):
        overall_status = ":large_green_circle: Sucesso"
        color = "#36a64f"  # verde
    elif any(status == "ERRO" for status in [landing_to_bronze_status, bronze_to_silver_status, silver_to_gold_status]):
        overall_status = ":red_circle: Falha"
        color = "#ff0000"  # vermelho
    else:
        overall_status = ":large_yellow_circle: Parcial"
        color = "#ffcc00"  # amarelo
    
    # Construção da mensagem
    message = {
        "attachments": [
            {
                "color": color,
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f"Pipeline de Inadimplência - {overall_status}"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Data de Execução:* {execution_date}\n*Pipeline:* ETL Inadimplência SaudeBliss"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*Arquivos Processados:*\nPagamentos: {payment_files}\nTransações: {transaction_files}"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*Status das Camadas:*\nLanding → Bronze: {landing_to_bronze_status}\nBronze → Silver: {bronze_to_silver_status}\nSilver → Gold: {silver_to_gold_status}"
                            }
                        ]
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*Métricas de Processamento:*"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*Landing → Bronze:*\nRegistros: {landing_to_bronze_records}\nErros: {landing_to_bronze_errors}"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*Bronze → Silver:*\nRegistros: {bronze_to_silver_records}\nErros: {bronze_to_silver_errors}"
                            }
                        ]
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*Silver → Gold:*\nRegistros: {silver_to_gold_records}\nErros: {silver_to_gold_errors}"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*Mais Informações:*\n<https://console.cloud.google.com/logs/query?project={PROJECT_ID}|Log Explorer>"
                            }
                        ]
                    }
                ]
            }
        ]
    }
    
    return message


# ----- Definição das Tasks -----

# Task para verificar arquivos novos
check_files = ShortCircuitOperator(
    task_id='check_new_files',
    python_callable=check_new_files,
    dag=dag,
)

# Processamento Landing to Bronze - Utiliza PySpark local para transformação simples
landing_to_bronze = SparkSubmitOperator(
    task_id='landing_to_bronze',
    application="{{ var.value.spark_scripts_path }}/script-landing-bronze.py",
    name="SaudeBliss-LandingToBronze-{{ ds }}",
    conn_id='spark_default',
    conf={
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.spark_catalog.type": "hadoop",
        "spark.sql.catalog.spark_catalog.warehouse": f"gs://{BUCKET_NAME}"
    },
    application_args=['--entity', 'transactions', '--execution-date', '{{ ds }}'],
    dag=dag,
)

# Verificação de status do Landing to Bronze
check_landing_to_bronze = PythonOperator(
    task_id='check_landing_to_bronze',
    python_callable=check_process_logs,
    op_kwargs={
        'entity': 'LandingToBronze',
        'error_threshold': 0.05  # Máximo 5% de erros
    },
    dag=dag,
)

# Cria cluster Dataproc para executar as transformações mais pesadas
create_dataproc_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    cluster_config=CLUSTER_CONFIG,
    region=REGION,
    dag=dag,
)

# Processamento Bronze to Silver
bronze_to_silver_job = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"{PYSPARK_URI}/bronze_to_silver.py",
        "args": [
            "--bronze-path", f"gs://{BUCKET_NAME}/bronze",
            "--silver-path", f"gs://{BUCKET_NAME}/silver",
            "--silver-db", "silver",
            "--num-partitions", "8",
            "--warehouse-path", f"gs://{BUCKET_NAME}"
        ],
    },
}

bronze_to_silver = DataprocSubmitJobOperator(
    task_id='bronze_to_silver',
    project_id=PROJECT_ID,
    region=REGION,
    job=bronze_to_silver_job,
    dag=dag,
)

# Verificação de status do Bronze to Silver
check_bronze_to_silver = PythonOperator(
    task_id='check_bronze_to_silver',
    python_callable=check_process_logs,
    op_kwargs={
        'entity': 'BronzeToSilver',
        'error_threshold': 0.05  # Máximo 5% de erros
    },
    dag=dag,
)

# Processamento Silver to Gold
silver_to_gold_job = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"{PYSPARK_URI}/silver_to_gold.py",
        "args": [
            "--silver-path", f"gs://{BUCKET_NAME}/silver",
            "--gold-path", f"gs://{BUCKET_NAME}/gold",
            "--gold-db", "gold",
            "--execution-date", "{{ ds }}",
            "--num-partitions", "8",
            "--enable-bucketing",
            "--warehouse-path", f"gs://{BUCKET_NAME}"
        ],
    },
}

silver_to_gold = DataprocSubmitJobOperator(
    task_id='silver_to_gold',
    project_id=PROJECT_ID,
    region=REGION,
    job=silver_to_gold_job,
    dag=dag,
)

# Verificação de status do Silver to Gold
check_silver_to_gold = PythonOperator(
    task_id='check_silver_to_gold',
    python_callable=check_process_logs,
    op_kwargs={
        'entity': 'SilverToGold',
        'error_threshold': 0.02  # Máximo 2% de erros - mais estrito na camada analítica
    },
    dag=dag,
)

# Deletar o cluster após processamento 
delete_dataproc_cluster = DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    trigger_rule='all_done',  # Executar mesmo se houver falhas anteriores
    dag=dag,
)

# Verificação de qualidade de dados
quality_check = BigQueryCheckOperator(
    task_id='quality_check',
    sql='''
    SELECT
        IF(COUNT(*) > 0 AND (SELECT COUNT(*) FROM `gold.inadimplencia_metricas`) > 0, 1, 0) as success
    FROM
        `gold.inadimplencia_clientes`
    WHERE
        inadimplente = true
        AND reference_month = "{{ execution_date.strftime('%Y-%m') }}"
    ''',
    use_legacy_sql=False,
    dag=dag,
)

# Envio de notificação para Slack
slack_notification = SlackWebhookOperator(
    task_id='slack_notification',
    webhook_token=Variable.get('slack_webhook_token', default_var=''),
    message="",
    http_conn_id='slack_conn',
    webhook_method='POST',
    python_callable=send_slack_notification,
    dag=dag,
)

# Definição do fluxo de tarefas
check_files >> landing_to_bronze >> check_landing_to_bronze
check_landing_to_bronze >> create_dataproc_cluster >> bronze_to_silver
bronze_to_silver >> check_bronze_to_silver >> silver_to_gold 
silver_to_gold >> check_silver_to_gold
check_silver_to_gold >> [quality_check, delete_dataproc_cluster]
quality_check >> slack_notification
delete_dataproc_cluster >> slack_notification