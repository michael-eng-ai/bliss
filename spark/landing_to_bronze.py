#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Pipeline de Ingestão: Landing Zone → Bronze Layer.

Este script implementa o processo de ingestão de dados da Landing Zone (arquivos Parquet 
gerados pelo Airbyte) para a camada Bronze usando o formato Iceberg.

Funcionalidades:
1. Identificação automática dos arquivos novos do Airbyte
2. Validação de esquema e tipos de dados
3. Rastreamento de origem para governança de dados
4. Registro detalhado de processos e erros
5. Resolução automática de problemas de calendário em timestamps
6. Adicionar colunas de auditoria e hash para rastreabilidade

Autor: Time de Engenharia de Dados SaudeBliss
Data: Abril/2025
"""

import logging
import os
import json
import yaml
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from google.cloud import storage
import uuid
import sys
from typing import Dict, List, Any, Optional

# Importações específicas para a aplicação
try:
    from utils.spark_session import create_spark_session
    from utils.logging_config import configure_logging
    from utils.metrics import log_data_metrics
except ImportError:
    # Fallback para execução standalone
    def create_spark_session(app_name="Landing to Bronze", enable_hive=True, enable_iceberg=True):
        """Cria uma sessão Spark com configurações necessárias."""
        builder = SparkSession.builder.appName(app_name)
        
        if enable_iceberg:
            builder = builder \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
                .config("spark.sql.catalog.spark_catalog.warehouse", "gs://saudebliss-datalake")
        
        if enable_hive:
            builder = builder.enableHiveSupport()
            
        return builder.getOrCreate()
    
    def configure_logging():
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def log_data_metrics(logger, name, count, partitions, other=None):
        logger.info(f"Métricas para {name}: registros={count}, partições={partitions}")


class LandingToBronzeProcessor:
    """
    Processador para transformação de dados da Landing Zone para Bronze.
    
    Realiza as seguintes operações:
    - Descoberta e seleção de arquivos Parquet na Landing Zone
    - Validação de esquema dos dados
    - Aplicação de transformações básicas para padronização
    - Adição de colunas de auditoria e rastreabilidade
    - Registro detalhado de processos e erros 
    - Escrita de dados no formato Iceberg na camada Bronze
    """
    
    def __init__(
        self,
        spark: SparkSession,
        entity_name: str,
        bucket_name: str = "saudebliss-datalake",
        landing_path: str = None,
        bronze_path: str = None,
        execution_date: str = None
    ):
        """
        Inicializa o processador.
        
        Args:
            spark: Sessão Spark ativa
            entity_name: Nome da entidade/tabela a ser processada
            bucket_name: Nome do bucket de armazenamento
            landing_path: Caminho na landing zone (opcional)
            bronze_path: Caminho na camada bronze (opcional)
            execution_date: Data de execução no formato YYYY-MM-DD (opcional)
        """
        self.spark = spark
        self.entity_name = entity_name
        self.bucket_name = bucket_name
        
        # Definição dos caminhos no storage
        self.landing_path = landing_path or f"landing/{entity_name}"
        self.bronze_path = bronze_path or f"bronze/{entity_name}"
        self.log_path = f"logs/landing_to_bronze/{entity_name}"
        
        # Caminhos completos
        self.input_path = f"gs://{bucket_name}/{self.landing_path}"
        self.output_path = f"gs://{bucket_name}/{self.bronze_path}"
        self.log_file_path = f"gs://{bucket_name}/{self.log_path}"
        
        # Configuração de logging
        self.logger = logging.getLogger(f'landing_to_bronze.{entity_name}')
        
        # Definição de variáveis de tempo para rastreabilidade
        self.execution_datetime = datetime.now() if not execution_date else datetime.strptime(execution_date, "%Y-%m-%d")
        self.execution_id = self.execution_datetime.strftime("%Y%m%d_%H%M%S")
        self.formatted_datetime = self.execution_datetime.strftime("%Y-%m-%d %H:%M:%S")
        
        # Coleção para armazenar registros de erro
        self.error_records = []
        
        # Inicializar processo
        self.process_info = self._initialize_process_info()
        
        # Log de inicialização
        self.logger.info(f"Iniciando processamento de {entity_name} em {self.formatted_datetime}")
    
    def _initialize_process_info(self) -> Dict[str, Any]:
        """
        Inicializa o registro do processo que será atualizado em cada etapa.
        
        Returns:
            Dicionário com informações iniciais do processo
        """
        return {
            "process_id": str(uuid.uuid4()),  # ID único do processo
            "execution_id": self.execution_id,
            "entity_name": self.entity_name,
            "process_name": f"LandingToBronze_{self.entity_name}",
            "start_time": self.formatted_datetime,
            "end_time": "",
            "status": "STARTED",
            "error_message": "",
            "input_path": self.input_path,
            "output_path": self.output_path,
            "total_files_processed": 0,
            "total_records_processed": 0,
            "total_records_error": 0,
            "file_discovery_start": "",
            "file_discovery_end": "",
            "schema_validation_start": "",
            "schema_validation_end": "",
            "data_loading_start": "",
            "data_loading_end": "",
            "data_transformation_start": "",
            "data_transformation_end": "",
            "data_writing_start": "",
            "data_writing_end": ""
        }
    
    def find_parquet_files(self, bucket, prefix):
        """Encontra arquivos parquet no diretório especificado."""
        blobs = list(bucket.list_blobs(prefix=prefix))
        return [blob for blob in blobs if blob.name.endswith('.parquet')]

    def get_most_recent_parquet(self, parquet_files):
        """Retorna o arquivo parquet mais recente com base na data de atualização."""
        if not parquet_files:
            return None
        
        most_recent = parquet_files[0]
        for blob in parquet_files:
            if blob.updated > most_recent.updated:
                most_recent = blob
        return most_recent

    # Modificando para lidar com arquivos JSON
    def find_json_files(self, bucket, prefix):
        """Encontra arquivos JSON no diretório especificado."""
        blobs = list(bucket.list_blobs(prefix=prefix))
        return [blob for blob in blobs if blob.name.endswith('.json')]

    def get_most_recent_json(self, json_files):
        """Retorna o arquivo JSON mais recente com base na data de atualização."""
        if not json_files:
            return None
        
        most_recent = json_files[0]
        for blob in json_files:
            if blob.updated > most_recent.updated:
                most_recent = blob
        return most_recent

    def get_file_metadata(self, blob):
        """Extrair metadados do arquivo para rastreabilidade."""
        return {
            "file_name": blob.name.split("/")[-1],
            "file_path": blob.name,
            "file_size": blob.size,
            "creation_time": blob.time_created.isoformat() if hasattr(blob, 'time_created') else None,
            "last_modified": blob.updated.isoformat() if hasattr(blob, 'updated') else None
        }

    def create_error_record(self, record, error_type, error_message, source_file):
        """Cria um registro de erro para a tabela de erros."""
        error_record = {
            "error_id": str(uuid.uuid4()),
            "process_id": self.process_info["process_id"],
            "entity_name": self.entity_name,
            "error_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_type": error_type,
            "error_message": error_message,
            "source_file": source_file,
            "record_content": json.dumps(record)
        }
        return error_record

    def save_process_log(self):
        """Salva o log do processo na tabela de processos."""
        try:
            # Finalizar informações do processo
            self.process_info["end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            self.logger.info('\n' + '-'*50)
            self.logger.info('Resumo da execução:')
            for key, value in self.process_info.items():
                self.logger.info(f' - {key}: {value}')
                
            # Criar DataFrame de log do processo
            df_process_log = self.spark.createDataFrame([self.process_info])
            
            # Salvar em formato Iceberg
            processes_table_path = f"gs://{self.bucket_name}/control/process_logs"
            
            # Verificar se a tabela já existe
            try:
                self.spark.read.format("iceberg").load(processes_table_path)
                table_exists = True
            except:
                table_exists = False
                
            if table_exists:
                df_process_log.write \
                    .format("iceberg") \
                    .mode("append") \
                    .save(processes_table_path)
            else:
                df_process_log.write \
                    .format("iceberg") \
                    .option("write.format.default", "parquet") \
                    .option("write.metadata.compression-codec", "gzip") \
                    .partitionBy("entity_name") \
                    .saveAsTable("control.process_logs", path=processes_table_path)
                    
            self.logger.info(f"Log do processo salvo em: {processes_table_path}")
        except Exception as e:
            self.logger.error(f"Erro ao salvar log do processo: {str(e)}")

    def save_error_records(self):
        """Salva registros de erro na tabela de erros."""
        if not self.error_records:
            self.logger.info("Sem registros de erro para salvar")
            return
            
        try:
            # Criar DataFrame com os erros
            df_errors = self.spark.createDataFrame(self.error_records)
            
            # Salvar em formato Iceberg
            errors_table_path = f"gs://{self.bucket_name}/control/error_records"
            
            # Verificar se a tabela já existe
            try:
                self.spark.read.format("iceberg").load(errors_table_path)
                table_exists = True
            except:
                table_exists = False
                
            if table_exists:
                df_errors.write \
                    .format("iceberg") \
                    .mode("append") \
                    .save(errors_table_path)
            else:
                df_errors.write \
                    .format("iceberg") \
                    .option("write.format.default", "parquet") \
                    .option("write.metadata.compression-codec", "gzip") \
                    .partitionBy("entity_name", "error_type") \
                    .saveAsTable("control.error_records", path=errors_table_path)
                    
            self.logger.info(f"Registros de erro salvos: {len(self.error_records)}")
        except Exception as e:
            self.logger.error(f"Erro ao salvar registros de erro: {str(e)}")

    def get_expected_schema(self):
        """Retorna o esquema esperado para a entidade atual."""
        expected_schemas = {
            "transactions": StructType([
                StructField("transaction_id", StringType(), False),
                StructField("customer_id", StringType(), False),
                StructField("amount", DoubleType(), True),
                StructField("date", StringType(), True)
            ]),
            "payments": StructType([
                StructField("payment_id", StringType(), False),
                StructField("transaction_id", StringType(), False),
                StructField("amount_paid", DoubleType(), True),
                StructField("date_paid", StringType(), True)
            ])
        }
        
        if self.entity_name not in expected_schemas:
            raise ValueError(f"Esquema não definido para a entidade: {self.entity_name}")
            
        return expected_schemas[self.entity_name]

    def process(self):
        """
        Função principal para processar os arquivos JSON para Iceberg.
        Realiza a descoberta dos arquivos, validação, transformação e escrita dos dados.
        """
        try:
            # 1. Descoberta dos arquivos
            self.process_info["file_discovery_start"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.logger.info(f"Iniciando descoberta de arquivos em: {self.input_path}")
            
            client = storage.Client()
            bucket = client.bucket(self.bucket_name)
            
            # Primeiro tentamos encontrar arquivos Parquet
            parquet_files = self.find_parquet_files(bucket, self.landing_path)
            
            # Se não houver arquivos Parquet, tentamos encontrar arquivos JSON
            if not parquet_files:
                self.logger.info("Nenhum arquivo Parquet encontrado. Buscando arquivos JSON...")
                json_files = self.find_json_files(bucket, self.landing_path)
                
                if not json_files:
                    # Verifica se o arquivo específico existe na raiz do projeto
                    file_name = f"{self.entity_name}.json"
                    self.logger.info(f"Tentando encontrar arquivo {file_name} na raiz do projeto...")
                    
                    # Caminho direto para o arquivo JSON na raiz
                    direct_path = file_name
                    source_path = direct_path
                    
                    # Verifica se o arquivo existe
                    import os
                    if not os.path.exists(direct_path):
                        self.process_info["status"] = "CONCLUÍDO"
                        self.process_info["error_message"] = "Nenhum arquivo encontrado para processamento"
                        self.logger.info(f"Arquivo {direct_path} não encontrado")
                        self.process_info["file_discovery_end"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        return
                    
                    self.logger.info(f"Arquivo encontrado: {source_path}")
                else:
                    # Obtém o arquivo JSON mais recente
                    source_blob = self.get_most_recent_json(json_files)
                    source_path = f"gs://{self.bucket_name}/{source_blob.name}"
                    self.logger.info(f"Arquivo JSON encontrado: {source_path}")
            else:
                # Obtém o arquivo Parquet mais recente
                source_blob = self.get_most_recent_parquet(parquet_files)
                source_path = f"gs://{self.bucket_name}/{source_blob.name}"
                self.logger.info(f"Arquivo Parquet encontrado: {source_path}")
            
            self.process_info["file_discovery_end"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 2. Validação de esquema
            self.process_info["schema_validation_start"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            expected_schema = self.get_expected_schema()
            self.process_info["schema_validation_end"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 3. Carregamento dos dados
            self.process_info["data_loading_start"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.logger.info(f"Iniciando carregamento do arquivo: {source_path}")
            
            # Leitura com validação de esquema
            df = self.spark.read.schema(expected_schema).json(source_path)
            record_count = df.count()
            self.logger.info(f"Arquivo carregado com sucesso. Registros: {record_count}")
            self.process_info["data_loading_end"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 4. Transformação e enriquecimento dos dados
            self.process_info["data_transformation_start"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Verificar colunas de timestamp e configurar calendário
            self.logger.info("Configurando calendário para timestamps")
            timestamp_columns = [field.name for field in df.schema.fields 
                               if str(field.dataType) == "TimestampType()"]
            
            # Se houver colunas de timestamp, verificar datas antigas
            if timestamp_columns:
                has_old_dates = False
                for col_name in timestamp_columns:
                    if df.filter(col(col_name) < '1900-01-01').count() > 0:
                        has_old_dates = True
                        break
                calendar_spark = 'LEGACY' if has_old_dates else 'CORRECTED'
            else:
                calendar_spark = 'CORRECTED'  # Padrão
            
            # Configurar calendário
            self.spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", calendar_spark)
            self.spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", calendar_spark)
            self.logger.info(f"Calendário configurado: {calendar_spark}")
            
            # Identificar colunas para chave hash
            df_columns = df.columns
            
            # Identificar automaticamente colunas chave (baseado em prefixos comuns)
            key_prefixes = ['id', 'cod_', 'num_']
            key_columns = [c for c in df_columns if any(c.startswith(prefix) for prefix in key_prefixes) 
                          or any(prefix in c for prefix in key_prefixes)]
            
            # Se não encontrar nenhuma coluna chave, usar todas as colunas
            hash_columns = key_columns if key_columns else df_columns
            self.logger.info(f"Colunas para hash: {', '.join(hash_columns)}")
            
            # Adicionar colunas de auditoria e hash
            df_bronze = df.withColumns({
                # Colunas de auditoria
                "dt_ingestao": lit(datetime.now().strftime("%Y-%m-%d")),
                "dt_atualizacao": lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                "source_file": lit(source_path),
                "process_id": lit(self.process_info["process_id"]),
                "execution_id": lit(self.execution_id),
                
                # Colunas de hash para rastreabilidade
                "hash_chave": sha2(concat_ws("||", *[col(c) for c in hash_columns]), 256),
                "hash_columns": sha2(concat_ws("||", *[col(c) for c in df_columns]), 256)
            })
            
            # Convertendo datas para formatos adequados
            # Transactions
            if self.entity_name == "transactions":
                df_bronze = df_bronze.withColumn("transaction_date", to_timestamp(col("date")))
                df_bronze = df_bronze.withColumn("due_date", date_add(to_date(col("date")), 30))  # Assumindo vencimento 30 dias após
                df_bronze = df_bronze.withColumn("year", year(col("transaction_date")))
                df_bronze = df_bronze.withColumn("month", month(col("transaction_date")))
            # Payments
            elif self.entity_name == "payments":
                df_bronze = df_bronze.withColumn("payment_date", to_timestamp(col("date_paid")))
                df_bronze = df_bronze.withColumn("year", year(col("payment_date")))
                df_bronze = df_bronze.withColumn("month", month(col("payment_date")))
            
            self.process_info["data_transformation_end"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 5. Escrita dos dados na camada Bronze como Iceberg
            self.process_info["data_writing_start"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.logger.info(f"Iniciando escrita na camada Bronze: {self.output_path}")
            
            # Verificar se a tabela já existe
            table_exists = False
            try:
                self.spark.read.format("iceberg").load(self.output_path)
                table_exists = True
                self.logger.info("Tabela já existe. Realizando merge/append.")
            except:
                self.logger.info("Tabela não existe. Criando nova tabela.")
            
            # Se a tabela existe, fazer merge/append. Caso contrário, criar nova tabela
            if table_exists:
                # Usando merge para atualizar registros existentes ou inserir novos
                try:
                    # Merge usando Iceberg
                    target_table = f"spark_catalog.bronze.{self.entity_name}"
                    source_view = f"df_{self.entity_name}_source"
                    df_bronze.createOrReplaceTempView(source_view)
                    
                    # Executar merge SQL
                    self.spark.sql(f"""
                        MERGE INTO {target_table} t
                        USING {source_view} s
                        ON t.hash_chave = s.hash_chave
                        WHEN MATCHED THEN UPDATE SET *
                        WHEN NOT MATCHED THEN INSERT *
                    """)
                    
                    # Obter contagem atualizada
                    updated_count = self.spark.read.format("iceberg").load(self.output_path).count()
                    self.process_info["total_records_processed"] = record_count
                except Exception as merge_error:
                    self.logger.warning(f"Erro no merge: {str(merge_error)}. Tentando append como fallback.")
                    # Fallback para append
                    df_bronze.write \
                        .format("iceberg") \
                        .mode("append") \
                        .save(self.output_path)
                    self.process_info["total_records_processed"] = record_count
            else:
                # Criar nova tabela
                df_bronze.write \
                    .format("iceberg") \
                    .option("write.format.default", "parquet") \
                    .option("write.metadata.compression-codec", "snappy") \
                    .partitionBy("year", "month") \
                    .saveAsTable(f"bronze.{self.entity_name}", path=self.output_path)
                self.process_info["total_records_processed"] = record_count
            
            self.logger.info(f"Dados escritos com sucesso na camada Bronze")
            self.process_info["data_writing_end"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.process_info["status"] = "SUCESSO"
            self.process_info["total_files_processed"] = 1
            
            # Opcional: Mover o arquivo processado para uma pasta de "processados" ou excluir
            # source_blob.delete()  # Descomentar se quiser excluir o arquivo original
            
        except Exception as e:
            error_message = str(e)
            self.logger.error(f"Erro no processamento: {error_message}", exc_info=True)
            self.process_info["status"] = "ERRO"
            self.process_info["error_message"] = error_message
            
            # Registrar erro no sistema
            error_record = {
                "error_id": str(uuid.uuid4()),
                "process_id": self.process_info["process_id"],
                "entity_name": self.entity_name,
                "error_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "error_type": "PROCESS_FAILURE",
                "error_message": error_message,
                "source_file": self.input_path,
                "record_content": "N/A"  # Não temos um registro específico para este tipo de erro
            }
            self.error_records.append(error_record)
            
        finally:
            # Salvar logs e encerrar
            if self.error_records:
                self.save_error_records()
                self.process_info["total_records_error"] = len(self.error_records)
                
            self.save_process_log()
            self.logger.info("Processo finalizado")
    
def main():
    """Função principal para execução do script."""
    # Configuração do parser de argumentos
    parser = argparse.ArgumentParser(description='Pipeline de Ingestão Landing to Bronze')
    parser.add_argument('--entity', 
                        type=str, 
                        choices=['transactions', 'payments'], 
                        required=True, 
                        help='Nome da entidade/tabela a ser processada')
    parser.add_argument('--bucket', 
                        type=str, 
                        default='saudebliss-datalake', 
                        help='Nome do bucket de armazenamento')
    parser.add_argument('--landing-path', 
                        type=str, 
                        help='Caminho para os dados na landing zone')
    parser.add_argument('--bronze-path', 
                        type=str, 
                        help='Caminho para os dados na camada bronze')
    parser.add_argument('--execution-date', 
                        type=str, 
                        help='Data de execução no formato YYYY-MM-DD')
    
    args = parser.parse_args()
    
    # Configurar logging
    try:
        configure_logging()
    except:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    logger = logging.getLogger('landing_to_bronze.main')
    logger.info("Inicializando processamento Landing to Bronze")
    
    try:
        # Criar sessão Spark
        app_name = f"SaudeBliss_LandingToBronze_{args.entity}"
        try:
            spark = create_spark_session(app_name=app_name)
        except:
            spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
                .config("spark.sql.catalog.spark_catalog.warehouse", "gs://saudebliss-datalake") \
                .getOrCreate()
        
        # Criar e executar o processador
        processor = LandingToBronzeProcessor(
            spark=spark,
            entity_name=args.entity,
            bucket_name=args.bucket,
            landing_path=args.landing_path,
            bronze_path=args.bronze_path,
            execution_date=args.execution_date
        )
        
        processor.process()
        
    except Exception as e:
        logger.error(f"Erro na execução do script: {str(e)}", exc_info=True)
        sys.exit(1)
    
    logger.info("Processamento Landing to Bronze concluído com sucesso")

if __name__ == "__main__":
    main()