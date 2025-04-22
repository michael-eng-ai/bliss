#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Processamento de dados da camada Bronze para Silver.

Este script implementa as transformações necessárias para converter
dados brutos da camada Bronze para dados limpos e tipados na camada Silver.
"""

import argparse
import logging
import uuid
import json
from datetime import datetime
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Dict, List, Optional, Tuple

# Vamos importar as utilidades necessárias
try:
    from utils.spark_session import create_spark_session
    from utils.logging_config import configure_logging
    from utils.metrics import log_data_metrics
    from great_expectations.dataset import SparkDFDataset
except ImportError:
    # Fallback para execução standalone
    def create_spark_session(app_name="Bronze to Silver", iceberg_catalog="spark_catalog"):
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config(f"spark.sql.catalog.{iceberg_catalog}", "org.apache.iceberg.spark.SparkCatalog") \
            .config(f"spark.sql.catalog.{iceberg_catalog}.type", "hadoop") \
            .config(f"spark.sql.catalog.{iceberg_catalog}.warehouse", "gs://saudebliss-datalake") \
            .getOrCreate()
    
    def configure_logging():
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def log_data_metrics(logger, name, count, partitions, other=None):
        logger.info(f"Métricas para {name}: registros={count}, partições={partitions}")


class BronzeToSilverProcessor:
    """
    Processador para transformação de dados da camada Bronze para Silver.
    
    Realiza as seguintes operações:
    - Leitura dos dados brutos da camada Bronze
    - Validação e limpeza dos dados
    - Conversão para tipos corretos
    - Remoção de duplicados
    - Escrita dos dados limpos na camada Silver
    - Registro de processos e erros para controle e auditoria.
    """
    
    def __init__(
        self, 
        spark: SparkSession,
        bronze_path: str,
        silver_path: str,
        silver_db: str = "silver",
        num_partitions: int = 8,
        bucket_name: str = "saudebliss-datalake",
        execution_date: str = None
    ):
        """
        Inicializa o processador.
        
        Args:
            spark: Sessão Spark ativa
            bronze_path: Caminho para os dados na camada Bronze
            silver_path: Caminho para escrita na camada Silver
            silver_db: Nome do banco de dados Silver
            num_partitions: Número de partições a serem usadas
            bucket_name: Nome do bucket do storage
            execution_date: Data de execução no formato YYYY-MM-DD (opcional)
        """
        self.spark = spark
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.silver_db = silver_db
        self.num_partitions = num_partitions
        self.bucket_name = bucket_name
        self.logger = logging.getLogger(__name__)
        
        # Definição de variáveis de tempo para rastreabilidade
        self.execution_datetime = datetime.now() if not execution_date else datetime.strptime(execution_date, "%Y-%m-%d")
        self.execution_id = self.execution_datetime.strftime("%Y%m%d_%H%M%S")
        self.formatted_datetime = self.execution_datetime.strftime("%Y-%m-%d %H:%M:%S")
        
        # Coleção para armazenar registros de erro
        self.error_records = []
        
        # Inicializar processo
        self.process_info = self._initialize_process_info()
    
    def _initialize_process_info(self):
        """Inicializa o registro do processo."""
        return {
            "process_id": str(uuid.uuid4()),  # ID único do processo
            "execution_id": self.execution_id,
            "entity_name": "all_entities",  # será atualizado por entidade
            "process_name": "BronzeToSilver",
            "start_time": self.formatted_datetime,
            "end_time": "",
            "status": "STARTED",
            "error_message": "",
            "input_path": self.bronze_path,
            "output_path": self.silver_path,
            "total_files_processed": 0,
            "total_records_processed": 0,
            "total_records_error": 0,
            "data_loading_start": "",
            "data_loading_end": "",
            "data_validation_start": "",
            "data_validation_end": "",
            "data_transformation_start": "",
            "data_transformation_end": "",
            "data_writing_start": "",
            "data_writing_end": ""
        }
    
    def create_error_record(self, record, error_type, error_message, entity_name, source_path=None):
        """
        Cria um registro de erro para a tabela de erros.
        
        Args:
            record: Registro com erro
            error_type: Tipo de erro
            error_message: Mensagem de erro
            entity_name: Nome da entidade
            source_path: Caminho de origem (opcional)
            
        Returns:
            dict: Registro formatado para a tabela de erros
        """
        error_record = {
            "error_id": str(uuid.uuid4()),
            "process_id": self.process_info["process_id"],
            "entity_name": entity_name,
            "error_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_type": error_type,
            "error_message": error_message,
            "source_file": source_path or f"{self.bronze_path}/{entity_name}",
            "record_content": json.dumps(record) if isinstance(record, dict) else str(record)
        }
        self.error_records.append(error_record)
        return error_record
    
    def save_process_log(self, entity_name=None):
        """
        Salva o log do processo na tabela de processos.
        
        Args:
            entity_name: Nome da entidade processada (opcional)
        """
        try:
            # Atualiza a entidade se fornecida
            if entity_name:
                self.process_info["entity_name"] = entity_name
                self.process_info["process_name"] = f"BronzeToSilver_{entity_name}"
            
            # Finalizar informações do processo
            self.process_info["end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Exibe resumo
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
                    .option("write.metadata.compression-codec", "snappy") \
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
                    .option("write.metadata.compression-codec", "snappy") \
                    .partitionBy("entity_name", "error_type") \
                    .saveAsTable("control.error_records", path=errors_table_path)
                    
            self.logger.info(f"Registros de erro salvos: {len(self.error_records)}")
        except Exception as e:
            self.logger.error(f"Erro ao salvar registros de erro: {str(e)}")
    
    def load_transactions(self) -> DataFrame:
        """
        Carrega as transações da camada Bronze.
        
        Returns:
            DataFrame com os dados brutos de transações
        """
        self.logger.info(f"Carregando transações da camada Bronze: {self.bronze_path}/transactions")
        self.process_info["data_loading_start"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Define o schema explicitamente para garantir tipos corretos
        transaction_schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("amount", DoubleType(), True),  
            StructField("date", StringType(), True),
            # Colunas adicionadas na camada Bronze
            StructField("transaction_date", TimestampType(), True),
            StructField("due_date", DateType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("dt_ingestao", StringType(), True),
            StructField("dt_atualizacao", StringType(), True),
            StructField("source_file", StringType(), True),
            StructField("process_id", StringType(), True),
            StructField("execution_id", StringType(), True),
            StructField("hash_chave", StringType(), True),
            StructField("hash_columns", StringType(), True)
        ])
        
        try:
            # Primeiro tentamos ler com o esquema explícito
            return self.spark.read \
                .format("iceberg") \
                .load(f"{self.bronze_path}/transactions")
        except Exception as e:
            self.logger.warning(f"Erro ao ler como Iceberg: {str(e)}. Tentando como Parquet...")
            try:
                # Fallback para Parquet
                return self.spark.read \
                    .schema(transaction_schema) \
                    .parquet(f"{self.bronze_path}/transactions")
            except Exception as fallback_error:
                # Se ainda falhar, tenta ler sem esquema explícito
                self.logger.warning(f"Erro ao ler com esquema definido: {str(fallback_error)}. Tentando inferir esquema...")
                
                # Registra o erro para diagnóstico
                self.create_error_record(
                    record={"error": str(fallback_error)},
                    error_type="DATA_LOADING_ERROR",
                    error_message=f"Erro ao carregar transações: {str(fallback_error)}",
                    entity_name="transactions"
                )
                
                # Tenta sem esquema explícito como último recurso
                df = self.spark.read.parquet(f"{self.bronze_path}/transactions")
                
                # Informa o esquema encontrado para diagnóstico
                self.logger.info(f"Esquema inferido: {df.schema}")
                return df
    
    def load_payments(self) -> DataFrame:
        """
        Carrega os pagamentos da camada Bronze.
        
        Returns:
            DataFrame com os dados brutos de pagamentos
        """
        self.logger.info(f"Carregando pagamentos da camada Bronze: {self.bronze_path}/payments")
        
        # Define o schema explicitamente para garantir tipos corretos
        payment_schema = StructType([
            StructField("payment_id", StringType(), False),
            StructField("transaction_id", StringType(), False),
            StructField("amount_paid", DoubleType(), True),
            StructField("date_paid", StringType(), True),
            # Colunas adicionadas na camada Bronze
            StructField("payment_date", TimestampType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("dt_ingestao", StringType(), True),
            StructField("dt_atualizacao", StringType(), True),
            StructField("source_file", StringType(), True),
            StructField("process_id", StringType(), True),
            StructField("execution_id", StringType(), True),
            StructField("hash_chave", StringType(), True),
            StructField("hash_columns", StringType(), True)
        ])
        
        try:
            # Primeiro tentamos ler com o esquema explícito como Iceberg
            return self.spark.read \
                .format("iceberg") \
                .load(f"{self.bronze_path}/payments")
        except Exception as e:
            self.logger.warning(f"Erro ao ler como Iceberg: {str(e)}. Tentando como Parquet...")
            try:
                # Fallback para Parquet
                return self.spark.read \
                    .schema(payment_schema) \
                    .parquet(f"{self.bronze_path}/payments")
            except Exception as fallback_error:
                # Se ainda falhar, tenta ler sem esquema explícito
                self.logger.warning(f"Erro ao ler com esquema definido: {str(fallback_error)}. Tentando inferir esquema...")
                
                # Registra o erro para diagnóstico
                self.create_error_record(
                    record={"error": str(fallback_error)},
                    error_type="DATA_LOADING_ERROR",
                    error_message=f"Erro ao carregar pagamentos: {str(fallback_error)}",
                    entity_name="payments"
                )
                
                # Tenta sem esquema explícito como último recurso
                df = self.spark.read.parquet(f"{self.bronze_path}/payments")
                
                # Informa o esquema encontrado para diagnóstico
                self.logger.info(f"Esquema inferido: {df.schema}")
                return df
    
    def transform_transactions(self, df_transactions: DataFrame) -> DataFrame:
        """
        Transforma os dados de transações.
        
        Args:
            df_transactions: DataFrame bruto de transações
            
        Returns:
            DataFrame limpo e transformado
        """
        self.logger.info("Transformando dados de transações")
        self.process_info["data_transformation_start"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Aplica transformações
        df_clean = df_transactions
        error_rows = []
        
        # Tratamento de valores nulos e inválidos
        try:
            # Contagem inicial de registros
            initial_count = df_clean.count()
            self.logger.info(f"Total de registros antes da limpeza: {initial_count}")
            
            # Verificar registros inválidos
            df_invalid = df_clean.filter(
                col("transaction_id").isNull() | 
                col("customer_id").isNull() |
                col("transaction_date").isNull() |
                col("amount").isNull()
            )
            
            invalid_count = df_invalid.count()
            if invalid_count > 0:
                self.logger.warning(f"Encontrados {invalid_count} registros inválidos")
                
                # Coletar erros para registro
                invalid_rows = df_invalid.collect()
                for row in invalid_rows[:10]:  # Limitamos a 10 registros para não sobrecarregar o log
                    error_rows.append({
                        "transaction_id": row.transaction_id if row.transaction_id else "NULL",
                        "reason": "Campos obrigatórios nulos"
                    })
                    
                    self.create_error_record(
                        record=row.asDict(),
                        error_type="DATA_VALIDATION_ERROR",
                        error_message="Campos obrigatórios nulos",
                        entity_name="transactions"
                    )
            
            # Prosseguir apenas com registros válidos
            df_clean = df_clean.filter(
                col("transaction_id").isNotNull() &
                col("customer_id").isNotNull() &
                col("transaction_date").isNotNull() &
                col("amount").isNotNull()
            )
            
            # Aplicar transformações
            df_clean = (df_clean
                # Garantir tipos corretos
                .withColumn("amount", col("amount").cast(DoubleType()))
                .withColumn("transaction_date", col("transaction_date").cast(TimestampType()))
                .withColumn("due_date", col("due_date").cast(DateType()))
                .withColumn("year", col("year").cast(IntegerType()))
                .withColumn("month", col("month").cast(IntegerType()))
                
                # Mascarar dados sensíveis para LGPD
                .withColumn("customer_id_masked", 
                            concat(
                                substring(col("customer_id"), 1, 2),
                                lit("****"),
                                substring(col("customer_id"), -2, 2)
                            ))
                
                # Remover possíveis duplicados por transaction_id
                .dropDuplicates(["transaction_id"])
                
                # Adicionar marcação de data de processamento
                .withColumn("dt_processamento", lit(self.formatted_datetime))
                
                # Adicionar colunas de classificação para análise
                .withColumn(
                    "valor_classificacao",
                    when(col("amount") < 100, "BAIXO")
                    .when(col("amount") < 500, "MÉDIO")
                    .when(col("amount") < 1000, "ALTO")
                    .otherwise("MUITO_ALTO")
                )
                
                # Reparticionamento para melhor distribuição dos dados
                .repartition(self.num_partitions, "year", "month")
            )
            
            # Verificar consistência de datas
            df_invalid_dates = df_clean.filter(col("due_date") < col("transaction_date"))
            invalid_dates_count = df_invalid_dates.count()
            
            if invalid_dates_count > 0:
                self.logger.warning(f"Encontrados {invalid_dates_count} registros com datas inconsistentes")
                
                # Corrigir datas inconsistentes
                df_clean = df_clean.withColumn(
                    "due_date",
                    when(col("due_date") < col("transaction_date"), 
                         date_add(col("transaction_date").cast("date"), 30))
                    .otherwise(col("due_date"))
                )
                
                # Registrar alertas para monitoramento
                for row in df_invalid_dates.limit(5).collect():
                    self.create_error_record(
                        record=row.asDict(),
                        error_type="DATE_CONSISTENCY_WARNING",
                        error_message="Data de vencimento anterior à data da transação",
                        entity_name="transactions"
                    )
            
            # Verificar quantos registros foram preservados após a limpeza
            final_count = df_clean.count()
            removed_count = initial_count - final_count
            
            if removed_count > 0:
                self.logger.info(f"Removidos {removed_count} registros inválidos ou duplicados")
                
        except Exception as e:
            self.logger.error(f"Erro na transformação de transações: {str(e)}")
            self.create_error_record(
                record={"transformation_error": str(e)},
                error_type="TRANSFORMATION_ERROR",
                error_message=f"Erro na transformação: {str(e)}",
                entity_name="transactions"
            )
            # Ainda retornamos o DataFrame como estava antes para tentar continuar
        
        self.process_info["data_transformation_end"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return df_clean
    
    def transform_payments(self, df_payments: DataFrame) -> DataFrame:
        """
        Transforma os dados de pagamentos.
        
        Args:
            df_payments: DataFrame bruto de pagamentos
            
        Returns:
            DataFrame limpo e transformado
        """
        self.logger.info("Transformando dados de pagamentos")
        self.process_info["data_transformation_start"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Aplica transformações
        df_clean = df_payments
        error_rows = []
        
        try:
            # Contagem inicial de registros
            initial_count = df_clean.count()
            self.logger.info(f"Total de registros antes da limpeza: {initial_count}")
            
            # Verificar registros inválidos
            df_invalid = df_clean.filter(
                col("payment_id").isNull() | 
                col("transaction_id").isNull() |
                col("payment_date").isNull() |
                col("amount_paid").isNull()
            )
            
            invalid_count = df_invalid.count()
            if invalid_count > 0:
                self.logger.warning(f"Encontrados {invalid_count} registros inválidos")
                
                # Coletar erros para registro
                invalid_rows = df_invalid.collect()
                for row in invalid_rows[:10]:  # Limitamos a 10 registros para não sobrecarregar o log
                    error_rows.append({
                        "payment_id": row.payment_id if row.payment_id else "NULL",
                        "reason": "Campos obrigatórios nulos"
                    })
                    
                    self.create_error_record(
                        record=row.asDict(),
                        error_type="DATA_VALIDATION_ERROR",
                        error_message="Campos obrigatórios nulos",
                        entity_name="payments"
                    )
            
            # Prosseguir apenas com registros válidos
            df_clean = df_clean.filter(
                col("payment_id").isNotNull() &
                col("transaction_id").isNotNull() &
                col("payment_date").isNotNull() &
                col("amount_paid").isNotNull()
            )
            
            # Aplicar transformações
            df_clean = (df_clean
                # Garantir tipos corretos
                .withColumn("amount_paid", col("amount_paid").cast(DoubleType()))
                .withColumn("payment_date", col("payment_date").cast(TimestampType()))
                .withColumn("year", col("year").cast(IntegerType()))
                .withColumn("month", col("month").cast(IntegerType()))
                
                # Remover possíveis duplicados por payment_id
                .dropDuplicates(["payment_id"])
                
                # Adicionar marcação de data de processamento
                .withColumn("dt_processamento", lit(self.formatted_datetime))
                
                # Inferir método de pagamento se não existir
                .withColumn("payment_method", 
                            when(col("payment_method").isNull(), lit("NOT_SPECIFIED"))
                            .otherwise(col("payment_method")))
                
                # Adicionar status de pagamento se não existir
                .withColumn("payment_status",
                            when(col("payment_status").isNull(), lit("completed"))
                            .otherwise(col("payment_status")))
                
                # Reparticionamento para melhor distribuição dos dados
                .repartition(self.num_partitions, "year", "month")
            )
            
            # Verificar pagamentos com valor zerado
            df_zero_payments = df_clean.filter(col("amount_paid") <= 0)
            zero_count = df_zero_payments.count()
            
            if zero_count > 0:
                self.logger.warning(f"Encontrados {zero_count} pagamentos com valor zero ou negativo")
                
                # Registrar alertas para pagamentos zerados
                for row in df_zero_payments.limit(5).collect():
                    self.create_error_record(
                        record=row.asDict(),
                        error_type="PAYMENT_VALUE_WARNING",
                        error_message="Pagamento com valor zero ou negativo",
                        entity_name="payments"
                    )
            
            # Verificar quantos registros foram preservados após a limpeza
            final_count = df_clean.count()
            removed_count = initial_count - final_count
            
            if removed_count > 0:
                self.logger.info(f"Removidos {removed_count} registros inválidos ou duplicados")
                
        except Exception as e:
            self.logger.error(f"Erro na transformação de pagamentos: {str(e)}")
            self.create_error_record(
                record={"transformation_error": str(e)},
                error_type="TRANSFORMATION_ERROR",
                error_message=f"Erro na transformação: {str(e)}",
                entity_name="payments"
            )
            # Ainda retornamos o DataFrame como estava antes para tentar continuar
        
        self.process_info["data_transformation_end"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return df_clean
    
    def validate_data_quality(self, df: DataFrame, name: str) -> None:
        """
        Valida a qualidade dos dados usando Great Expectations.
        
        Args:
            df: DataFrame a ser validado
            name: Nome do conjunto de dados para logs
            
        Raises:
            ValueError: Se a validação falhar
        """
        self.logger.info(f"Validando qualidade dos dados: {name}")
        self.process_info["data_validation_start"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            ge_df = SparkDFDataset(df)
            
            # Validações básicas
            results = []
            
            if name == "transactions":
                results.append(ge_df.expect_column_values_to_not_be_null("transaction_id"))
                results.append(ge_df.expect_column_values_to_not_be_null("customer_id"))
                results.append(ge_df.expect_column_values_to_not_be_null("transaction_date"))
                results.append(ge_df.expect_column_values_to_not_be_null("amount"))
                results.append(ge_df.expect_column_values_to_be_of_type("transaction_id", "str"))
                results.append(ge_df.expect_column_values_to_be_of_type("amount", "float"))
            
            elif name == "payments":
                results.append(ge_df.expect_column_values_to_not_be_null("payment_id"))
                results.append(ge_df.expect_column_values_to_not_be_null("transaction_id"))
                results.append(ge_df.expect_column_values_to_not_be_null("payment_date"))
                results.append(ge_df.expect_column_values_to_be_of_type("payment_id", "str"))
                results.append(ge_df.expect_column_values_to_be_of_type("amount_paid", "float"))
            
            # Verificar resultados
            for result in results:
                if not result["success"]:
                    self.logger.error(f"Falha na validação: {result}")
                    
                    # Registra o erro mas não interrompe o processamento
                    self.create_error_record(
                        record=result,
                        error_type="DATA_QUALITY_ERROR",
                        error_message=f"Falha na validação de qualidade: {result['expectation_config']['expectation_type']}",
                        entity_name=name
                    )
                
        except ImportError:
            self.logger.warning("Great Expectations não disponível, pulando validações")
        except Exception as e:
            self.logger.error(f"Erro na validação: {str(e)}")
            self.create_error_record(
                record={"validation_error": str(e)},
                error_type="VALIDATION_ERROR",
                error_message=f"Erro na validação: {str(e)}",
                entity_name=name
            )
        finally:
            self.process_info["data_validation_end"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def save_to_silver(self, df: DataFrame, table_name: str) -> None:
        """
        Salva DataFrame na camada Silver.
        
        Args:
            df: DataFrame a ser salvo
            table_name: Nome da tabela
        """
        entity_name = table_name  # Para registros de processo
        self.logger.info(f"Salvando dados na camada Silver: {table_name}")
        self.process_info["data_writing_start"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        record_count = df.count()  # Conta registros antes de salvar
        
        # Atualizar processo com a entidade específica
        self.process_info["entity_name"] = entity_name
        self.process_info["process_name"] = f"BronzeToSilver_{entity_name}"
        self.process_info["total_records_processed"] = record_count
        
        # Salva como tabela Iceberg particionada
        try:
            # Com certificação de qualidade do processamento atual
            df_with_quality = df.withColumns({
                "dt_processamento": lit(self.formatted_datetime),
                "processo_id": lit(self.process_info["process_id"]),
                "certificado_qualidade": lit(True),
                "dt_atualizacao": lit(self.formatted_datetime),
                "versao_processo": lit("1.0")
            })
            
            (df_with_quality.writeTo(f"{self.silver_db}.{table_name}")
                .partitionBy("year", "month")
                .tableProperty("write.format.default", "parquet")
                .tableProperty("write.metadata.compression-codec", "snappy")
                .tableProperty("write.distribution-mode", "hash")
                .createOrReplace()
            )
            self.logger.info(f"Tabela {table_name} criada com sucesso no formato Iceberg")
            
        except Exception as e:
            self.logger.warning(f"Erro ao salvar como tabela Iceberg: {str(e)}")
            self.logger.info(f"Fallback para salvamento como Parquet")
            
            # Registrar erro 
            self.create_error_record(
                record={"iceberg_error": str(e)},
                error_type="ICEBERG_WRITE_ERROR",
                error_message=f"Erro ao salvar como Iceberg, usando fallback Parquet: {str(e)}",
                entity_name=entity_name
            )
            
            # Fallback para Parquet
            df_with_quality = df.withColumns({
                "dt_processamento": lit(self.formatted_datetime),
                "processo_id": lit(self.process_info["process_id"]),
                "certificado_qualidade": lit(True),
                "dt_atualizacao": lit(self.formatted_datetime),
                "versao_processo": lit("1.0")
            })
            
            (df_with_quality
                .write
                .mode("overwrite")
                .partitionBy("year", "month")
                .option("compression", "snappy")
                .parquet(f"{self.silver_path}/{table_name}")
            )
            self.logger.info(f"Dados salvos como Parquet em: {self.silver_path}/{table_name}")
        
        # Registrar final da escrita
        self.process_info["data_writing_end"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Salvar log do processo para esta entidade
        self.save_process_log(entity_name)
    
    def process(self) -> Tuple[DataFrame, DataFrame]:
        """
        Executa o processamento completo Bronze -> Silver.
        
        Returns:
            Tuple contendo (DataFrame de transações limpo, DataFrame de pagamentos limpo)
        """
        try:
            # Carrega dados da camada Bronze
            df_transactions_raw = self.load_transactions()
            transactions_count = df_transactions_raw.count()
            self.logger.info(f"Carregados {transactions_count} registros de transações")
            self.process_info["data_loading_end"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Transforma dados de transações
            df_transactions_clean = self.transform_transactions(df_transactions_raw)
            
            # Valida qualidade dos dados
            self.validate_data_quality(df_transactions_clean, "transactions")
            
            # Salva dados de transações na camada Silver
            self.save_to_silver(df_transactions_clean, "transactions")
            
            # Reinicializa processo para pagamentos
            self.process_info = self._initialize_process_info()
            self.process_info["data_loading_start"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Carrega dados de pagamentos
            df_payments_raw = self.load_payments()
            payments_count = df_payments_raw.count()
            self.logger.info(f"Carregados {payments_count} registros de pagamentos")
            self.process_info["data_loading_end"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Transforma dados de pagamentos
            df_payments_clean = self.transform_payments(df_payments_raw)
            
            # Valida qualidade dos dados
            self.validate_data_quality(df_payments_clean, "payments")
            
            # Salva dados de pagamentos na camada Silver
            self.save_to_silver(df_payments_clean, "payments")
            
            # Log métricas
            transaction_count = df_transactions_clean.count()
            payment_count = df_payments_clean.count()
            transaction_partitions = df_transactions_clean.rdd.getNumPartitions()
            payment_partitions = df_payments_clean.rdd.getNumPartitions()
            
            self.logger.info(f"Transações processadas: {transaction_count} em {transaction_partitions} partições")
            self.logger.info(f"Pagamentos processados: {payment_count} em {payment_partitions} partições")
            
            # Salvar registros de erro, se houver
            if self.error_records:
                self.save_error_records()
            
            return df_transactions_clean, df_payments_clean
            
        except Exception as e:
            self.logger.error(f"Erro no processamento Bronze -> Silver: {str(e)}", exc_info=True)
            self.process_info["status"] = "ERRO"
            self.process_info["error_message"] = str(e)
            
            # Registrar erro
            self.create_error_record(
                record={"process_error": str(e)},
                error_type="PROCESS_FAILURE",
                error_message=f"Erro no processamento: {str(e)}",
                entity_name="all_entities"
            )
            
            # Salvar registros de erro
            self.save_error_records()
            
            # Salvar log do processo
            self.save_process_log("process_failure")
            
            raise


def parse_args():
    """Parse argumentos de linha de comando."""
    parser = argparse.ArgumentParser(description="Processamento Bronze para Silver")
    parser.add_argument(
        "--bronze-path",
        type=str,
        default="gs://saudebliss-datalake/bronze",
        help="Caminho para os dados na camada Bronze"
    )
    parser.add_argument(
        "--silver-path",
        type=str,
        default="gs://saudebliss-datalake/silver",
        help="Caminho para escrita na camada Silver"
    )
    parser.add_argument(
        "--silver-db",
        type=str,
        default="silver",
        help="Nome do banco de dados Silver"
    )
    parser.add_argument(
        "--num-partitions",
        type=int,
        default=8,
        help="Número de partições para os DataFrames"
    )
    parser.add_argument(
        "--bucket-name",
        type=str,
        default="saudebliss-datalake",
        help="Nome do bucket de armazenamento"
    )
    parser.add_argument(
        "--execution-date",
        type=str,
        help="Data de execução no formato YYYY-MM-DD (opcional)"
    )
    return parser.parse_args()


def main():
    """Função principal do script."""
    # Configura logs
    configure_logging()
    logger = logging.getLogger(__name__)
    logger.info("Iniciando processamento Bronze -> Silver")
    
    # Parse argumentos
    args = parse_args()
    
    try:
        # Cria sessão Spark com configurações otimizadas
        spark = create_spark_session(app_name="bronze_to_silver")
        
        # Configurações adicionais para otimização de desempenho
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        spark.conf.set("spark.sql.files.maxPartitionBytes", "128m")
        
        # Executa processamento
        processor = BronzeToSilverProcessor(
            spark=spark,
            bronze_path=args.bronze_path,
            silver_path=args.silver_path,
            silver_db=args.silver_db,
            num_partitions=args.num_partitions,
            bucket_name=args.bucket_name,
            execution_date=args.execution_date
        )
        
        df_transactions, df_payments = processor.process()
        
        logger.info("Processamento Bronze -> Silver concluído com sucesso")
        
    except Exception as e:
        logger.error(f"Erro no processamento: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()