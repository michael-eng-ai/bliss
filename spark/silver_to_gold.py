#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Processamento de dados da camada Silver para Gold.

Este script implementa a aplicação de regras de negócio e agregações
para criar visões analíticas na camada Gold a partir dos dados da camada Silver.

Autor: Time de Engenharia de Dados SaudeBliss
Data: Abril/2025
"""

import argparse
import logging
import uuid
import json
from datetime import datetime, timedelta
import sys
from typing import Dict, List, Tuple, Optional

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Importações específicas para a aplicação
try:
    from utils.spark_session import create_spark_session
    from utils.logging_config import configure_logging
    from utils.metrics import log_data_metrics
except ImportError:
    # Fallback para execução standalone
    def create_spark_session(app_name="Silver to Gold", enable_hive=True, enable_iceberg=True):
        """Cria uma sessão Spark com configurações necessárias."""
        builder = SparkSession.builder.appName(app_name)
        
        if enable_iceberg:
            builder = builder \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        
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


class SilverToGoldProcessor:
    """
    Processador para transformação de dados da camada Silver para Gold.
    
    Realiza as seguintes operações:
    - Leitura de transações e pagamentos da camada Silver
    - Aplicação das regras de negócio para identificação de inadimplência
    - Criação de visões analíticas agregadas por cliente, período e status
    - Escrita dos resultados na camada Gold
    - Registro detalhado de processos e erros para auditoria e rastreabilidade
    """
    
    def __init__(
        self,
        spark: SparkSession,
        silver_path: str,
        gold_path: str,
        gold_db: str = "gold",
        num_partitions: int = 8,
        enable_bucketing: bool = False,
        bucket_columns: List[str] = None,
        enable_hive: bool = True,
        bucket_name: str = "saudebliss-datalake",
        execution_date: str = None
    ):
        """
        Inicializa o processador.
        
        Args:
            spark: Sessão Spark ativa
            silver_path: Caminho para os dados na camada Silver
            gold_path: Caminho para escrita na camada Gold
            gold_db: Nome do banco de dados Gold
            num_partitions: Número de partições a serem usadas
            enable_bucketing: Se True, utiliza bucketing para otimizar
            bucket_columns: Colunas para bucketing (opcional)
            enable_hive: Se True, utiliza tabelas Hive
            bucket_name: Nome do bucket de armazenamento
            execution_date: Data de execução no formato YYYY-MM-DD (opcional)
        """
        self.spark = spark
        self.silver_path = silver_path
        self.gold_path = gold_path
        self.gold_db = gold_db
        self.num_partitions = num_partitions
        self.enable_bucketing = enable_bucketing
        self.bucket_columns = bucket_columns or ['customer_id', 'transaction_id']
        self.enable_hive = enable_hive
        self.bucket_name = bucket_name
        self.logger = logging.getLogger(__name__)
        
        # Definição de variáveis de tempo para rastreabilidade
        self.execution_datetime = datetime.now() if not execution_date else datetime.strptime(execution_date, "%Y-%m-%d")
        self.execution_id = self.execution_datetime.strftime("%Y%m%d_%H%M%S")
        self.formatted_datetime = self.execution_datetime.strftime("%Y-%m-%d %H:%M:%S")
        
        # Definir períodos de análise (últimos 3 e 6 meses)
        self.data_referencia = self.execution_datetime.replace(day=1) - timedelta(days=1)
        self.tres_meses_atras = (self.data_referencia - timedelta(days=90)).strftime("%Y-%m-%d")
        self.seis_meses_atras = (self.data_referencia - timedelta(days=180)).strftime("%Y-%m-%d")
        
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
            "process_name": "SilverToGold",
            "start_time": self.formatted_datetime,
            "end_time": "",
            "status": "STARTED",
            "error_message": "",
            "input_path": self.silver_path,
            "output_path": self.gold_path,
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
            "source_file": source_path or f"{self.silver_path}/{entity_name}",
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
                self.process_info["process_name"] = f"SilverToGold_{entity_name}"
            
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
        Carrega as transações da camada Silver.
        
        Returns:
            DataFrame com os dados limpos de transações
        """
        self.logger.info(f"Carregando transações da camada Silver")
        self.process_info["data_loading_start"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            # Primeiro tenta carregar como tabela Iceberg
            df_transactions = self.spark.read \
                .format("iceberg") \
                .load(f"{self.silver_path}/transactions")
            
            self.logger.info("Transações carregadas com sucesso (formato Iceberg)")
            
        except Exception as e:
            self.logger.warning(f"Erro ao ler como Iceberg: {str(e)}. Tentando como Parquet...")
            
            # Fallback para Parquet
            df_transactions = self.spark.read \
                .parquet(f"{self.silver_path}/transactions")
                
            self.logger.info("Transações carregadas com sucesso (formato Parquet)")
        
        record_count = df_transactions.count()
        partition_count = df_transactions.rdd.getNumPartitions()
        log_data_metrics(self.logger, "Transações carregadas", record_count, partition_count)
        
        return df_transactions
    
    def load_payments(self) -> DataFrame:
        """
        Carrega os pagamentos da camada Silver.
        
        Returns:
            DataFrame com os dados limpos de pagamentos
        """
        self.logger.info(f"Carregando pagamentos da camada Silver")
        
        try:
            # Primeiro tenta carregar como tabela Iceberg
            df_payments = self.spark.read \
                .format("iceberg") \
                .load(f"{self.silver_path}/payments")
                
            self.logger.info("Pagamentos carregados com sucesso (formato Iceberg)")
            
        except Exception as e:
            self.logger.warning(f"Erro ao ler como Iceberg: {str(e)}. Tentando como Parquet...")
            
            # Fallback para Parquet
            df_payments = self.spark.read \
                .parquet(f"{self.silver_path}/payments")
                
            self.logger.info("Pagamentos carregados com sucesso (formato Parquet)")
        
        record_count = df_payments.count()
        partition_count = df_payments.rdd.getNumPartitions()
        log_data_metrics(self.logger, "Pagamentos carregados", record_count, partition_count)
        
        self.process_info["data_loading_end"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return df_payments
    
    def aplicar_regras_inadimplencia(
        self, 
        df_transactions: DataFrame, 
        df_payments: DataFrame
    ) -> DataFrame:
        """
        Aplica as regras de negócio para identificar clientes inadimplentes.
        
        Args:
            df_transactions: DataFrame com as transações
            df_payments: DataFrame com os pagamentos
            
        Returns:
            DataFrame com os resultados de inadimplência
        """
        self.logger.info("Aplicando regras de negócio para identificação de inadimplência")
        self.process_info["data_transformation_start"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            # Registrar para diagnóstico
            transactions_count = df_transactions.count()
            payments_count = df_payments.count()
            self.logger.info(f"Processando {transactions_count} transações e {payments_count} pagamentos")
            
            # Preparar visão de transações
            df_transactions_prep = df_transactions \
                .select(
                    "transaction_id", "customer_id", "amount", 
                    "transaction_date", "due_date"
                )
            
            # Preparar visão de pagamentos
            df_payments_prep = df_payments \
                .select(
                    "payment_id", "transaction_id", "amount_paid", 
                    "payment_date"
                )
            
            # Verificar pagamentos zerados ou negativos
            df_invalid_payments = df_payments_prep.filter(col("amount_paid") <= 0)
            invalid_count = df_invalid_payments.count()
            
            if invalid_count > 0:
                self.logger.warning(f"Encontrados {invalid_count} pagamentos inválidos (valor <= 0)")
                
                # Registrar alguns erros para diagnóstico
                for row in df_invalid_payments.limit(5).collect():
                    self.create_error_record(
                        record=row.asDict(),
                        error_type="INVALID_PAYMENT",
                        error_message="Pagamento com valor zero ou negativo",
                        entity_name="payments"
                    )
                
                # Remover pagamentos inválidos para não afetar os cálculos
                df_payments_prep = df_payments_prep.filter(col("amount_paid") > 0)
                
            # Juntar transações e pagamentos em uma visão única
            df_joined = df_transactions_prep.join(
                df_payments_prep,
                on="transaction_id",
                how="left"
            )
            
            # Calcular diferença em dias entre data de pagamento e data de vencimento
            df_joined = df_joined.withColumn(
                "dias_atraso",
                when(col("payment_date").isNull(), 
                     datediff(current_date(), col("due_date")))  # Se não pagou, conta dias até hoje
                .otherwise(datediff(col("payment_date"), col("due_date")))  # Se pagou, conta dias até pagamento
            )
            
            # Calcular diferença entre valor da transação e pagamento
            df_joined = df_joined.withColumn(
                "valor_em_aberto",
                when(col("amount_paid").isNull(), col("amount"))  # Se não pagou nada, é o valor total
                .otherwise(col("amount") - col("amount_paid"))  # Se pagou, é a diferença
            )
            
            # Calcular regras de inadimplência
            
            # Regra 1: Pagamentos com mais de 30 dias de atraso nos últimos 3 meses
            df_regra1 = df_joined \
                .filter(
                    (col("transaction_date") >= self.tres_meses_atras) &
                    (col("dias_atraso") > 30)
                ) \
                .select(
                    "customer_id",
                    lit("regra1").alias("regra_aplicada"),
                    lit("Pagamento com mais de 30 dias de atraso nos últimos 3 meses").alias("descricao"),
                    col("transaction_id"),
                    col("amount"),
                    col("dias_atraso"),
                    col("valor_em_aberto")
                )
            
            # Regra 2: Mais de 3 pagamentos com mais de 15 dias de atraso nos últimos 6 meses
            df_regra2_base = df_joined \
                .filter(
                    (col("transaction_date") >= self.seis_meses_atras) &
                    (col("dias_atraso") > 15)
                ) \
                .groupBy("customer_id") \
                .agg(
                    count("transaction_id").alias("qtd_atrasos"),
                    sum("valor_em_aberto").alias("total_em_aberto")
                ) \
                .filter(col("qtd_atrasos") > 3)
            
            df_regra2 = df_regra2_base \
                .select(
                    "customer_id",
                    lit("regra2").alias("regra_aplicada"),
                    lit("Mais de 3 pagamentos com mais de 15 dias de atraso nos últimos 6 meses").alias("descricao"),
                    lit(None).cast("string").alias("transaction_id"),  # Não temos ID específico nesta agregação
                    lit(None).cast("double").alias("amount"),
                    lit(None).cast("integer").alias("dias_atraso"),
                    col("total_em_aberto").alias("valor_em_aberto")
                )
            
            # Combinar resultados das duas regras
            df_inadimplentes = df_regra1.union(df_regra2)
            
            # Adicionar colunas de auditoria e referência
            df_inadimplentes = df_inadimplentes \
                .withColumn("data_classificacao", lit(self.formatted_datetime)) \
                .withColumn("processo_id", lit(self.process_info["process_id"])) \
                .withColumn("dt_processamento", lit(self.formatted_datetime)) \
                .withColumn("reference_date", lit(self.data_referencia.strftime("%Y-%m-%d"))) \
                .withColumn("reference_month", lit(self.data_referencia.strftime("%Y-%m")))
            
            # Criar visão agregada por cliente
            df_inadimplentes_agg = df_inadimplentes \
                .groupBy("customer_id") \
                .agg(
                    collect_set("regra_aplicada").alias("regras_aplicadas"),
                    count("transaction_id").alias("total_ocorrencias"),
                    sum("valor_em_aberto").alias("total_valor_em_aberto"),
                    max("dias_atraso").alias("max_dias_atraso")
                ) \
                .withColumn("inadimplente", lit(True)) \
                .withColumn("data_classificacao", lit(self.formatted_datetime)) \
                .withColumn("processo_id", lit(self.process_info["process_id"])) \
                .withColumn("reference_date", lit(self.data_referencia.strftime("%Y-%m-%d"))) \
                .withColumn("reference_month", lit(self.data_referencia.strftime("%Y-%m")))
            
            # Criar lista de todos os clientes (para identificar também os não inadimplentes)
            df_todos_clientes = df_transactions \
                .select("customer_id") \
                .distinct() \
                .withColumn("inadimplente", lit(False)) \
                .withColumn("regras_aplicadas", array()) \
                .withColumn("total_ocorrencias", lit(0)) \
                .withColumn("total_valor_em_aberto", lit(0.0)) \
                .withColumn("max_dias_atraso", lit(0)) \
                .withColumn("data_classificacao", lit(self.formatted_datetime)) \
                .withColumn("processo_id", lit(self.process_info["process_id"])) \
                .withColumn("reference_date", lit(self.data_referencia.strftime("%Y-%m-%d"))) \
                .withColumn("reference_month", lit(self.data_referencia.strftime("%Y-%m")))
            
            # Unir inadimplentes com todos os clientes
            df_final = df_inadimplentes_agg \
                .join(df_todos_clientes, on="customer_id", how="right_outer") \
                .select(
                    df_todos_clientes["customer_id"],
                    coalesce(df_inadimplentes_agg["inadimplente"], df_todos_clientes["inadimplente"]).alias("inadimplente"),
                    coalesce(df_inadimplentes_agg["regras_aplicadas"], df_todos_clientes["regras_aplicadas"]).alias("regras_aplicadas"),
                    coalesce(df_inadimplentes_agg["total_ocorrencias"], df_todos_clientes["total_ocorrencias"]).alias("total_ocorrencias"),
                    coalesce(df_inadimplentes_agg["total_valor_em_aberto"], df_todos_clientes["total_valor_em_aberto"]).alias("total_valor_em_aberto"),
                    coalesce(df_inadimplentes_agg["max_dias_atraso"], df_todos_clientes["max_dias_atraso"]).alias("max_dias_atraso"),
                    df_todos_clientes["data_classificacao"],
                    df_todos_clientes["processo_id"],
                    df_todos_clientes["reference_date"],
                    df_todos_clientes["reference_month"]
                )
            
            # Adicionar indicador de severidade
            df_final = df_final \
                .withColumn(
                    "severidade",
                    when(col("max_dias_atraso") > 90, "CRÍTICA")
                    .when(col("max_dias_atraso") > 60, "ALTA")
                    .when(col("max_dias_atraso") > 30, "MÉDIA")
                    .when(col("max_dias_atraso") > 15, "BAIXA")
                    .otherwise("NENHUMA")
                )
            
            # Reparticionamento adequado
            if self.enable_bucketing:
                # Bucketing é uma otimização para junções frequentes
                df_final = df_final.repartition(self.num_partitions, "customer_id")
            else:
                df_final = df_final.repartition(self.num_partitions)
            
            # Registrar contagem de inadimplentes para diagnóstico
            inadimplentes_count = df_final.filter(col("inadimplente")).count()
            total_count = df_final.count()
            self.logger.info(f"Identificados {inadimplentes_count} clientes inadimplentes de um total de {total_count}")
            
            self.process_info["total_records_processed"] = total_count
            
            return df_final
            
        except Exception as e:
            error_message = str(e)
            self.logger.error(f"Erro ao aplicar regras de inadimplência: {error_message}")
            
            # Registrar erro
            self.create_error_record(
                record={"error": error_message},
                error_type="TRANSFORMATION_ERROR",
                error_message=f"Erro ao aplicar regras de inadimplência: {error_message}",
                entity_name="inadimplencia"
            )
            
            # Propagação da exceção
            raise
        finally:
            self.process_info["data_transformation_end"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def criar_metricas_inadimplencia(self, df_inadimplencia: DataFrame) -> DataFrame:
        """
        Cria métricas agregadas de inadimplência.
        
        Args:
            df_inadimplencia: DataFrame com dados de inadimplência
            
        Returns:
            DataFrame com métricas agregadas
        """
        self.logger.info("Criando métricas agregadas de inadimplência")
        
        try:
            # Agregações por mês de referência
            df_metrics = df_inadimplencia \
                .groupBy("reference_month") \
                .agg(
                    count("customer_id").alias("total_clientes"),
                    sum(when(col("inadimplente"), 1).otherwise(0)).alias("total_inadimplentes"),
                    avg(when(col("inadimplente"), 1).otherwise(0)).alias("taxa_inadimplencia"),
                    sum("total_valor_em_aberto").alias("valor_total_inadimplencia"),
                    max("max_dias_atraso").alias("max_dias_atraso")
                ) \
                .withColumn("data_geracao", lit(self.formatted_datetime)) \
                .withColumn("processo_id", lit(self.process_info["process_id"]))
            
            # Adicionar indicador de variação (simulado - em uma implementação real, compararíamos com o mês anterior)
            df_metrics = df_metrics \
                .withColumn("variacao_mes_anterior", lit(0.0))  # Simulado
            
            return df_metrics
            
        except Exception as e:
            error_message = str(e)
            self.logger.error(f"Erro ao criar métricas de inadimplência: {error_message}")
            
            # Registrar erro
            self.create_error_record(
                record={"error": error_message},
                error_type="METRICS_ERROR",
                error_message=f"Erro ao criar métricas de inadimplência: {error_message}",
                entity_name="inadimplencia_metricas"
            )
            
            # Propagação da exceção
            raise
    
    def criar_alertas_inadimplencia(self, df_inadimplencia: DataFrame) -> DataFrame:
        """
        Cria alertas de inadimplência baseados em critérios específicos.
        
        Args:
            df_inadimplencia: DataFrame com dados de inadimplência
            
        Returns:
            DataFrame com alertas gerados
        """
        self.logger.info("Gerando alertas de inadimplência")
        
        try:
            # Filtrar casos críticos
            df_alertas = df_inadimplencia \
                .filter(
                    (col("severidade") == "CRÍTICA") |
                    (col("total_valor_em_aberto") > 10000)  # Valor em aberto alto
                ) \
                .select(
                    "customer_id",
                    "severidade",
                    "total_ocorrencias",
                    "total_valor_em_aberto",
                    "max_dias_atraso",
                    lit(self.formatted_datetime).alias("data_alerta"),
                    array_join(col("regras_aplicadas"), ", ").alias("regras_aplicadas"),
                    col("reference_date")
                ) \
                .withColumn("processo_id", lit(self.process_info["process_id"])) \
                .withColumn("alerta_enviado", lit(False))  # Para controle de envio
            
            # Contar alertas gerados
            alertas_count = df_alertas.count()
            self.logger.info(f"Gerados {alertas_count} alertas de inadimplência")
            
            return df_alertas
            
        except Exception as e:
            error_message = str(e)
            self.logger.error(f"Erro ao gerar alertas de inadimplência: {error_message}")
            
            # Registrar erro
            self.create_error_record(
                record={"error": error_message},
                error_type="ALERT_ERROR",
                error_message=f"Erro ao gerar alertas de inadimplência: {error_message}",
                entity_name="inadimplencia_alertas"
            )
            
            # Propagação da exceção
            raise
    
    def save_to_gold(
        self, 
        df: DataFrame, 
        table_name: str, 
        partition_by: List[str] = None
    ) -> None:
        """
        Salva DataFrame na camada Gold.
        
        Args:
            df: DataFrame a ser salvo
            table_name: Nome da tabela
            partition_by: Colunas para particionamento (opcional)
        """
        entity_name = table_name.replace("inadimplencia_", "")  # Para registros de processo
        self.logger.info(f"Salvando dados na camada Gold: {table_name}")
        self.process_info["data_writing_start"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Configurações para a escrita
        partition_by = partition_by or []
        num_buckets = min(10, self.num_partitions)  # Limitar número de buckets
        
        # Atualizar processo com a entidade específica
        self.process_info["entity_name"] = entity_name
        self.process_info["process_name"] = f"SilverToGold_{entity_name}"
        self.process_info["total_records_processed"] = df.count()
        
        try:
            # Adicionar colunas de auditoria final
            df_with_audit = df.withColumns({
                "dt_processamento": lit(self.formatted_datetime),
                "dt_atualizacao": lit(self.formatted_datetime),
                "dt_snapshot": lit(self.data_referencia.strftime("%Y-%m-%d"))
            })
            
            # Salva como tabela Iceberg particionada e com bucketing
            if self.enable_bucketing and self.bucket_columns:
                # Com bucketing para otimizar consultas
                writer = df_with_audit.writeTo(f"{self.gold_db}.{table_name}")
                
                if partition_by:
                    writer = writer.partitionBy(*partition_by)
                
                buckets = [col for col in self.bucket_columns if col in df.columns]
                if buckets:
                    writer = writer.bucketBy(num_buckets, buckets[0])
                
                writer.tableProperty("write.format.default", "parquet") \
                      .tableProperty("write.metadata.compression-codec", "snappy") \
                      .tableProperty("write.distribution-mode", "hash") \
                      .createOrReplace()
                
                self.logger.info(f"Tabela {table_name} criada com sucesso no formato Iceberg (com bucketing)")
                
            else:
                # Escrita sem bucketing
                writer = df_with_audit.writeTo(f"{self.gold_db}.{table_name}")
                
                if partition_by:
                    writer = writer.partitionBy(*partition_by)
                
                writer.tableProperty("write.format.default", "parquet") \
                      .tableProperty("write.metadata.compression-codec", "snappy") \
                      .tableProperty("write.distribution-mode", "hash") \
                      .createOrReplace()
                
                self.logger.info(f"Tabela {table_name} criada com sucesso no formato Iceberg")
                
            # Também salvar no formato Parquet para compatibilidade
            df_with_audit.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(f"{self.gold_path}/{table_name}")
            
            self.logger.info(f"Backup em Parquet salvo em: {self.gold_path}/{table_name}")
            
        except Exception as e:
            error_message = str(e)
            self.logger.error(f"Erro ao salvar na camada Gold: {error_message}")
            
            # Registrar erro
            self.create_error_record(
                record={"error": error_message},
                error_type="WRITING_ERROR",
                error_message=f"Erro ao salvar na camada Gold: {error_message}",
                entity_name=entity_name
            )
            
            # Tenta salvar no formato Parquet como fallback
            try:
                self.logger.info("Tentando salvar em formato Parquet como fallback")
                df_with_audit = df.withColumns({
                    "dt_processamento": lit(self.formatted_datetime),
                    "dt_atualizacao": lit(self.formatted_datetime),
                    "dt_snapshot": lit(self.data_referencia.strftime("%Y-%m-%d"))
                })
                
                writer = df_with_audit.write.mode("overwrite").option("compression", "snappy")
                
                if partition_by:
                    writer = writer.partitionBy(*partition_by)
                
                writer.parquet(f"{self.gold_path}/{table_name}")
                
                self.logger.info(f"Dados salvos como Parquet em: {self.gold_path}/{table_name}")
            except Exception as fallback_error:
                self.logger.error(f"Erro também no fallback para Parquet: {str(fallback_error)}")
        finally:
            # Registrar final da escrita
            self.process_info["data_writing_end"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Salvar log do processo para esta entidade
            self.save_process_log(entity_name)
    
    def process(self) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """
        Executa o processamento completo Silver -> Gold.
        
        Returns:
            Tuple contendo (DataFrame de inadimplência, DataFrame de métricas, DataFrame de alertas)
        """
        try:
            # Carrega dados da camada Silver
            df_transactions = self.load_transactions()
            df_payments = self.load_payments()
            
            # Aplica regras de negócio
            df_inadimplencia = self.aplicar_regras_inadimplencia(
                df_transactions, 
                df_payments
            )
            
            # Cria métricas
            df_metricas = self.criar_metricas_inadimplencia(df_inadimplencia)
            
            # Gera alertas
            df_alertas = self.criar_alertas_inadimplencia(df_inadimplencia)
            
            # Salva resultados na camada Gold
            self.save_to_gold(
                df_inadimplencia, 
                "inadimplencia_clientes", 
                partition_by=["inadimplente", "reference_month"]
            )
            
            self.save_to_gold(
                df_metricas, 
                "inadimplencia_metricas", 
                partition_by=["reference_month"]
            )
            
            self.save_to_gold(
                df_alertas, 
                "inadimplencia_alertas"
            )
            
            # Salvar registros de erro, se houver
            if self.error_records:
                self.save_error_records()
            
            # Logar métricas finais
            inadimplentes_count = df_inadimplencia.filter(col("inadimplente")).count()
            total_clientes = df_inadimplencia.count()
            self.logger.info(f"Processamento concluído: {inadimplentes_count} clientes inadimplentes de {total_clientes}")
            
            return df_inadimplencia, df_metricas, df_alertas
        
        except Exception as e:
            error_message = str(e)
            self.logger.error(f"Erro no processamento Silver -> Gold: {error_message}", exc_info=True)
            self.process_info["status"] = "ERRO"
            self.process_info["error_message"] = error_message
            
            # Registrar erro
            self.create_error_record(
                record={"process_error": error_message},
                error_type="PROCESS_FAILURE",
                error_message=f"Erro no processamento: {error_message}",
                entity_name="all_entities"
            )
            
            # Salvar registros de erro
            self.save_error_records()
            
            # Salvar log do processo
            self.save_process_log("process_failure")
            
            raise
        finally:
            self.process_info["end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.process_info["status"] = self.process_info["status"] or "SUCESSO"
            self.save_process_log()


def parse_args():
    """Parse argumentos de linha de comando."""
    parser = argparse.ArgumentParser(description="Processamento Silver para Gold")
    parser.add_argument(
        "--silver-path",
        type=str,
        default="gs://saudebliss-datalake/silver",
        help="Caminho para os dados na camada Silver"
    )
    parser.add_argument(
        "--gold-path",
        type=str,
        default="gs://saudebliss-datalake/gold",
        help="Caminho para escrita na camada Gold"
    )
    parser.add_argument(
        "--gold-db",
        type=str,
        default="gold",
        help="Nome do banco de dados Gold"
    )
    parser.add_argument(
        "--execution-date",
        type=str,
        help="Data de execução no formato YYYY-MM-DD (opcional)"
    )
    parser.add_argument(
        "--num-partitions",
        type=int,
        default=8,
        help="Número de partições para os DataFrames"
    )
    parser.add_argument(
        "--enable-bucketing",
        action="store_true",
        help="Habilita bucketing para otimização"
    )
    parser.add_argument(
        "--bucket-name",
        type=str,
        default="saudebliss-datalake",
        help="Nome do bucket de armazenamento"
    )
    return parser.parse_args()


def main():
    """Função principal do script."""
    # Configura logs
    configure_logging()
    logger = logging.getLogger(__name__)
    logger.info("Iniciando processamento Silver -> Gold")
    
    # Parse argumentos
    args = parse_args()
    
    try:
        # Cria sessão Spark com configurações otimizadas
        spark = create_spark_session(app_name="silver_to_gold")
        
        # Configurações adicionais para otimização de desempenho
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        spark.conf.set("spark.sql.shuffle.partitions", str(args.num_partitions))
        
        # Executa processamento
        processor = SilverToGoldProcessor(
            spark=spark,
            silver_path=args.silver_path,
            gold_path=args.gold_path,
            gold_db=args.gold_db,
            num_partitions=args.num_partitions,
            enable_bucketing=args.enable_bucketing,
            bucket_name=args.bucket_name,
            execution_date=args.execution_date
        )
        
        processor.process()
        
        logger.info("Processamento Silver -> Gold concluído com sucesso")
        
    except Exception as e:
        logger.error(f"Erro no processamento: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()