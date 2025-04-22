#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Processamento de streaming para análise de inadimplência em tempo real.

Este script implementa um job de Spark Structured Streaming para processar
dados de transações e pagamentos em tempo real, aplicando as mesmas regras
de inadimplência utilizadas no processamento em batch.

Autores: Time de Engenharia de Dados SaudeBliss
Data: Abril/2025
"""

import os
import argparse
import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, BooleanType, DateType
)
from pyspark.sql.window import Window

from utils.spark_session import get_spark_session
from utils.logging_config import configure_logging
from utils.metrics import log_streaming_metrics

# Configuração de logging
logger = logging.getLogger(__name__)
configure_logging()

def parse_args():
    """Configura os argumentos de linha de comando para o script."""
    parser = argparse.ArgumentParser(description='Pipeline de streaming para análise de inadimplência')
    
    parser.add_argument('--checkpoint-location', type=str, 
                        default='gs://saudebliss-checkpoints/streaming-inadimplencia',
                        help='Localização do checkpoint para o streaming')
    parser.add_argument('--transactions-topic', type=str, 
                        default='transactions',
                        help='Nome do tópico Kafka para transações')
    parser.add_argument('--payments-topic', type=str, 
                        default='payments',
                        help='Nome do tópico Kafka para pagamentos')
    parser.add_argument('--bootstrap-servers', type=str, 
                        default='kafka:9092',
                        help='Lista de brokers Kafka')
    parser.add_argument('--output-path', type=str, 
                        default='gs://saudebliss-gold/streaming',
                        help='Caminho para saída dos dados')
    parser.add_argument('--trigger-interval', type=str, 
                        default='1 minute',
                        help='Intervalo de processamento do streaming')
    parser.add_argument('--num-partitions', type=int,
                        default=8,
                        help='Número de partições para reparticionamento interno')
    parser.add_argument('--watermark-delay', type=str,
                        default='2 hours',
                        help='Tempo de atraso para watermark')
    parser.add_argument('--enable-state-cleanup', action='store_true',
                        help='Habilitar limpeza automática de estado')
                        
    return parser.parse_args()

def define_schemas():
    """Define os schemas para os dados de entrada de streaming."""
    
    # Schema para transações - baseado nos dados observados em transactions.json
    transaction_schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("date", StringType(), False)
    ])
    
    # Schema para pagamentos - baseado nos dados observados em payments.json
    payment_schema = StructType([
        StructField("payment_id", StringType(), False),
        StructField("transaction_id", StringType(), False),
        StructField("amount_paid", DoubleType(), False),
        StructField("date_paid", StringType(), False)
    ])
    
    return transaction_schema, payment_schema

def read_transaction_stream(spark, args, transaction_schema):
    """
    Lê o stream de transações do Kafka ou de arquivos.
    
    Para fins de demonstração e desenvolvimento, também oferece 
    opção de ler de arquivos para emular streaming.
    """
    # Se estamos em modo de emulação, lê de arquivos
    if args.bootstrap_servers.startswith("file:"):
        transactions_stream = (spark
            .readStream
            .format("json")
            .schema(transaction_schema)
            .option("maxFilesPerTrigger", 1)  # Simula streaming lendo 1 arquivo por vez
            .load(args.bootstrap_servers.replace("file:", "")))
        
        # Transformações iniciais conforme os dados de origem
        transactions_stream = (transactions_stream
            .withColumn("transaction_date", F.to_timestamp(F.col("date")))
            .withColumn("due_date", F.date_add(F.to_date(F.col("date")), 30))  # Assumindo vencimento em 30 dias
            .withColumn("value", F.col("amount"))
            # Adicionar colunas para particionamento
            .withColumn("year", F.year(F.col("transaction_date")))
            .withColumn("month", F.month(F.col("transaction_date")))
            .withColumn("day", F.dayofmonth(F.col("transaction_date")))
            .withColumn("processing_time", F.current_timestamp())
            .drop("date", "amount")
        )
    else:
        # Lê do Kafka em produção
        transactions_stream = (spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.transactions_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")  # Para ser mais resiliente em caso de apagamento de dados
            .option("maxOffsetsPerTrigger", 10000)  # Limite de registros por microbatch para controle de fluxo
            .load()
            # Parsing do valor em formato JSON
            .selectExpr("CAST(value AS STRING) as json", "timestamp as kafka_timestamp")
            .select(F.from_json("json", transaction_schema).alias("data"), "kafka_timestamp")
            .select("data.*", "kafka_timestamp")
            
            # Transformações semelhantes às do modo arquivo
            .withColumn("transaction_date", F.to_timestamp(F.col("date")))
            .withColumn("due_date", F.date_add(F.to_date(F.col("date")), 30))
            .withColumn("value", F.col("amount"))
            .withColumn("year", F.year(F.col("transaction_date")))
            .withColumn("month", F.month(F.col("transaction_date")))
            .withColumn("day", F.dayofmonth(F.col("transaction_date")))
            .withColumn("processing_time", F.col("kafka_timestamp"))
            .drop("date", "amount", "kafka_timestamp")
        )
    
    # Reparticionamento para melhor distribuição de dados
    transactions_stream = transactions_stream.repartition(args.num_partitions)
    
    return transactions_stream

def read_payment_stream(spark, args, payment_schema):
    """
    Lê o stream de pagamentos do Kafka ou de arquivos.
    
    Para fins de demonstração e desenvolvimento, também oferece 
    opção de ler de arquivos para emular streaming.
    """
    # Se estamos em modo de emulação, lê de arquivos
    if args.bootstrap_servers.startswith("file:"):
        payments_stream = (spark
            .readStream
            .format("json")
            .schema(payment_schema)
            .option("maxFilesPerTrigger", 1)  # Simula streaming lendo 1 arquivo por vez
            .load(args.bootstrap_servers.replace("file:", "")))
        
        # Transformações iniciais conforme os dados de origem
        payments_stream = (payments_stream
            .withColumn("payment_date", F.to_timestamp(F.col("date_paid")))
            .withColumn("payment_amount", F.col("amount_paid"))
            # Adicionar colunas para particionamento
            .withColumn("year", F.year(F.col("payment_date")))
            .withColumn("month", F.month(F.col("payment_date")))
            .withColumn("day", F.dayofmonth(F.col("payment_date")))
            .withColumn("processing_time", F.current_timestamp())
            .drop("date_paid", "amount_paid")
            .withColumn("payment_method", F.lit("unknown"))  # Valor padrão
            .withColumn("status", F.lit("completed"))  # Valor padrão
        )
    else:
        # Lê do Kafka em produção
        payments_stream = (spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.payments_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")  # Para ser mais resiliente em caso de apagamento de dados
            .option("maxOffsetsPerTrigger", 10000)  # Limite de registros por microbatch para controle de fluxo
            .load()
            # Parsing do valor em formato JSON
            .selectExpr("CAST(value AS STRING) as json", "timestamp as kafka_timestamp")
            .select(F.from_json("json", payment_schema).alias("data"), "kafka_timestamp")
            .select("data.*", "kafka_timestamp")
            
            # Transformações semelhantes às do modo arquivo
            .withColumn("payment_date", F.to_timestamp(F.col("date_paid")))
            .withColumn("payment_amount", F.col("amount_paid"))
            .withColumn("year", F.year(F.col("payment_date")))
            .withColumn("month", F.month(F.col("payment_date")))
            .withColumn("day", F.dayofmonth(F.col("payment_date")))
            .withColumn("processing_time", F.col("kafka_timestamp"))
            .drop("date_paid", "amount_paid", "kafka_timestamp")
            .withColumn("payment_method", F.lit("unknown"))  # Valor padrão
            .withColumn("status", F.lit("completed"))  # Valor padrão
        )
    
    # Reparticionamento para melhor distribuição de dados
    payments_stream = payments_stream.repartition(args.num_partitions)
    
    return payments_stream

def process_streaming_data(transactions_df, payments_df, args):
    """
    Processa os dados de streaming para análise de inadimplência.
    
    Esta função aplica uma lógica similar à do processamento batch,
    mas adaptada para o contexto de streaming com uso de watermarks
    para gerenciamento de estado.
    """
    # Definir watermarks para limitar o crescimento de estado
    # Este é um ponto crítico para escalabilidade do streaming
    watermark_delay = args.watermark_delay
    
    transactions_with_watermark = transactions_df \
        .withWatermark("transaction_date", watermark_delay)
    
    payments_with_watermark = payments_df \
        .withWatermark("payment_date", watermark_delay)
    
    # Calcular data de referência (atual)
    current_date = F.current_timestamp()
    
    # Calcular datas limite para os períodos de análise
    date_6_months_ago = F.date_sub(current_date, 180)
    date_3_months_ago = F.date_sub(current_date, 90)
    
    # Join entre transações e pagamentos usando chave transaction_id
    # e respeitando as watermarks para evitar crescimento indefinido de estado
    joined_df = transactions_with_watermark \
        .join(
            payments_with_watermark,
            "transaction_id",
            "left_outer"
        )
    
    # Calcular dias de atraso
    processed_df = joined_df \
        .withColumn(
            "payment_done", 
            F.when(
                (F.col("payment_date").isNotNull()) & 
                (F.col("status") == "completed"), 
                True
            ).otherwise(False)
        ) \
        .withColumn(
            "days_late",
            F.when(
                F.col("payment_done"),
                F.datediff(F.col("payment_date"), F.col("due_date"))
            ).otherwise(
                F.datediff(current_date, F.col("due_date"))
            )
        ) \
        .withColumn(
            "days_late",
            F.when(F.col("days_late") < 0, 0).otherwise(F.col("days_late"))
        )
    
    # Aplicar regras de inadimplência
    default_df = processed_df \
        .withColumn(
            "criterio_1",
            F.when(
                (F.col("transaction_date") >= date_3_months_ago) & 
                (F.col("days_late") > 30),
                True
            ).otherwise(False)
        ) \
        .withColumn(
            "criterio_2",
            F.when(
                (F.col("transaction_date") >= date_6_months_ago) & 
                (F.col("days_late") > 15),
                True
            ).otherwise(False)
        ) \
        .withColumn(
            "inadimplente",
            F.when(
                F.col("criterio_1") | F.col("criterio_2"),
                True
            ).otherwise(False)
        ) \
        .withColumn(
            "reference_date",
            F.date_format(current_date, "yyyy-MM-dd")
        ) \
        .withColumn(
            "reference_month",
            F.date_format(current_date, "yyyy-MM")
        ) \
        .withColumn(
            "reference_quarter",
            F.concat(
                F.year(current_date),
                F.lit("-Q"),
                F.quarter(current_date)
            )
        ) \
        .withColumn(
            "processed_timestamp", 
            F.current_timestamp()
        )
    
    return default_df

def start_streaming_job(spark, args):
    """
    Inicia o job de streaming para processamento contínuo de dados.
    """
    # Definir schemas
    transaction_schema, payment_schema = define_schemas()
    
    # Ler streams de entrada
    transactions_stream = read_transaction_stream(spark, args, transaction_schema)
    payments_stream = read_payment_stream(spark, args, payment_schema)
    
    # Processar dados de streaming
    processed_stream = process_streaming_data(transactions_stream, payments_stream, args)
    
    # Definir colunas de particionamento baseado nas análises de dados
    # Isso melhora desempenho de consulta e escrita
    partition_columns = ["reference_month", "year", "month", "inadimplente"]
    
    # Criar consulta de streaming para salvar dados detalhados de inadimplência
    # Particionando adequadamente para facilitar consultas posteriores
    streaming_query_detalhes = processed_stream \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", f"{args.output_path}/inadimplencia_detalhes") \
        .option("checkpointLocation", f"{args.checkpoint_location}/inadimplencia_detalhes") \
        .partitionBy(*partition_columns) \
        .trigger(processingTime=args.trigger_interval) \
        .start()
    
    # Criar stream agregado com métricas de inadimplência
    # Agrupamos por período para ter visões históricas
    metricas_stream = processed_stream \
        .withWatermark("processed_timestamp", "10 minutes") \
        .groupBy(
            F.window(F.col("processed_timestamp"), "10 minutes"),
            F.col("reference_month"),
            F.col("reference_quarter")
        ) \
        .agg(
            F.count("transaction_id").alias("total_transactions"),
            F.sum(F.when(F.col("inadimplente") == True, 1).otherwise(0)).alias("total_inadimplentes"),
            F.sum(F.when(F.col("days_late") > 0, 1).otherwise(0)).alias("total_atrasados"),
            F.avg("days_late").alias("media_dias_atraso"),
            F.max("days_late").alias("max_dias_atraso"),
            F.min(F.when(F.col("days_late") > 0, F.col("days_late")).otherwise(None)).alias("min_dias_atraso_positivo")
        ) \
        .withColumn(
            "percentual_inadimplentes",
            (F.col("total_inadimplentes") / F.col("total_transactions")) * 100
        )
    
    # Salvar stream de métricas em formato compactado
    streaming_query_metricas = metricas_stream \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", f"{args.output_path}/inadimplencia_metricas") \
        .option("checkpointLocation", f"{args.checkpoint_location}/inadimplencia_metricas") \
        .partitionBy("reference_month", "reference_quarter") \
        .trigger(processingTime=args.trigger_interval) \
        .start()
    
    # Criar um fluxo de alertas para inadimplência acima de um limite
    alertas_stream = metricas_stream \
        .filter(F.col("percentual_inadimplentes") > 20) \
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "reference_month",
            "reference_quarter",
            "total_transactions",
            "total_inadimplentes",
            "percentual_inadimplentes"
        )
    
    # Salvar alertas para monitoramento
    streaming_query_alertas = alertas_stream \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", f"{args.output_path}/inadimplencia_alertas") \
        .option("checkpointLocation", f"{args.checkpoint_location}/inadimplencia_alertas") \
        .trigger(processingTime=args.trigger_interval) \
        .foreachBatch(lambda df, batch_id: process_alertas(df, batch_id)) \
        .start()
    
    # Exibir progresso do streaming no console
    logger.info("Streaming queries iniciadas. Aguardando término...")
    
    # Aguardar término do streaming (interrupção manual)
    spark.streams.awaitAnyTermination()

def process_alertas(df, batch_id):
    """
    Processa alertas de inadimplência.
    Esta função poderia enviar notificações, acionar workflows, etc.
    """
    if df.count() > 0:
        logger.warning(f"ALERTA DE INADIMPLÊNCIA (Batch {batch_id}): Percentual acima do limite!")
        df.show(truncate=False)
        
        # Aqui poderíamos acionar notificações, webhooks, etc.
        # Por exemplo:
        # send_notification(f"Alerta de inadimplência: {df.collect()[0]['percentual_inadimplentes']}% inadimplentes")

def write_batch_with_metrics(df, batch_id, output_path):
    """
    Escreve um batch de dados e registra métricas.
    Esta função é chamada por cada batch do streaming.
    """
    # Processar e registrar métricas
    batch_time = datetime.now()
    record_count = df.count()
    
    # Calcular métricas de inadimplência
    metrics = df.groupBy("inadimplente").count().collect()
    
    # Formatar métricas
    default_count = 0
    non_default_count = 0
    
    for row in metrics:
        if row["inadimplente"]:
            default_count = row["count"]
        else:
            non_default_count = row["count"]
    
    total_count = default_count + non_default_count
    default_percentage = (default_count / total_count) * 100 if total_count > 0 else 0
    
    # Registrar métricas
    log_streaming_metrics(
        timestamp=batch_time,
        batch_id=batch_id,
        records_processed=record_count,
        defaulters_count=default_count,
        defaulters_percentage=default_percentage
    )
    
    logger.info(f"Batch {batch_id}: Processados {record_count} registros, {default_percentage:.2f}% inadimplentes")
    
    # Salvar resultados agregados para o batch atual
    if record_count > 0:
        # Criar DF de métricas para este batch
        metrics_data = [
            (batch_id, batch_time.isoformat(), record_count, default_count, non_default_count, default_percentage)
        ]
        
        metrics_schema = ["batch_id", "timestamp", "total_records", "default_count", "non_default_count", "default_percentage"]
        metrics_df = df.sparkSession.createDataFrame(metrics_data, metrics_schema)
        
        # Particionar pelos mês e dia para facilitar consultas históricas
        batch_year = batch_time.year
        batch_month = batch_time.month
        batch_day = batch_time.day
        
        # Salvar métricas particionadas
        metrics_df.withColumn("year", F.lit(batch_year)) \
                 .withColumn("month", F.lit(batch_month)) \
                 .withColumn("day", F.lit(batch_day)) \
                 .write \
                 .partitionBy("year", "month", "day") \
                 .format("parquet") \
                 .mode("append") \
                 .save(f"{output_path}/metrics")
        
        # Salvar também detalhes dos inadimplentes
        df.filter(F.col("inadimplente") == True) \
          .withColumn("year", F.lit(batch_year)) \
          .withColumn("month", F.lit(batch_month)) \
          .withColumn("day", F.lit(batch_day)) \
          .write \
          .partitionBy("year", "month", "day") \
          .format("parquet") \
          .mode("append") \
          .save(f"{output_path}/defaulters_details")

def main():
    """Função principal para iniciar o job de streaming."""
    # Parse argumentos
    args = parse_args()
    
    try:
        # Criar sessão Spark com configurações otimizadas para streaming
        spark = get_spark_session("SaudeBliss - Streaming Inadimplência")
        
        # Configurar o Spark para streaming
        spark.conf.set("spark.sql.streaming.checkpointLocation", args.checkpoint_location)
        spark.conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
        
        # Otimizações para streaming
        spark.conf.set("spark.sql.shuffle.partitions", str(args.num_partitions))
        spark.conf.set("spark.default.parallelism", str(args.num_partitions * 2))
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        # Configurações para Streaming estável com optimização de memória
        spark.conf.set("spark.sql.streaming.stateStore.providerClass", 
                      "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        spark.conf.set("spark.sql.streaming.stateStore.minDeltasForSnapshot", "10")
        spark.conf.set("spark.sql.streaming.aggregation.stateFormatVersion", "2")
        
        # Habilitar limpeza automática de estado se solicitado
        if args.enable_state_cleanup:
            spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
        
        logger.info("Iniciando o job de streaming para análise de inadimplência")
        logger.info(f"Usando tópicos: {args.transactions_topic} e {args.payments_topic}")
        logger.info(f"Watermark delay: {args.watermark_delay}")
        logger.info(f"Trigger interval: {args.trigger_interval}")
        logger.info(f"Reparticionamento: {args.num_partitions} partições")
        
        # Iniciar job de streaming
        start_streaming_job(spark, args)
        
    except Exception as e:
        logger.error(f"Erro no job de streaming: {str(e)}")
        raise
    
    finally:
        # Finalizar sessão Spark
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()