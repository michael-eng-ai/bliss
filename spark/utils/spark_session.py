#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Módulo para criar e configurar sessões Spark padrão.

Este módulo fornece funções para criar sessões Spark com
configurações otimizadas para o pipeline de inadimplência.
"""

from pyspark.sql import SparkSession
import logging


def create_spark_session(app_name="Data Pipeline", config_dict=None):
    """
    Cria uma sessão Spark com configurações otimizadas.
    
    Args:
        app_name: Nome da aplicação Spark
        config_dict: Dicionário com configurações adicionais
        
    Returns:
        SparkSession configurada
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Criando sessão Spark para aplicação: {app_name}")
    
    # Inicializa o builder da sessão
    builder = (
        SparkSession.builder
        .appName(app_name)
        # Configurações de performance
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.shuffle.service.enabled", "true")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        # Suporte para mascaramento de dados sensíveis (LGPD)
        .config("spark.sql.redaction.options.enabled", "true")
        .config("spark.sql.redaction.string.regex", "(\\w*customer_id\\w*|\\w*cpf\\w*|\\w*email\\w*)")
    )
    
    # Adiciona configurações personalizadas
    if config_dict:
        for key, value in config_dict.items():
            builder = builder.config(key, value)
            
    # Cria a sessão
    spark = builder.getOrCreate()
    
    # Configura log level
    spark.sparkContext.setLogLevel("WARN")
    
    # Log das configurações
    logger.info("Sessão Spark criada com sucesso")
    logger.debug(f"Configurações: {spark.sparkContext.getConf().getAll()}")
    
    return spark