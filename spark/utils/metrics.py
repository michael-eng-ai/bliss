#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Módulo para registro e exposição de métricas do pipeline.

Este módulo fornece funções para registrar métricas de processamento
e exportá-las para sistemas de monitoramento.
"""

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    from prometheus_client import Counter, Gauge, Histogram, Summary, push_to_gateway
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False


class MetricsCollector:
    """
    Coletor de métricas para o pipeline de dados.
    
    Registra métricas de performance, volume e qualidade dos dados
    processados pelo pipeline.
    """
    
    def __init__(self, job_name: str, push_gateway_url: Optional[str] = None):
        """
        Inicializa o coletor de métricas.
        
        Args:
            job_name: Nome do job para identificação
            push_gateway_url: URL do Prometheus Pushgateway (opcional)
        """
        self.job_name = job_name
        self.push_gateway_url = push_gateway_url
        self.logger = logging.getLogger(__name__)
        self.metrics = {}
        self.start_time = time.time()
        
        # Inicializa métricas se Prometheus estiver disponível
        if PROMETHEUS_AVAILABLE and push_gateway_url:
            self._setup_prometheus_metrics()
            
    def _setup_prometheus_metrics(self):
        """Configura métricas Prometheus."""
        # Contador de registros processados
        self.metrics["records_processed"] = Counter(
            "pipeline_records_processed_total",
            "Total de registros processados",
            ["component", "table"]
        )
        
        # Gauge para tamanho dos dados
        self.metrics["data_size"] = Gauge(
            "pipeline_data_size_bytes",
            "Tamanho dos dados processados em bytes",
            ["component", "table"]
        )
        
        # Histograma para tempo de processamento
        self.metrics["processing_time"] = Histogram(
            "pipeline_processing_time_seconds",
            "Tempo de processamento em segundos",
            ["component", "operation"]
        )
        
        # Gauge para qualidade de dados
        self.metrics["data_quality"] = Gauge(
            "pipeline_data_quality_score",
            "Pontuação de qualidade dos dados",
            ["component", "check_name"]
        )
        
    def record_count(self, component: str, table: str, count: int):
        """
        Registra contagem de registros processados.
        
        Args:
            component: Nome do componente (ex: 'bronze_to_silver')
            table: Nome da tabela processada
            count: Número de registros
        """
        self.logger.info(f"[Métrica] Contagem em {component}.{table}: {count} registros")
        
        if PROMETHEUS_AVAILABLE and "records_processed" in self.metrics:
            self.metrics["records_processed"].labels(component=component, table=table).inc(count)
            
    def record_processing_time(self, component: str, operation: str, seconds: float):
        """
        Registra tempo de processamento.
        
        Args:
            component: Nome do componente
            operation: Nome da operação
            seconds: Tempo em segundos
        """
        self.logger.info(f"[Métrica] Tempo de {component}.{operation}: {seconds:.2f}s")
        
        if PROMETHEUS_AVAILABLE and "processing_time" in self.metrics:
            self.metrics["processing_time"].labels(
                component=component, 
                operation=operation
            ).observe(seconds)
            
    def record_data_quality(self, component: str, check_name: str, score: float):
        """
        Registra pontuação de qualidade dos dados.
        
        Args:
            component: Nome do componente
            check_name: Nome da verificação
            score: Pontuação (0.0 a 1.0)
        """
        self.logger.info(f"[Métrica] Qualidade em {component}.{check_name}: {score:.4f}")
        
        if PROMETHEUS_AVAILABLE and "data_quality" in self.metrics:
            self.metrics["data_quality"].labels(
                component=component, 
                check_name=check_name
            ).set(score)
            
    def export_metrics(self) -> Dict[str, Any]:
        """
        Exporta métricas coletadas em formato JSON.
        
        Returns:
            Dicionário com métricas
        """
        # Calcula tempo total de execução
        execution_time = time.time() - self.start_time
        
        metrics_data = {
            "job_name": self.job_name,
            "timestamp": datetime.now().isoformat(),
            "execution_time_seconds": execution_time,
            # Adicionar outras métricas conforme necessário
        }
        
        self.logger.info(f"Métricas exportadas: {json.dumps(metrics_data)}")
        return metrics_data
        
    def push_to_prometheus(self):
        """Envia métricas para Prometheus Pushgateway."""
        if not PROMETHEUS_AVAILABLE or not self.push_gateway_url:
            self.logger.warning("Prometheus não configurado, ignorando push de métricas")
            return False
        
        try:
            push_to_gateway(
                self.push_gateway_url,
                job=self.job_name,
                registry=None  # Usa o registry default
            )
            self.logger.info(f"Métricas enviadas para Prometheus: {self.push_gateway_url}")
            return True
        except Exception as e:
            self.logger.error(f"Erro ao enviar métricas para Prometheus: {str(e)}")
            return False


def log_data_metrics(logger, name, count, partitions, quality_score=None):
    """
    Função auxiliar para log de métricas simples.
    
    Args:
        logger: Logger para registrar métricas
        name: Nome do componente/tabela
        count: Contagem de registros
        partitions: Número de partições
        quality_score: Pontuação de qualidade (opcional)
    """
    logger.info(f"===== Métricas: {name} =====")
    logger.info(f"Registros processados: {count}")
    logger.info(f"Partições: {partitions}")
    
    if quality_score is not None:
        logger.info(f"Qualidade dos dados: {quality_score:.2f}%")
        
    logger.info("="*30)