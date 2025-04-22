#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Módulo para configuração de logging padronizado.

Este módulo fornece funções para configurar logs consistentes
em todo o pipeline de dados.
"""

import logging
import os
import sys
from datetime import datetime


def configure_logging(log_level=logging.INFO, output_dir=None):
    """
    Configura o sistema de logging com formatação padronizada.
    
    Args:
        log_level: Nível de log (default: INFO)
        output_dir: Diretório para arquivo de log (opcional)
    """
    # Formatar para incluir timestamp, nível e módulo
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format)
    
    # Configurar log para console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # Configurar root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Limpar handlers existentes para evitar duplicação
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
        
    root_logger.addHandler(console_handler)
    
    # Configurar arquivo de log, se especificado
    if output_dir:
        # Garantir que o diretório exista
        os.makedirs(output_dir, exist_ok=True)
        
        # Nome do arquivo com timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = os.path.join(output_dir, f'pipeline_{timestamp}.log')
        
        # Adicionar handler de arquivo
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
        
        root_logger.info(f"Log configurado para arquivo: {log_file}")
    
    # Log da configuração completa
    root_logger.info("Configuração de logging concluída")
    
    return root_logger


def get_logger(name, log_level=None):
    """
    Retorna um logger configurado para um módulo específico.
    
    Args:
        name: Nome do módulo/componente
        log_level: Nível de log específico (opcional)
        
    Returns:
        Logger configurado
    """
    logger = logging.getLogger(name)
    
    if log_level:
        logger.setLevel(log_level)
        
    return logger