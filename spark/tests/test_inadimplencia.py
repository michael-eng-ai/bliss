#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Testes unitários para as regras de inadimplência.

Este módulo contém testes para validar a implementação das regras
de inadimplência no pipeline de dados.
"""

import unittest
from datetime import datetime, timedelta
import os
import sys
import tempfile

# Adiciona o diretório raiz ao path para importar módulos
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importações específicas do Spark (com tratamento de erro para execução local)
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType
    from pyspark.sql.functions import col, to_date, lit
except ImportError:
    print("PySpark não disponível. Testes que dependem do Spark serão ignorados.")
    has_spark = False
else:
    has_spark = True

# Ignorar testes se o Spark não estiver disponível
@unittest.skipIf(not has_spark, "PySpark não disponível")
class TestInadimplenciaRules(unittest.TestCase):
    """Testes para regras de inadimplência."""
    
    @classmethod
    def setUpClass(cls):
        """
        Configuração inicial para os testes.
        Cria sessão Spark e prepara dados de teste.
        """
        cls.spark = (SparkSession.builder
                    .appName("TestInadimplenciaRules")
                    .master("local[1]")
                    .config("spark.sql.shuffle.partitions", "1")
                    .getOrCreate())
        
        # Criar schemas
        cls.transaction_schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("transaction_date", DateType(), False),
            StructField("value", DoubleType(), False),
            StructField("due_date", DateType(), False)
        ])
        
        cls.payment_schema = StructType([
            StructField("payment_id", StringType(), False),
            StructField("transaction_id", StringType(), False),
            StructField("payment_date", DateType(), True),
            StructField("value", DoubleType(), False)
        ])
        
        # Configurar dados de teste
        cls.setup_test_data()
    
    @classmethod
    def tearDownClass(cls):
        """Limpeza após os testes."""
        cls.spark.stop()
    
    @classmethod
    def setup_test_data(cls):
        """Cria dados de teste para transações e pagamentos."""
        # Data de referência (hoje)
        today = datetime.now().date()
        
        # Caso 1: Cliente inadimplente pelo critério 1 (atraso > 30 dias após 3 meses)
        # Transação de 4 meses atrás com pagamento 35 dias após vencimento
        transactions_data = [
            # Cliente 1 - Inadimplente critério 1
            ("tx001", "c001", today - timedelta(days=120), 100.0, today - timedelta(days=90)),  # Vencimento há 3 meses
            
            # Cliente 2 - Inadimplente critério 2
            ("tx002", "c002", today - timedelta(days=60), 200.0, today - timedelta(days=30)),  # Vencimento há 1 mês
            
            # Cliente 3 - Não inadimplente
            ("tx003", "c003", today - timedelta(days=60), 300.0, today - timedelta(days=30))  # Vencimento há 1 mês
        ]
        
        payments_data = [
            # Cliente 1 - Pagamento com 35 dias de atraso 
            ("p001", "tx001", today - timedelta(days=55), 100.0),  # Pagamento após 35 dias do vencimento
            
            # Cliente 2 - Pagamento com 16 dias de atraso
            ("p002", "tx002", today - timedelta(days=14), 200.0),  # Pagamento após 16 dias do vencimento
            
            # Cliente 3 - Pagamento em dia
            ("p003", "tx003", today - timedelta(days=30), 300.0)  # Pagamento no dia do vencimento
        ]
        
        # Criar DataFrames
        cls.df_transactions = cls.spark.createDataFrame(transactions_data, cls.transaction_schema)
        cls.df_payments = cls.spark.createDataFrame(payments_data, cls.payment_schema)
    
    def test_join_transactions_payments(self):
        """Testa a junção entre transações e pagamentos."""
        from silver_to_gold import InadimplenciaProcessor
        
        # Cria o processador
        processor = InadimplenciaProcessor(self.spark)
        
        # Executa a junção
        joined_df = processor.join_transactions_payments(self.df_transactions, self.df_payments)
        
        # Verifica se foram geradas as colunas esperadas
        self.assertTrue("delay_days" in joined_df.columns)
        self.assertEqual(joined_df.count(), 3)  # Devemos ter 3 registros
    
    def test_inadimplencia_criteria_1(self):
        """Testa o critério 1 de inadimplência: Atraso > 30 dias após 3 meses."""
        from silver_to_gold import InadimplenciaProcessor
        
        # Cria o processador
        processor = InadimplenciaProcessor(self.spark)
        
        # Executa o fluxo até a aplicação das regras de inadimplência
        joined_df = processor.join_transactions_payments(self.df_transactions, self.df_payments)
        result_df = processor.apply_inadimplencia_rules(joined_df)
        
        # Filtra o cliente que deve ser inadimplente pelo critério 1
        c1_df = result_df.filter(col("customer_id") == "c001")
        
        # Verifica se está inadimplente
        self.assertEqual(c1_df.first()["inadimplente"], True)
        self.assertGreaterEqual(c1_df.first()["late_30d_3m"], 1)
    
    def test_inadimplencia_criteria_2(self):
        """Testa o critério 2 de inadimplência: Atraso > 15 dias em 6 meses."""
        from silver_to_gold import InadimplenciaProcessor
        
        # Cria o processador
        processor = InadimplenciaProcessor(self.spark)
        
        # Executa o fluxo até a aplicação das regras de inadimplência
        joined_df = processor.join_transactions_payments(self.df_transactions, self.df_payments)
        result_df = processor.apply_inadimplencia_rules(joined_df)
        
        # Filtra o cliente que deve ser inadimplente pelo critério 2
        c2_df = result_df.filter(col("customer_id") == "c002")
        
        # Verifica se está inadimplente
        self.assertEqual(c2_df.first()["inadimplente"], True)
        self.assertGreaterEqual(c2_df.first()["late_15d_6m"], 1)
    
    def test_cliente_nao_inadimplente(self):
        """Testa que clientes sem atrasos não são marcados como inadimplentes."""
        from silver_to_gold import InadimplenciaProcessor
        
        # Cria o processador
        processor = InadimplenciaProcessor(self.spark)
        
        # Executa o fluxo até a aplicação das regras de inadimplência
        joined_df = processor.join_transactions_payments(self.df_transactions, self.df_payments)
        result_df = processor.apply_inadimplencia_rules(joined_df)
        
        # Filtra o cliente que não deve ser inadimplente
        c3_df = result_df.filter(col("customer_id") == "c003")
        
        # Verifica que não está inadimplente
        self.assertEqual(c3_df.first()["inadimplente"], False)
        self.assertEqual(c3_df.first()["late_30d_3m"], 0)
        self.assertEqual(c3_df.first()["late_15d_6m"], 0)
    
    def test_calculo_metricas(self):
        """Testa o cálculo de métricas de inadimplência."""
        from silver_to_gold import InadimplenciaProcessor
        
        # Cria o processador
        processor = InadimplenciaProcessor(self.spark)
        
        # Executa o fluxo até o cálculo de métricas
        joined_df = processor.join_transactions_payments(self.df_transactions, self.df_payments)
        result_df = processor.apply_inadimplencia_rules(joined_df)
        metrics_df = processor.calculate_metrics(result_df)
        
        # Obtém os resultados
        metrics_row = metrics_df.collect()[0]
        
        # Verifica as métricas
        self.assertEqual(metrics_row["total_clientes"], 3)
        self.assertEqual(metrics_row["clientes_inadimplentes"], 2)
        self.assertEqual(metrics_row["pct_inadimplencia"], 66.67)  # 2/3 = 66.67%


if __name__ == "__main__":
    unittest.main()