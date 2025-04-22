#!/bin/bash
set -e

# Configurações padrão
DEFAULT_SCRIPT="bronze_to_silver"

# Diretório do aplicativo
APP_DIR="/app"

# Parâmetros para scripts específicos
BRONZE_TO_SILVER_PARAMS=""
SILVER_TO_GOLD_PARAMS=""

# Função de ajuda
show_help() {
    echo "Uso: $0 [bronze_to_silver|silver_to_gold] [opções]"
    echo ""
    echo "Scripts disponíveis:"
    echo "  bronze_to_silver: Processa dados da camada Bronze para Silver"
    echo "  silver_to_gold: Aplica regras de negócio e processa dados para camada Gold"
    echo ""
    echo "Opções para bronze_to_silver:"
    echo "  --bronze-path PATH : Caminho para os dados na camada Bronze (padrão: gs://bronze-bucket)"
    echo "  --silver-path PATH : Caminho para escrita na camada Silver (padrão: gs://silver-bucket)"
    echo "  --silver-db NAME   : Nome do banco de dados Silver (padrão: silver)"
    echo ""
    echo "Opções para silver_to_gold:"
    echo "  --silver-path PATH : Caminho para os dados na camada Silver (padrão: gs://silver-bucket)"
    echo "  --gold-path PATH   : Caminho para escrita na camada Gold (padrão: gs://gold-bucket)"
    echo "  --gold-db NAME     : Nome do banco de dados Gold (padrão: gold)"
    echo ""
}

# Processar argumentos
SCRIPT=${1:-$DEFAULT_SCRIPT}
shift || true

# Verificar se script é válido
if [ "$SCRIPT" != "bronze_to_silver" ] && [ "$SCRIPT" != "silver_to_gold" ]; then
    if [ "$SCRIPT" == "--help" ] || [ "$SCRIPT" == "-h" ]; then
        show_help
        exit 0
    else
        echo "Script não reconhecido: $SCRIPT"
        show_help
        exit 1
    fi
fi

echo "Executando script: $SCRIPT"

# Configuração do Spark com valores padrão
SPARK_CONF=(
    "--conf" "spark.sql.adaptive.enabled=true"
    "--conf" "spark.dynamicAllocation.enabled=true" 
    "--conf" "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--conf" "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog"
    "--conf" "spark.sql.catalog.spark_catalog.type=hive"
    "--conf" "spark.sql.legacy.createHiveTableByDefault=false"
    "--conf" "spark.sql.sources.partitionOverwriteMode=dynamic"
)

# Adicionar JARs necessários ao classpath
if [ -n "$JARS_PATH" ]; then
    SPARK_CONF+=(
        "--jars" "$JARS_PATH"
    )
fi

# Preparar argumentos específicos do script
case $SCRIPT in
    bronze_to_silver)
        SPARK_SCRIPT="$APP_DIR/bronze_to_silver.py"
        ARGS="$BRONZE_TO_SILVER_PARAMS $@"
        ;;
    silver_to_gold)
        SPARK_SCRIPT="$APP_DIR/silver_to_gold.py"
        ARGS="$SILVER_TO_GOLD_PARAMS $@"
        ;;
esac

# Executar o script com os argumentos
echo "Executando: python $SPARK_SCRIPT $ARGS"
cd $APP_DIR
exec python $SPARK_SCRIPT $ARGS