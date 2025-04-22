# Dockerfile para imagem de processamento Spark
FROM apache/spark-py:3.3.2

USER root

# Configurar variáveis de ambiente
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:${PYTHONPATH}" \
    PIP_NO_CACHE_DIR=1

# Instalar dependências do sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libssl-dev \
    libffi-dev \
    git \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copiar os requisitos primeiro para aproveitar o cache
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copiar o código do aplicativo
COPY scripts/ /app/scripts/
COPY utils/ /app/utils/
COPY bronze_to_silver.py silver_to_gold.py /app/

# Tornar scripts executáveis
RUN chmod +x /app/scripts/*.sh

# Script de entrypoint
COPY scripts/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Definir o entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

# Comando padrão (pode ser substituído no runtime)
CMD ["bronze_to_silver"]