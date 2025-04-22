# Variáveis para ambiente de desenvolvimento
environment = "dev"
region     = "us-central1"

# Configuração dos buckets
storage_class  = "STANDARD"
force_destroy  = true

# Configuração do Cloud Composer (Airflow)
composer_image_version = "composer-2.0.31-airflow-2.3.4"
composer_node_count    = 3
composer_machine_type  = "n1-standard-2"
composer_disk_size_gb  = 50

# Dataproc
dataproc_version      = "2.0.57-debian10"
dataproc_machine_type = "n1-standard-4"

# Airbyte
airbyte_machine_type = "e2-standard-4"
airbyte_disk_size_gb = 50

# Configurações para alertas
alert_notification_channels = []

# Identificadores de conexões Airbyte
airbyte_transactions_connection_id = "00000000-0000-0000-0000-000000000000" # Placeholder
airbyte_payments_connection_id     = "00000000-0000-0000-0000-000000000000" # Placeholder

# Configurações de CI/CD
cicd_trigger_branch = "develop"
app_version         = "dev"