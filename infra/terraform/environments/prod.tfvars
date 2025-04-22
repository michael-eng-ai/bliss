# Variáveis para ambiente de produção
environment = "prod"
region     = "us-central1"

# Configuração dos buckets
storage_class  = "STANDARD"
force_destroy  = false  # Proteção contra exclusão acidental em produção

# Configuração do Cloud Composer (Airflow)
composer_image_version = "composer-2.0.31-airflow-2.3.4"
composer_node_count    = 5
composer_machine_type  = "n1-standard-4"
composer_disk_size_gb  = 100

# Dataproc
dataproc_version      = "2.0.57-debian10"
dataproc_machine_type = "n1-standard-8"

# Airbyte
airbyte_machine_type = "e2-standard-8"
airbyte_disk_size_gb = 100

# Configurações para alertas
alert_notification_channels = ["projects/saudebliss-prod/notificationChannels/email-alerts"]

# Identificadores de conexões Airbyte
airbyte_transactions_connection_id = "00000000-0000-0000-0000-000000000000" # Placeholder
airbyte_payments_connection_id     = "00000000-0000-0000-0000-000000000000" # Placeholder

# Configurações de CI/CD
cicd_trigger_branch = "main"
app_version         = "latest"  # Será substituído pela versão da tag durante o deploy