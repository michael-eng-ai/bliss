# Terraform para infraestrutura de CI/CD

# Variáveis para CI/CD
variable "github_owner" {
  description = "Nome do proprietário do repositório GitHub"
  type        = string
  default     = "saudebliss"
}

variable "github_repo" {
  description = "Nome do repositório GitHub"
  type        = string
  default     = "data-pipeline-inadimplencia"
}

variable "github_branch" {
  description = "Branch principal para trigger de CI/CD"
  type        = string
  default     = "main"
}

variable "app_version" {
  description = "Versão da aplicação"
  type        = string
  default     = "latest"
}

# Bucket para armazenar artefatos do Cloud Build
resource "google_storage_bucket" "ci_artifacts" {
  name          = "${var.project_id}-ci-artifacts"
  location      = var.region
  storage_class = var.storage_class
  force_destroy = var.force_destroy

  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

# Artifact Registry para artefatos do pipeline de dados
resource "google_artifact_registry_repository" "pipeline_artifacts" {
  location      = var.region
  repository_id = "data-pipeline-artifacts"
  description   = "Repositório para artefatos do pipeline de dados"
  format        = "DOCKER"
}

# Artifact Registry para pacotes Python
resource "google_artifact_registry_repository" "python_packages" {
  location      = var.region
  repository_id = "python-packages"
  description   = "Repositório para pacotes Python do pipeline de dados"
  format        = "PYTHON"
}

# Secret para armazenar token do GitHub
resource "google_secret_manager_secret" "github_token" {
  secret_id = "github-token"
  
  replication {
    automatic = true
  }
}

# Secret para armazenar chave API do Airbyte
resource "google_secret_manager_secret" "airbyte_api_key" {
  secret_id = "airbyte-api-key"
  
  replication {
    automatic = true
  }
}

# Nota: As versões iniciais dos secrets precisam ser criadas manualmente
# Exemplo: gcloud secrets create github-token --replication-policy="automatic"
# Exemplo: gcloud secrets versions add github-token --data-file="path/to/token.txt"

# Service account para Cloud Build
resource "google_service_account" "cloudbuild_sa" {
  account_id   = "cloudbuild-sa"
  display_name = "Service Account para Cloud Build"
}

# Conceder permissões ao service account do Cloud Build
resource "google_project_iam_member" "cloudbuild_permissions" {
  for_each = toset([
    "roles/cloudbuild.builds.builder",
    "roles/artifactregistry.admin",
    "roles/storage.admin",
    "roles/secretmanager.secretAccessor",
    "roles/composer.admin",
    "roles/dataproc.admin",
    "roles/iam.serviceAccountUser",
    "roles/pubsub.publisher"
  ])
  
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.cloudbuild_sa.email}"
}

# Tópico Pub/Sub para notificações de CI/CD
resource "google_pubsub_topic" "ci_cd_notifications" {
  name = "ci-cd-notifications"
}

# Assinatura do Pub/Sub para processar notificações
resource "google_pubsub_subscription" "ci_cd_notification_sub" {
  name  = "ci-cd-notifications-sub"
  topic = google_pubsub_topic.ci_cd_notifications.name
  
  ack_deadline_seconds = 20
  
  # Opcional: Definir push endpoint para webhook
  # push_config {
  #   push_endpoint = "https://example.com/push-handler"
  # }
  
  # Configuração de expiração de mensagem
  message_retention_duration = "604800s" # 7 dias
  retain_acked_messages = true
  
  # Filtro para mensagens específicas
  filter = "attributes.type = \"build\""
}

# Cloud Build Trigger para branch main
resource "google_cloudbuild_trigger" "main_branch_trigger" {
  name        = "main-branch-trigger"
  description = "Trigger para CI/CD na branch main"
  
  github {
    owner = var.github_owner
    name  = var.github_repo
    push {
      branch = "^main$"
    }
  }
  
  included_files = ["**/*"]
  filename       = ".cloudbuild/cloudbuild.yaml"
  
  service_account = google_service_account.cloudbuild_sa.id
  
  substitutions = {
    _REGION: var.region,
    _ARTIFACT_REPO: google_artifact_registry_repository.pipeline_artifacts.repository_id,
    _ARTIFACT_BUCKET: google_storage_bucket.ci_artifacts.name,
    _SCRIPTS_BUCKET_PROD: "${var.project_id}-scripts-${var.environment}",
    _NOTIFICATION_TOPIC: google_pubsub_topic.ci_cd_notifications.id
  }
}

# Cloud Build Trigger para branch develop
resource "google_cloudbuild_trigger" "develop_branch_trigger" {
  name        = "develop-branch-trigger"
  description = "Trigger para CI/CD na branch develop"
  
  github {
    owner = var.github_owner
    name  = var.github_repo
    push {
      branch = "^develop$"
    }
  }
  
  included_files = ["**/*"]
  filename       = ".cloudbuild/cloudbuild.yaml"
  
  service_account = google_service_account.cloudbuild_sa.id
  
  substitutions = {
    _REGION: var.region,
    _ARTIFACT_REPO: google_artifact_registry_repository.pipeline_artifacts.repository_id,
    _ARTIFACT_BUCKET: google_storage_bucket.ci_artifacts.name,
    _SCRIPTS_BUCKET_DEV: "${var.project_id}-scripts-${var.environment}",
    _NOTIFICATION_TOPIC: google_pubsub_topic.ci_cd_notifications.id
  }
}

# Cloud Build Trigger para Pull Requests
resource "google_cloudbuild_trigger" "pull_request_trigger" {
  name        = "pull-request-trigger"
  description = "Trigger para validação de Pull Requests"
  
  github {
    owner = var.github_owner
    name  = var.github_repo
    pull_request {
      branch = ".*"
      comment_control = "COMMENTS_ENABLED"
    }
  }
  
  included_files = ["**/*"]
  filename       = ".cloudbuild/pr-validation.yaml"
  
  service_account = google_service_account.cloudbuild_sa.id
  
  substitutions = {
    _ARTIFACT_BUCKET: google_storage_bucket.ci_artifacts.name
  }
}

# Cloud Build Trigger para releases (tags)
resource "google_cloudbuild_trigger" "release_trigger" {
  name        = "release-trigger"
  description = "Trigger para deploy de releases (tags)"
  
  github {
    owner = var.github_owner
    name  = var.github_repo
    push {
      tag = "^v[0-9]+\\.[0-9]+\\.[0-9]+$"
    }
  }
  
  included_files = ["**/*"]
  filename       = ".cloudbuild/release.yaml"
  
  service_account = google_service_account.cloudbuild_sa.id
  
  substitutions = {
    _REGION: var.region,
    _ARTIFACT_REPO: google_artifact_registry_repository.pipeline_artifacts.repository_id,
    _ARTIFACT_BUCKET: google_storage_bucket.ci_artifacts.name,
    _SCRIPTS_BUCKET_PROD: "${var.project_id}-scripts-prod",
    _NOTIFICATION_TOPIC: google_pubsub_topic.ci_cd_notifications.id,
    _GITHUB_OWNER: var.github_owner,
    _GITHUB_REPO: var.github_repo,
    _GITHUB_TOKEN_SECRET: google_secret_manager_secret.github_token.name
  }
}

# Dashboard de monitoramento para CI/CD
resource "google_monitoring_dashboard" "cicd_dashboard" {
  dashboard_json = <<EOF
{
  "displayName": "CI/CD Pipeline Dashboard",
  "gridLayout": {
    "widgets": [
      {
        "title": "Cloud Build Execuções por Status",
        "xyChart": {
          "dataSets": [
            {
              "timeSeriesQuery": {
                "timeSeriesFilter": {
                  "filter": "resource.type=\"build\" AND metric.type=\"cloudbuild.googleapis.com/build/count\"",
                  "aggregation": {
                    "alignmentPeriod": "86400s",
                    "perSeriesAligner": "ALIGN_COUNT",
                    "crossSeriesReducer": "REDUCE_SUM",
                    "groupByFields": [
                      "metric.label.status"
                    ]
                  }
                },
                "unitOverride": "1"
              },
              "plotType": "LINE",
              "targetAxis": "Y1"
            }
          ],
          "yAxis": {
            "label": "Builds",
            "scale": "LINEAR"
          }
        }
      },
      {
        "title": "Tempo médio de build",
        "xyChart": {
          "dataSets": [
            {
              "timeSeriesQuery": {
                "timeSeriesFilter": {
                  "filter": "resource.type=\"build\" AND metric.type=\"cloudbuild.googleapis.com/build/execution_time\"",
                  "aggregation": {
                    "alignmentPeriod": "86400s",
                    "perSeriesAligner": "ALIGN_MEAN"
                  }
                },
                "unitOverride": "s"
              },
              "plotType": "LINE",
              "targetAxis": "Y1"
            }
          ],
          "yAxis": {
            "label": "Segundos",
            "scale": "LINEAR"
          }
        }
      }
    ]
  }
}
EOF
}

# Alertas para falhas de CI/CD
resource "google_monitoring_alert_policy" "cicd_failure_alert" {
  display_name = "CI/CD Build Failure Alert"
  combiner     = "OR"
  conditions {
    display_name = "Cloud Build failure rate"
    condition_threshold {
      filter     = "resource.type=\"build\" AND metric.type=\"cloudbuild.googleapis.com/build/count\" AND metric.label.status=\"FAILURE\""
      duration   = "0s"
      comparison = "COMPARISON_GT"
      threshold_value = 0
      trigger {
        count = 1
      }
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_COUNT"
      }
    }
  }

  notification_channels = var.alert_notification_channels
  
  documentation {
    content   = "Um ou mais builds do CI/CD falharam. Verifique os logs do Cloud Build para mais detalhes."
    mime_type = "text/markdown"
  }
}

# Outputs
output "cloudbuild_service_account" {
  value = google_service_account.cloudbuild_sa.email
  description = "Service account usado pelo Cloud Build"
}

output "artifact_registry_docker_repo" {
  value = google_artifact_registry_repository.pipeline_artifacts.name
  description = "Nome do Artifact Registry para imagens Docker"
}

output "artifact_registry_python_repo" {
  value = google_artifact_registry_repository.python_packages.name
  description = "Nome do Artifact Registry para pacotes Python"
}

output "ci_artifacts_bucket" {
  value = google_storage_bucket.ci_artifacts.name
  description = "Nome do bucket para artefatos de CI"
}