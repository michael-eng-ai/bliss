terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

# Variáveis
variable "project_id" {
  description = "ID do projeto GCP"
  type        = string
}

variable "region" {
  description = "Região principal do GCP"
  type        = string
  default     = "us-central1"
}

variable "env" {
  description = "Ambiente (dev, staging, prod)"
  type        = string
  default     = "dev"
}

# Provider
provider "google" {
  project = var.project_id
  region  = var.region
}

# Storage buckets para as camadas do lakehouse
resource "google_storage_bucket" "landing_bucket" {
  name     = "${var.project_id}-landing-${var.env}"
  location = var.region
  storage_class = "STANDARD"
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "bronze_bucket" {
  name     = "${var.project_id}-bronze-${var.env}"
  location = var.region
  storage_class = "STANDARD"
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "silver_bucket" {
  name     = "${var.project_id}-silver-${var.env}"
  location = var.region
  storage_class = "STANDARD"
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "gold_bucket" {
  name     = "${var.project_id}-gold-${var.env}"
  location = var.region
  storage_class = "STANDARD"
  uniform_bucket_level_access = true
}

# Bucket para scripts
resource "google_storage_bucket" "scripts_bucket" {
  name     = "${var.project_id}-scripts-${var.env}"
  location = var.region
  storage_class = "STANDARD"
  uniform_bucket_level_access = true
}

# Service accounts
resource "google_service_account" "dataproc_service_account" {
  account_id   = "dataproc-sa"
  display_name = "Dataproc Service Account"
}

resource "google_service_account" "airbyte_service_account" {
  account_id   = "airbyte-sa"
  display_name = "Airbyte Service Account"
}

resource "google_service_account" "composer_service_account" {
  account_id   = "composer-sa"
  display_name = "Cloud Composer Service Account"
}

# IAM bindings para os buckets
resource "google_storage_bucket_iam_binding" "dataproc_landing_access" {
  bucket = google_storage_bucket.landing_bucket.name
  role   = "roles/storage.objectViewer"
  members = [
    "serviceAccount:${google_service_account.dataproc_service_account.email}",
  ]
}

resource "google_storage_bucket_iam_binding" "dataproc_bronze_access" {
  bucket = google_storage_bucket.bronze_bucket.name
  role   = "roles/storage.objectAdmin"
  members = [
    "serviceAccount:${google_service_account.dataproc_service_account.email}",
    "serviceAccount:${google_service_account.airbyte_service_account.email}",
  ]
}

resource "google_storage_bucket_iam_binding" "dataproc_silver_access" {
  bucket = google_storage_bucket.silver_bucket.name
  role   = "roles/storage.objectAdmin"
  members = [
    "serviceAccount:${google_service_account.dataproc_service_account.email}",
  ]
}

resource "google_storage_bucket_iam_binding" "dataproc_gold_access" {
  bucket = google_storage_bucket.gold_bucket.name
  role   = "roles/storage.objectAdmin"
  members = [
    "serviceAccount:${google_service_account.dataproc_service_account.email}",
  ]
}

# Dataproc Metastore para tabelas Iceberg
resource "google_dataproc_metastore_service" "iceberg_metastore" {
  service_id = "iceberg-metastore"
  location   = var.region
  tier       = "DEVELOPER"
  
  hive_metastore_config {
    version = "3.1.2"
    
    config_overrides = {
      "iceberg.enabled" = "true"
    }
  }
}

# Cloud Composer (Airflow)
resource "google_composer_environment" "composer" {
  name   = "data-pipeline-composer"
  region = var.region
  
  config {
    software_config {
      image_version = "composer-2.0.31-airflow-2.3.4"
      
      airflow_config_overrides = {
        core-dags_are_paused_at_creation = "True"
        core-load_examples = "False"
      }
      
      env_variables = {
        AIRFLOW_VAR_PROJECT_ID = var.project_id
        AIRFLOW_VAR_REGION = var.region
        AIRFLOW_VAR_ENV = var.env
        AIRFLOW_VAR_LANDING_BUCKET = google_storage_bucket.landing_bucket.name
        AIRFLOW_VAR_BRONZE_BUCKET = google_storage_bucket.bronze_bucket.name
        AIRFLOW_VAR_SILVER_BUCKET = google_storage_bucket.silver_bucket.name
        AIRFLOW_VAR_GOLD_BUCKET = google_storage_bucket.gold_bucket.name
        AIRFLOW_VAR_SCRIPTS_BUCKET = google_storage_bucket.scripts_bucket.name
      }
      
      pypi_packages = {
        "apache-airflow-providers-google" = "==8.9.0"
        "apache-airflow-providers-airbyte" = "==3.1.0"
      }
    }
    
    node_config {
      service_account = google_service_account.composer_service_account.email
    }
  }
}

# BigQuery Dataset para BigLake
resource "google_bigquery_dataset" "analytics" {
  dataset_id                  = "analytics"
  friendly_name               = "Analytics Data Lake"
  description                 = "Dataset para análise de inadimplência"
  location                    = var.region
  delete_contents_on_destroy  = false
  
  access {
    role          = "READER"
    user_by_email = google_service_account.dataproc_service_account.email
  }
  
  access {
    role          = "READER"
    special_group = "projectReaders"
  }
}

# Cloud Function para detecção de novos arquivos
resource "google_storage_bucket" "functions_bucket" {
  name     = "${var.project_id}-functions-${var.env}"
  location = var.region
}

resource "google_service_account" "cloud_function_sa" {
  account_id   = "cloud-function-sa"
  display_name = "Cloud Function Service Account"
}

resource "google_project_iam_member" "function_airbyte_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.cloud_function_sa.email}"
}

# Data Catalog para metadados e classificação
resource "google_data_catalog_taxonomy" "inadimplencia_taxonomy" {
  display_name = "Taxonomy for Inadimplência"
  description  = "Taxonomy for classifying data based on inadimplência metrics"
  region       = var.region
}

resource "google_data_catalog_policy_tag" "pii_tag" {
  taxonomy     = google_data_catalog_taxonomy.inadimplencia_taxonomy.id
  display_name = "PII"
  description  = "Tag for personally identifiable information"
}

resource "google_data_catalog_policy_tag" "financial_tag" {
  taxonomy     = google_data_catalog_taxonomy.inadimplencia_taxonomy.id
  display_name = "Financial"
  description  = "Tag for financial data"
}

# Outputs
output "landing_bucket" {
  value = google_storage_bucket.landing_bucket.name
}

output "bronze_bucket" {
  value = google_storage_bucket.bronze_bucket.name
}

output "silver_bucket" {
  value = google_storage_bucket.silver_bucket.name
}

output "gold_bucket" {
  value = google_storage_bucket.gold_bucket.name
}

output "scripts_bucket" {
  value = google_storage_bucket.scripts_bucket.name
}

output "composer_environment" {
  value = google_composer_environment.composer.name
}

output "bq_dataset" {
  value = google_bigquery_dataset.analytics.dataset_id
}

output "dataproc_metastore" {
  value = google_dataproc_metastore_service.iceberg_metastore.name
}