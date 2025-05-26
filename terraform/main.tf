terraform {
  required_providers {
    aiven = {
      source  = "aiven/aiven"
      version = "~> 3.0"
    }
  }
}

provider "aiven" {
  api_token = var.aiven_api_token
}

resource "aiven_kafka" "kafka" {
  project                 = var.aiven_project
  cloud_name              = var.aiven_cloud
  plan                    = "startup-2"
  service_name            = "${var.project_name}-kafka"
  maintenance_window_dow  = "saturday"
  maintenance_window_time = "10:00:00"  
}

resource "aiven_pg" "postgresql" {
  project                 = var.aiven_project
  cloud_name              = var.aiven_cloud
  plan                    = "startup-4"
  service_name            = "${var.project_name}-pg"
  maintenance_window_dow  = "saturday"
  maintenance_window_time = "10:00:00"
}

resource "aiven_opensearch" "opensearch" {
  project                 = var.aiven_project
  cloud_name              = var.aiven_cloud
  plan                    = "startup-4"
  service_name            = "${var.project_name}-opensearch"
  maintenance_window_dow  = "saturday"
  maintenance_window_time = "10:00:00"
  
}


output "kafka_bootstrap_servers" {
  value = aiven_kafka.kafka.service_host
}

output "postgres_connection_info" {
  value = aiven_pg.postgresql.service_uri
  sensitive = true
}

output "opensearch_http_endpoint" {
  value = aiven_opensearch.opensearch.service_uri
  sensitive = true
}