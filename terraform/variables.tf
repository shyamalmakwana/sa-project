variable "aiven_api_token" {
  type = string
}

variable "aiven_project" {
  type = string
}

variable "aiven_cloud" {
  type    = string
  default = "google-europe-west1"
}

variable "project_name" {
  type    = string
  default = "sa-test-assignment"
}
