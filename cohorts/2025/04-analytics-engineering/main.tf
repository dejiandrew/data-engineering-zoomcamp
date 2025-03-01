terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.16.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)  # Path to the service account JSON key
  project     = var.project  # Replace with your Google Cloud project ID
  region      = var.region      # You can change the region
}

resource "google_storage_bucket" "module4_bucket" {
  name     = var.gcs_bucket_name  # Replace with a unique bucket name
  location = var.location  # Adjust based on your needs
  force_destroy = true
}

resource "google_bigquery_dataset" "module4-dataset" {

  dataset_id = var.bq_dataset_name
  location   = var.location
}

output "gcs_bucket_name" {
  value = google_storage_bucket.module4_bucket.name
}

output "bq_dataset_id" {
  value = google_bigquery_dataset.module4-dataset.dataset_id
}
