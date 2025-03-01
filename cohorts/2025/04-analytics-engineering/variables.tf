variable "credentials" {
  description = "My Credentials"
  default     = "./de-zoomcamp-module-4-1309d28cb773.json"
}

variable "project" {
  description = "Project"
  default     = "de-zoomcamp-module-4"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "Yellow & Green Taxi Data 2020-2021"
  default     = "module4_all_trips_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "zoomcamp-module-4-terra-bucket-022625"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}