variable "credentials" {
  description = "My Credentials"
  default = "C:\\Users\\Yuao\\Desktop\\Data Engineer Projects\\google-cloud-sdk\\ny-rides.json"
}

variable "project" {
  description = "Project"
  default = "project-bb55979c-af2b-4840-9d9"
}

variable "region" {
  description = "region"
  default = "us-east4"
}


variable "location" {
  description = "Project Location"
  default = "US"
}

variable "bq_dataset_name" {
  description = "My first BigQuery Dataset name"
  default = "demo_dataset"
}


variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default = "project-bb55979c-af2b-4840-9d9-terra-bucket"
}