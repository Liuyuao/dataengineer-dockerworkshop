terraform {
  required_providers {
    google = {
        source = "hashicorp/google"
        version = "5.6.0"
    }
  }

}

provider "google" {
  project = "{project-bb55979c-af2b-4840-9d9}"
  region  = "us-east2"
}