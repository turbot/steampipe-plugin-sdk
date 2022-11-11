terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "~> 4.36"
    }
  }

  required_version = ">= 0.14.9"
}

provider "google" {
  project = var.gcp_project
  region  = var.gcp_region
  zone    = var.gcp_zone
  credentials = "/Users/luis/gcp-turbot-dave.json"
}

data "google_client_config" "current" {}

resource "random_pet" "pet-prefix" {

}

resource "random_integer" "number-prefix" {
  min = 10
  max = 99
}
