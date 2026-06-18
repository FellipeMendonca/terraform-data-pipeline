terraform {
  backend "s3" {
    bucket         = "terraform-state-pokemon-data-pipeline"
    key            = "pokemon-data-pipeline/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "terraform-lock-pokemon-pipeline"
    encrypt        = true
  }
}
