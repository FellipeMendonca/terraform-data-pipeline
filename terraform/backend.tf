terraform {
  backend "s3" {
    bucket         = "terraform-state-pokemon-pipeline"
    key            = "pokemon-data-pipeline/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "terraform-lock-pokemon-pipeline"
    encrypt        = true
  }
}
