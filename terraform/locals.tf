locals {
  env         = terraform.workspace
  name_prefix = "${local.env}-pokemon"

  common_tags = {
    Project     = "pokemon-data-pipeline"
    Environment = local.env
    ManagedBy   = "terraform"
    Workspace   = terraform.workspace
  }
}
