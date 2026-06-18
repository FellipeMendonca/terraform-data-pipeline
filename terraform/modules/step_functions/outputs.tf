# Step Functions Module - Outputs
# Pokemon Data Pipeline - Exported values from Step Functions module

output "state_machine_arn" {
  description = "ARN of the Step Functions state machine"
  value       = aws_sfn_state_machine.pipeline.arn
}

output "state_machine_name" {
  description = "Name of the Step Functions state machine"
  value       = aws_sfn_state_machine.pipeline.name
}
