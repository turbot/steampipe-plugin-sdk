
resource "aws_accessanalyzer_analyzer" "accessanalyzer_golden_wren" {
  provider      = aws.pumped_marlin
  analyzer_name = "golden_wren"
}
resource "aws_accessanalyzer_analyzer" "accessanalyzer_golden_123" {
  provider      = aws.pumped_marlin
  analyzer_name = "golden_wren"
}

output "accessanalyzer_golden_wren_region" {
  value = format("arn:aws::%s:%s", var.pumped_marlin_aws_region, local.pumped_marlin_account_id)
}

output "accessanalyzer_whole_bull_region" {
  value = format("arn:aws::%s:%s", var.pumped_marlin_aws_region, local.usable_rabbit_account_id)
}

