#
# Account Specific Resources
#

# 1.2 Ensure security contact information is registered
# NOTE: Steampipe account table doesn't support
# https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/account#GetAlternateContactOutput

resource "aws_account_alternate_contact" "aws_account_security_contact" {
  alternate_contact_type = "SECURITY"

  name          = "Example"
  title         = "Example"
  email_address = "test@example.com"
  phone_number  = "+1234567890"
}

output "aws_account_security_contact_pumped_marlin" {
  value = format("arn:aws:::%s", local.pumped_marlin_account_id)
}

output "aws_account_security_contact_usable_rabbit" {
  value = format("arn:aws:::%s", local.usable_rabbit_account_id)
}
