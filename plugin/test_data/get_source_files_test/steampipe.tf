resource "aws_account_alternate_contact" "aws_account_security_contact" {
  alternate_contact_type = "SECURITY"

  name          = "Example"
  title         = "Example"
  email_address = "test@example.com"
  phone_number  = "+1234567890"
}