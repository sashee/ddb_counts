provider "aws" {
}

# DDB

resource "random_id" "id" {
  byte_length = 8
}

resource "aws_dynamodb_table" "users" {
  name         = "ddb_counts_sample_${random_id.id.hex}_user"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "ID"

  attribute {
    name = "ID"
    type = "S"
  }
}
resource "aws_dynamodb_table" "counts" {
  name         = "ddb_counts_sample_${random_id.id.hex}_counts"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "type"

  attribute {
    name = "type"
    type = "S"
  }
}

output "users-table" {
	value = aws_dynamodb_table.users.id
}
output "counts-table" {
	value = aws_dynamodb_table.counts.id
}
