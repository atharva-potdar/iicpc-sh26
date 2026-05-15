terraform {
  backend "s3" {
    bucket         = "iicpc-tf-state"
    key            = "platform/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "iicpc-tf-locks"
    encrypt        = true
  }
}
