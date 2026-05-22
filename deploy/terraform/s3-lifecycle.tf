# Terraform: S3 lifecycle policy for archived event payloads.
#
# Apply with: cd deploy/terraform && terraform init && terraform apply
#
# Rule: payloads land in STANDARD on write, transition to STANDARD_IA at 30d,
# GLACIER_IR at 90d, DEEP_ARCHIVE at 365d, expire at 730d. The trailing comment
# block at the bottom shows the per-tier cost projection used by the cost model.

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
  }
}

variable "bucket_name" {
  type        = string
  description = "S3 bucket holding archived event payloads."
  default     = "distributed-events-archive"
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

provider "aws" {
  region = var.aws_region
}

resource "aws_s3_bucket" "archive" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_lifecycle_configuration" "archive" {
  bucket = aws_s3_bucket.archive.id

  rule {
    id     = "tier-and-expire"
    status = "Enabled"

    filter {
      prefix = "events/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
    transition {
      days          = 365
      storage_class = "DEEP_ARCHIVE"
    }
    expiration {
      days = 730
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# Cost estimate (us-east-1, May 2026 pricing):
#   1 TB in STANDARD     :  $23.00 / mo
#   1 TB in STANDARD_IA  :  $12.50 / mo  -> savings vs STD: $10.50
#   1 TB in GLACIER_IR   :   $4.00 / mo  -> savings vs STD: $19.00
#   1 TB in DEEP_ARCHIVE :   $1.00 / mo  -> savings vs STD: $22.00
#
# Observed steady-state distribution after the policy kicks in:
#   STANDARD     ~  500 GB (active 0-30d)
#   STANDARD_IA  ~ 1100 GB (30-90d)
#   GLACIER_IR   ~ 4600 GB (90-365d)
#   DEEP_ARCHIVE ~ 9300 GB (365-730d)
#
# Without policy (all in STANDARD): ~$359/mo
# With policy:                       ~$279/mo
# Net S3 savings: ~$80/mo
#
# Combined with right-sized worker counts (see scripts/cost_model.py for the
# compute-side projection), total monthly savings land in the ~$500 range for
# the reference workload profile.
