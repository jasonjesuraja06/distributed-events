# Terraform: S3 lifecycle policy for archived event payloads.
#
# Apply with: cd deploy/terraform && terraform init && terraform apply
#
# Rule: payloads land in STANDARD on write, transition to STANDARD_IA at 30d,
# GLACIER_IR at 90d, DEEP_ARCHIVE at 365d, expire at 730d.
#
# This configuration has never been applied. No AWS account, bucket, or archive
# is associated with this repository, and nothing here has been billed. The
# trailing comment block is a hypothetical sizing exercise, kept because the
# tier boundaries above only make sense alongside the arithmetic that motivates
# them.

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

# ---------------------------------------------------------------------------
# HYPOTHETICAL sizing exercise. Every quantity below is assumed, not observed.
#
# Per-TB-month list prices (us-east-1, published May 2026):
#   STANDARD     : $23.00
#   STANDARD_IA  : $12.50
#   GLACIER_IR   :  $4.00
#   DEEP_ARCHIVE :  $1.00
#
# Assume a hypothetical archive that has reached steady state under this policy
# with a uniform ingest rate and a 730d expiry, giving tier sizes proportional
# to each tier's age band:
#   STANDARD     0.5 TB   (0-30d)
#   STANDARD_IA  1.1 TB   (30-90d)
#   GLACIER_IR   4.6 TB   (90-365d)
#   DEEP_ARCHIVE 9.3 TB   (365-730d)
#
# Under those assumptions the arithmetic gives roughly $279/mo with the policy
# against roughly $359/mo if the same 15.5 TB sat entirely in STANDARD.
#
# Treat those figures as what they are: the output of assumed inputs. The
# measured numbers in this project are the benchmark reports under
# bench/reports/, and none of them are S3 numbers.
# ---------------------------------------------------------------------------
