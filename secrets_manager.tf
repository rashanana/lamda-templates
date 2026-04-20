# ──────────────────────────────────────────────────────────────────────────────
# Secrets Manager — J&J AWS Integration
# Manages two secrets:
#   1. HR Data Hub  — multi-field: Authorization (Basic) + apikey
#   2. Athennian    — single field: x-api-key
# ──────────────────────────────────────────────────────────────────────────────

# ── Variables ──────────────────────────────────────────────────────────────────

variable "environment" {
  description = "Deployment environment (dev, uat, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "uat", "prod"], var.environment)
    error_message = "environment must be one of: dev, uat, prod"
  }
}

variable "project" {
  description = "Project name prefix for resource naming"
  type        = string
  default     = "jnj-integration"
}

variable "kms_key_arn" {
  description = "KMS key ARN used to encrypt secrets. Leave empty to use AWS-managed key."
  type        = string
  default     = ""
}

variable "recovery_window_days" {
  description = "Days before secret is permanently deleted after destroy (0 = force delete, 7–30 = scheduled)"
  type        = number
  default     = 7

  validation {
    condition     = var.recovery_window_days == 0 || (var.recovery_window_days >= 7 && var.recovery_window_days <= 30)
    error_message = "recovery_window_days must be 0 (force delete) or between 7 and 30."
  }
}

# HR Data Hub secret values — injected via tfvars or CI/CD secrets store
# Never hardcode in source control
variable "hr_datahub_authorization_token" {
  description = "Base64-encoded Basic auth token for HR Data Hub (value after 'Basic ')"
  type        = string
  sensitive   = true
}

variable "hr_datahub_api_key" {
  description = "API key for HR Data Hub"
  type        = string
  sensitive   = true
}

# Athennian secret value
variable "athennian_x_api_key" {
  description = "x-api-key token for Athennian API"
  type        = string
  sensitive   = true
}


# ── Locals ─────────────────────────────────────────────────────────────────────

locals {
  name_prefix = "${var.project}-${var.environment}"

  common_tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "Terraform"
    Team        = "AWS-Integration"
  }

  # Use provided KMS key or fall back to AWS-managed default
  kms_key_id = var.kms_key_arn != "" ? var.kms_key_arn : null
}


# ── Secret 1: HR Data Hub ──────────────────────────────────────────────────────
# Multi-field secret:
#   Authorization : "Basic <TOKEN>"
#   apikey        : "<TOKEN>"

resource "aws_secretsmanager_secret" "hr_datahub" {
  name        = "${local.name_prefix}/hr-datahub/api-credentials"
  description = "HR Data Hub API credentials — Authorization (Basic) and API key"
  kms_key_id  = local.kms_key_id

  # Prevent accidental deletion in prod
  recovery_window_in_days = var.recovery_window_days

  tags = merge(local.common_tags, {
    SecretType = "api-credentials"
    Consumer   = "lambda-hr-delta-processor"
    System     = "HR-DataHub"
  })
}

resource "aws_secretsmanager_secret_version" "hr_datahub" {
  secret_id = aws_secretsmanager_secret.hr_datahub.id

  # Structured JSON — matches json.loads() call in Lambda
  secret_string = jsonencode({
    Authorization = "Basic ${var.hr_datahub_authorization_token}"
    apikey        = var.hr_datahub_api_key
  })

  # Prevent Terraform from showing secret values in plan/apply output
  lifecycle {
    ignore_changes = [secret_string]
  }
}


# ── Secret 2: Athennian ────────────────────────────────────────────────────────
# Single-field secret:
#   x-api-key : "<TOKEN>"
# Stored as JSON for consistency — Lambdas always use json.loads()

resource "aws_secretsmanager_secret" "athennian" {
  name        = "${local.name_prefix}/athennian/api-credentials"
  description = "Athennian API credentials — x-api-key"
  kms_key_id  = local.kms_key_id

  recovery_window_in_days = var.recovery_window_days

  tags = merge(local.common_tags, {
    SecretType = "api-credentials"
    Consumer   = "lambda-athennian-pull,lambda-athennian-upsert"
    System     = "Athennian"
  })
}

resource "aws_secretsmanager_secret_version" "athennian" {
  secret_id = aws_secretsmanager_secret.athennian.id

  secret_string = jsonencode({
    x-api-key = var.athennian_x_api_key
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}


# ── IAM Policy: Lambda Read Access ────────────────────────────────────────────
# Attach this policy to Lambda execution roles that need secret access.
# Scoped to specific secret ARNs — no wildcard access.

data "aws_iam_policy_document" "secrets_read" {
  statement {
    sid    = "AllowGetSecretValue"
    effect = "Allow"

    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret",
    ]

    resources = [
      aws_secretsmanager_secret.hr_datahub.arn,
      aws_secretsmanager_secret.athennian.arn,
    ]
  }

  # KMS decrypt permission — only added if a customer-managed key is provided
  dynamic "statement" {
    for_each = var.kms_key_arn != "" ? [1] : []
    content {
      sid    = "AllowKmsDecrypt"
      effect = "Allow"

      actions = [
        "kms:Decrypt",
        "kms:GenerateDataKey",
      ]

      resources = [var.kms_key_arn]
    }
  }
}

resource "aws_iam_policy" "secrets_read" {
  name        = "${local.name_prefix}-secrets-read-policy"
  description = "Allows Lambda functions to read HR Data Hub and Athennian secrets"
  policy      = data.aws_iam_policy_document.secrets_read.json

  tags = local.common_tags
}


# ── Outputs ────────────────────────────────────────────────────────────────────

output "hr_datahub_secret_arn" {
  description = "ARN of the HR Data Hub secret — reference in Lambda env var HR_DATA_HUB_SECRET_NAME"
  value       = aws_secretsmanager_secret.hr_datahub.arn
}

output "hr_datahub_secret_name" {
  description = "Name of the HR Data Hub secret"
  value       = aws_secretsmanager_secret.hr_datahub.name
}

output "athennian_secret_arn" {
  description = "ARN of the Athennian secret — reference in Lambda env var ATHENNIAN_SECRET_NAME"
  value       = aws_secretsmanager_secret.athennian.arn
}

output "athennian_secret_name" {
  description = "Name of the Athennian secret"
  value       = aws_secretsmanager_secret.athennian.name
}

output "secrets_read_policy_arn" {
  description = "IAM policy ARN to attach to Lambda execution roles needing secret access"
  value       = aws_iam_policy.secrets_read.arn
}
