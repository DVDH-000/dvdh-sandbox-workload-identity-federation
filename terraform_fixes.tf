# TERRAFORM ADDITIONS NEEDED FOR WIF TO WORK
# Add these to your existing Terraform configuration

# 1. ADD: Grant OpenID Token Creator role (CRITICAL - this was missing!)
resource "google_service_account_iam_member" "composer_tina_s3_openid_token_creator" {
  service_account_id = google_service_account.tina_gcp_to_s3_sa.name
  role               = "roles/iam.serviceAccountOpenIdTokenCreator"
  member             = "serviceAccount:${google_service_account.airflow.email}"
}

# 2. OPTIONAL: Create AWS OIDC Identity Provider (if not already exists)
# This would be in your AWS Terraform configuration
resource "aws_iam_openid_connect_provider" "google" {
  url = "https://accounts.google.com"
  
  client_id_list = [
    # This should be the unique ID of your GCP service account
    # You can get this from: gcloud iam service-accounts describe tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com --format="value(uniqueId)"
    "111691016524440319722"  # Replace with actual unique ID
  ]
  
  thumbprint_list = [
    "f5d53439cdd63bb411cc5c5640b264b9094b7667"  # Google's thumbprint
  ]
  
  tags = {
    Name        = "GoogleOIDCProvider"
    Environment = var.otap_environment
  }
}

# 3. CREATE: AWS IAM Role for WIF (if not already exists)
resource "aws_iam_role" "gcp_to_s3_wif_role" {
  name = "sandbox-dvdh-write-from-gcp-to-aws-${var.otap_environment}"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = aws_iam_openid_connect_provider.google.arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "accounts.google.com:aud" = "111691016524440319722"  # Service account unique ID
            "accounts.google.com:oaud" = "sts.amazonaws.com"     # The working audience
          }
        }
      }
    ]
  })
  
  tags = {
    Name        = "GCPToS3WIFRole"
    Environment = var.otap_environment
  }
}

# 4. ATTACH: S3 permissions to the role
resource "aws_iam_role_policy" "gcp_to_s3_permissions" {
  name = "GCPToS3Permissions"
  role = aws_iam_role.gcp_to_s3_wif_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:GetObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket_name}",
          "arn:aws:s3:::${var.s3_bucket_name}/*"
        ]
      }
    ]
  })
}

# 5. OUTPUT: Role ARN for Airflow configuration
output "aws_wif_role_arn" {
  value = aws_iam_role.gcp_to_s3_wif_role.arn
  description = "ARN of the AWS role for GCP to S3 WIF"
}

output "gcp_service_account_unique_id" {
  value = google_service_account.tina_gcp_to_s3_sa.unique_id
  description = "Unique ID of the GCP service account for AWS OIDC configuration"
}