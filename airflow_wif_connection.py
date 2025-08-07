# AIRFLOW AWS CONNECTION CONFIGURATION FOR WIF
# This shows how to configure your AWS connection in Airflow

# METHOD 1: Using Airflow UI (Recommended)
# ==========================================
# 1. Go to Admin > Connections in Airflow UI
# 2. Create/Edit connection with ID: aws_default (or your custom aws_conn_id)
# 3. Set Connection Type: Amazon Web Services
# 4. Leave AWS Access Key ID and AWS Secret Access Key BLANK
# 5. In the "Extra" field, add this JSON:

AIRFLOW_AWS_CONNECTION_EXTRA = {
    "role_arn": "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test",
    "assume_role_method": "assume_role_with_web_identity",
    "assume_role_with_web_identity_federation": "google",
    "assume_role_with_web_identity_federation_audience": "sts.amazonaws.com"
}

# METHOD 2: Using Airflow CLI (Alternative)
# ==========================================
# Run this command to create the connection:
"""
airflow connections add aws_wif_connection \
    --conn-type aws \
    --conn-extra '{
        "role_arn": "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test",
        "assume_role_method": "assume_role_with_web_identity", 
        "assume_role_with_web_identity_federation": "google",
        "assume_role_with_web_identity_federation_audience": "sts.amazonaws.com"
    }'
"""

# METHOD 3: Environment Variables (For Cloud Composer)
# ====================================================
# Set these environment variables in Cloud Composer:
ENVIRONMENT_VARIABLES = {
    "AIRFLOW_CONN_AWS_DEFAULT": "aws://?role_arn=arn%3Aaws%3Aiam%3A%3A277108755423%3Arole%2Fsandbox-dvdh-write-from-gcp-to-aws-test&assume_role_method=assume_role_with_web_identity&assume_role_with_web_identity_federation=google&assume_role_with_web_identity_federation_audience=sts.amazonaws.com"
}

# YAML CONFIGURATION UPDATE
# =========================
# In your YAML configuration files, ensure you have:
"""
connection_string:
  service_account_to_impersonate: "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
  aws_conn_id: "aws_default"  # or "aws_wif_connection" if using custom name
"""

# NO CODE CHANGES NEEDED IN YOUR DAG
# ==================================
# Your existing GCSToS3Operator configuration is correct:
"""
transfer_op = GCSToS3Operator(
    task_id=task_id,
    gcs_bucket=temp_bucket,
    prefix=f"{full_destination_path}_",
    dest_s3_key=f"s3://{bucket}/{full_destination_path}_",
    dest_aws_conn_id=aws_connection_id,  # This will use the WIF connection
    google_impersonation_chain=[service_account] if service_account else None,
    replace=overwrite,
)
"""