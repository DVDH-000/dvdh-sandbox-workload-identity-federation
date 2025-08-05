import boto3
import google.auth
import google.auth.transport.requests
import google.auth.impersonated_credentials
import google.oauth2.service_account
import google.auth.jwt
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Configuration Variables ---
# Load configuration from environment variables

# The email of the DEDICATED GCP service account you created in Phase 1.
# This is the identity we want to use.
GCP_SERVICE_ACCOUNT_EMAIL = os.getenv("GCP_SERVICE_ACCOUNT_EMAIL", "bq-to-s3-transfer-sa@your-gcp-project-id.iam.gserviceaccount.com")

# The ARN of the IAM Role you created in your AWS sandbox in Phase 2.
AWS_ROLE_ARN = os.getenv("ROLE_ARN")

# The name of the S3 bucket you created in your AWS sandbox.
S3_BUCKET_NAME = os.getenv("AWS_S3_BUCKET")

# The AWS region where your S3 bucket is located.
AWS_REGION = os.getenv("AWS_REGION")

def get_google_oidc_token(service_account_email):
    """
    Generates an OIDC token for a specified Google Service Account.
    
    This script assumes it's running in an environment (like a GCE VM or Cloud Shell)
    that has permissions to impersonate the target service account. This is the
    same "Service Account Token Creator" permission your Composer SA has.
    """
    print(f"Attempting to generate OIDC token for: {service_account_email}")
    
    # The audience for the OIDC token must match the audience configured
    # in the AWS IAM Identity Provider. This is the service account's uniqueId.
    # For simplicity in this script, we let the auth library handle discovery.
    # A more specific approach would be to fetch the uniqueId and pass it as the audience.
    credentials, project_id = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

    # Create credentials that are authorized to impersonate the target service account
    impersonated_creds = google.auth.impersonated_credentials.Credentials(
        source_credentials=credentials,
        target_principal=service_account_email,
        target_scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    
    request = google.auth.transport.requests.Request()
    impersonated_creds.refresh(request) # This step performs the impersonation
    
    # Now that we are impersonating, we can get an OIDC token for the target SA
    # For AWS workload identity federation, we need to generate a JWT identity token
    # using the Google Cloud IAM Service Account Credentials API
    
    from googleapiclient.discovery import build
    
    # Create IAM service client using impersonated credentials
    iam_service = build('iamcredentials', 'v1', credentials=impersonated_creds)
    
    # The audience should match what's configured in AWS Identity Provider
    # Typically this is the AWS account ID or a specific audience value
    aws_account_id = os.getenv("AWS_ACCOUNT_ID")
    audience = f"arn:aws:iam::{aws_account_id}:oidc-provider/accounts.google.com"
    
    # Generate OpenID Connect token (JWT)
    request_body = {
        'audience': audience,
        'includeEmail': True
    }
    
    response = iam_service.projects().serviceAccounts().generateIdToken(
        name=f'projects/-/serviceAccounts/{service_account_email}',
        body=request_body
    ).execute()
    
    oidc_token = response['token']
    
    print("Successfully generated Google OIDC token.")
    return oidc_token


def assume_aws_role_with_google_token(role_arn, oidc_token, region):
    """
    Exchanges the Google OIDC token for temporary AWS credentials.
    """
    print(f"Attempting to assume AWS Role: {role_arn}")
    sts_client = boto3.client("sts", region_name=region)
    
    response = sts_client.assume_role_with_web_identity(
        RoleArn=role_arn,
        RoleSessionName="gcp-wlif-test-session", # A descriptive name for the session
        WebIdentityToken=oidc_token
    )
    
    print("Successfully assumed AWS Role.")
    return response['Credentials']


def upload_to_s3_with_temp_credentials(credentials, bucket_name, region):
    """
    Uses the temporary AWS credentials to create an S3 client and upload a file.
    """
    print(f"Connecting to S3 with temporary credentials...")
    s3_client = boto3.client(
        's3',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
        region_name=region
    )
    
    file_content = "Hello from GCP! Workload Identity Federation test successful."
    file_name = "wlif-test.txt"
    
    print(f"Uploading '{file_name}' to bucket '{bucket_name}'...")
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=file_content
    )
    print("Upload complete!")


if __name__ == "__main__":
    try:
        # Step 1: Get the Google OIDC Identity Token
        google_token = get_google_oidc_token(GCP_SERVICE_ACCOUNT_EMAIL)
        
        # Step 2: Exchange the token for temporary AWS credentials
        aws_credentials = assume_aws_role_with_google_token(AWS_ROLE_ARN, google_token, AWS_REGION)
        
        # Step 3: Use the temporary credentials to write to S3
        upload_to_s3_with_temp_credentials(aws_credentials, S3_BUCKET_NAME, AWS_REGION)
        
        print("\n✅ End-to-end test successful!")
        
    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
        print("Please check your GCP/AWS configurations and IAM permissions.")