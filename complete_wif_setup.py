#!/usr/bin/env python3
"""
Complete WIF Setup for GCP to AWS using Google OIDC Provider
This script demonstrates the full flow for Workload Identity Federation
"""

import os
import json
import boto3
import google.auth
import google.oauth2.service_account
from google.auth.transport import requests
from google.auth import impersonated_credentials
from google.auth.transport import requests as gauth_requests
from botocore.exceptions import ClientError
import jwt
from datetime import datetime

def get_service_account_info(service_account_email):
    """Get information about the service account for WIF setup"""
    print(f"🔍 Getting service account info for: {service_account_email}")
    
    try:
        from google.cloud import iam_admin_v1
        
        client = iam_admin_v1.IAMClient()
        
        # Get service account details
        name = f"projects/-/serviceAccounts/{service_account_email}"
        service_account = client.get_service_account(name=name)
        
        print(f"✅ Service Account Details:")
        print(f"  📧 Email: {service_account.email}")
        print(f"  🆔 Unique ID: {service_account.unique_id}")
        print(f"  📛 Display Name: {service_account.display_name}")
        
        return {
            'email': service_account.email,
            'unique_id': service_account.unique_id,
            'client_id': service_account.unique_id  # For OIDC audience
        }
        
    except Exception as e:
        print(f"❌ Failed to get service account info: {e}")
        return None

def generate_oidc_token(service_account_email, audience):
    """
    Generate an OIDC token for the service account with specified audience
    """
    print(f"🎯 Generating OIDC token for audience: {audience}")
    
    try:
        # Get default credentials
        credentials, project = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        # Create impersonated credentials
        target_credentials = impersonated_credentials.Credentials(
            source_credentials=credentials,
            target_principal=service_account_email,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        # Generate ID token with the specified audience
        id_token_credentials = impersonated_credentials.IDTokenCredentials(
            target_credentials,
            target_audience=audience,
            include_email=True
        )
        
        # Refresh to get the token
        request = gauth_requests.Request()
        id_token_credentials.refresh(request)
        
        return id_token_credentials.token
        
    except Exception as e:
        print(f"❌ Failed to generate OIDC token: {e}")
        return None

def decode_and_inspect_token(token):
    """Decode the OIDC token to inspect its claims"""
    print("🔍 Inspecting OIDC token claims...")
    
    try:
        # Decode without verification to see claims
        decoded = jwt.decode(token, options={"verify_signature": False})
        
        print("📋 Token Claims:")
        for key, value in decoded.items():
            if key in ['aud', 'sub', 'iss', 'email']:
                print(f"  🎯 {key}: {value}")
            else:
                print(f"  📝 {key}: {value}")
        
        return decoded
        
    except Exception as e:
        print(f"❌ Failed to decode token: {e}")
        return None

def test_aws_assume_role(role_arn, oidc_token):
    """Test assuming the AWS role with the OIDC token"""
    print(f"🎭 Testing AWS role assumption...")
    print(f"Role: {role_arn}")
    
    try:
        sts = boto3.client('sts')
        
        response = sts.assume_role_with_web_identity(
            RoleArn=role_arn,
            RoleSessionName='wif-test-session',
            WebIdentityToken=oidc_token
        )
        
        print("✅ Successfully assumed AWS role!")
        
        # Test the credentials
        temp_credentials = response['Credentials']
        
        # Create S3 client with temporary credentials
        s3 = boto3.client(
            's3',
            aws_access_key_id=temp_credentials['AccessKeyId'],
            aws_secret_access_key=temp_credentials['SecretAccessKey'],
            aws_session_token=temp_credentials['SessionToken']
        )
        
        # Test S3 access
        buckets = s3.list_buckets()
        print(f"🪣 Can access {len(buckets['Buckets'])} S3 buckets")
        
        return temp_credentials
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        print(f"❌ AWS STS Error: {error_code}")
        print(f"📝 Message: {error_message}")
        
        if error_code == "InvalidIdentityToken":
            print("\n💡 TROUBLESHOOTING:")
            print("   1. Check that the token audience matches AWS OIDC provider")
            print("   2. Verify the role trust policy allows the correct conditions")
            print("   3. Ensure the service account subject is properly configured")
        
        return None

def create_airflow_connection_config(aws_credentials, service_account_info):
    """Create configuration for Airflow AWS connection using WIF"""
    print("\n🔧 Creating Airflow connection configuration...")
    
    config = {
        "airflow_connection": {
            "conn_id": "aws_wif",
            "conn_type": "Amazon Web Services",
            "description": "AWS connection using GCP WIF",
            "extra": {
                "aws_access_key_id": aws_credentials['AccessKeyId'],
                "aws_secret_access_key": aws_credentials['SecretAccessKey'],
                "aws_session_token": aws_credentials['SessionToken'],
                "region_name": "eu-central-1"
            }
        },
        "gcs_to_s3_operator_example": {
            "task_id": "gcs_to_s3_transfer",
            "gcs_bucket": "your-source-gcs-bucket",
            "prefix": "path/to/files/",
            "dest_s3_key": "s3://sandbox-dvdh-gcp-to-s3/destination/path/",
            "dest_aws_conn_id": "aws_wif",
            "google_impersonation_chain": service_account_info['email'],
            "replace": True
        }
    }
    
    return config

def main():
    print("🚀 COMPLETE WIF SETUP FOR GCP TO AWS")
    print("=" * 60)
    
    # Configuration
    service_account_email = "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
    aws_role_arn = "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"
    
    # First, get service account info
    print("\n📋 Step 1: Get Service Account Information")
    sa_info = get_service_account_info(service_account_email)
    
    if not sa_info:
        print("❌ Cannot proceed without service account info")
        return
    
    # Try different audience values that might be configured in AWS
    audiences_to_try = [
        sa_info['client_id'],  # Service account unique ID
        sa_info['email'],      # Service account email
        "sts.amazonaws.com",   # Common AWS audience
        f"arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"  # Role ARN
    ]
    
    print(f"\n📋 Step 2: Test Different Audiences")
    print("Will try these audiences in order:")
    for i, aud in enumerate(audiences_to_try, 1):
        print(f"  {i}. {aud}")
    
    successful_audience = None
    aws_credentials = None
    
    for audience in audiences_to_try:
        print(f"\n🎯 Testing audience: {audience}")
        
        # Generate OIDC token
        oidc_token = generate_oidc_token(service_account_email, audience)
        
        if not oidc_token:
            print("❌ Failed to generate token")
            continue
        
        # Inspect the token
        token_claims = decode_and_inspect_token(oidc_token)
        
        # Test AWS role assumption
        aws_creds = test_aws_assume_role(aws_role_arn, oidc_token)
        
        if aws_creds:
            print(f"✅ SUCCESS with audience: {audience}")
            successful_audience = audience
            aws_credentials = aws_creds
            break
        else:
            print(f"❌ Failed with audience: {audience}")
    
    if successful_audience:
        print(f"\n🎉 WIF SETUP SUCCESSFUL!")
        print(f"🎯 Working audience: {successful_audience}")
        
        # Create Airflow configuration
        print(f"\n📋 Step 3: Generate Airflow Configuration")
        airflow_config = create_airflow_connection_config(aws_credentials, sa_info)
        
        # Save configuration
        with open('airflow_wif_config.json', 'w') as f:
            json.dump(airflow_config, f, indent=2, default=str)
        
        print("💾 Saved configuration to: airflow_wif_config.json")
        print("\n🎯 NEXT STEPS:")
        print("1. Use the working audience in your AWS role trust policy")
        print("2. Set up the Airflow connection using the generated config")
        print("3. Update your GCSToS3Operator to use 'aws_wif' connection")
        
    else:
        print("\n❌ WIF SETUP FAILED")
        print("💡 Check your AWS OIDC provider configuration")
        print("💡 Verify your role trust policy conditions")

if __name__ == "__main__":
    main()