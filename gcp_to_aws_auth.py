#!/usr/bin/env python3
"""
GCP to AWS Authentication using Service Account ID Tokens
This is the correct approach for Scenario B: GCP → AWS federation
"""

import os
import json
import boto3
import google.auth
import google.oauth2.service_account
from google.auth.transport import requests
from google.auth import identity_pool
import google.auth.jwt
from datetime import datetime, timedelta

def get_gcp_id_token(service_account_email, audience):
    """
    Generate a GCP ID token that can be used with AWS STS AssumeRoleWithWebIdentity
    """
    print(f"🔑 Generating ID token for: {service_account_email}")
    print(f"🎯 Target audience: {audience}")
    
    # Get default credentials (these should have permission to impersonate the service account)
    credentials, project = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    
    # Create impersonated credentials for the target service account
    from google.auth import impersonated_credentials
    
    target_credentials = impersonated_credentials.Credentials(
        source_credentials=credentials,
        target_principal=service_account_email,
        target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
        delegates=[]
    )
    
    # Generate an ID token with the specific audience
    # This is what AWS will validate
    from google.auth.transport import requests as gauth_requests
    
    request = gauth_requests.Request()
    
    # Create ID token credentials
    id_token_credentials = impersonated_credentials.IDTokenCredentials(
        target_credentials,
        target_audience=audience,
        include_email=True
    )
    
    # Refresh to get the actual token
    id_token_credentials.refresh(request)
    
    return id_token_credentials.token

def assume_aws_role_with_gcp_token(role_arn, id_token, session_name="gcp-to-aws-session"):
    """
    Use the GCP ID token to assume an AWS role
    """
    print(f"🎭 Attempting to assume AWS role: {role_arn}")
    print(f"🆔 Using GCP ID token (length: {len(id_token)})")
    
    try:
        # Create STS client
        sts_client = boto3.client('sts')
        
        # Assume role with web identity
        response = sts_client.assume_role_with_web_identity(
            RoleArn=role_arn,
            RoleSessionName=session_name,
            WebIdentityToken=id_token
        )
        
        print("✅ Successfully assumed AWS role!")
        
        # Extract credentials
        aws_credentials = response['Credentials']
        
        return {
            'AccessKeyId': aws_credentials['AccessKeyId'],
            'SecretAccessKey': aws_credentials['SecretAccessKey'],
            'SessionToken': aws_credentials['SessionToken'],
            'Expiration': aws_credentials['Expiration']
        }
        
    except Exception as e:
        print(f"❌ Failed to assume role: {e}")
        return None

def test_aws_access(aws_credentials):
    """
    Test the AWS credentials by trying to access S3
    """
    if not aws_credentials:
        return
    
    print("🧪 Testing AWS credentials...")
    
    try:
        # Create S3 client with the temporary credentials
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_credentials['AccessKeyId'],
            aws_secret_access_key=aws_credentials['SecretAccessKey'],
            aws_session_token=aws_credentials['SessionToken']
        )
        
        # Try to list buckets (this requires minimal permissions)
        response = s3_client.list_buckets()
        
        print("✅ AWS credentials work! Found buckets:")
        for bucket in response['Buckets']:
            print(f"  📦 {bucket['Name']}")
            
    except Exception as e:
        print(f"❌ AWS access test failed: {e}")

def main():
    print("🚀 GCP TO AWS AUTHENTICATION TEST")
    print("=" * 50)
    
    # Configuration from your environment
    service_account_email = "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
    aws_role_arn = "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"
    
    # The audience should match what's configured in your AWS OIDC Identity Provider
    # For Google, this is typically the issuer URL or a custom audience
    audience = "sts.amazonaws.com"  # This is what AWS typically expects
    
    try:
        # Step 1: Get GCP ID token
        print("\n📋 Step 1: Generate GCP ID Token")
        id_token = get_gcp_id_token(service_account_email, audience)
        
        if not id_token:
            print("❌ Failed to get ID token")
            return
        
        print(f"✅ Got ID token: {id_token[:50]}...")
        
        # Step 2: Use ID token to assume AWS role
        print("\n📋 Step 2: Assume AWS Role")
        aws_credentials = assume_aws_role_with_gcp_token(aws_role_arn, id_token)
        
        # Step 3: Test AWS access
        print("\n📋 Step 3: Test AWS Access")
        test_aws_access(aws_credentials)
        
        if aws_credentials:
            print(f"\n🎉 SUCCESS! You can now use these credentials for AWS access")
            print(f"⏰ Credentials expire at: {aws_credentials['Expiration']}")
        
    except Exception as e:
        print(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()