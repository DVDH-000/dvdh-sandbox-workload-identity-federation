#!/usr/bin/env python3
"""
Simple WIF test with known service account information
"""

import os
import json
import boto3
import google.auth
from google.auth import impersonated_credentials
from google.auth.transport import requests as gauth_requests
from botocore.exceptions import ClientError
import jwt

def generate_oidc_token(service_account_email, audience):
    """Generate OIDC token for the service account with specified audience"""
    print(f"🎯 Generating OIDC token")
    print(f"   Service Account: {service_account_email}")
    print(f"   Audience: {audience}")
    
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

def decode_token(token):
    """Decode and display token claims"""
    try:
        decoded = jwt.decode(token, options={"verify_signature": False})
        
        print("📋 Token Claims:")
        for key, value in decoded.items():
            if key == 'aud':
                print(f"  🎯 {key}: {value}")
            elif key == 'sub':
                print(f"  👤 {key}: {value}")
            elif key == 'iss':
                print(f"  🏢 {key}: {value}")
            elif key == 'email':
                print(f"  📧 {key}: {value}")
            else:
                print(f"  📝 {key}: {value}")
        
        return decoded
        
    except Exception as e:
        print(f"❌ Failed to decode token: {e}")
        return None

def test_aws_role_assumption(role_arn, oidc_token):
    """Test AWS role assumption with the OIDC token"""
    print(f"\n🎭 Testing AWS role assumption...")
    print(f"Role ARN: {role_arn}")
    
    try:
        sts = boto3.client('sts')
        
        response = sts.assume_role_with_web_identity(
            RoleArn=role_arn,
            RoleSessionName='wif-test-session',
            WebIdentityToken=oidc_token
        )
        
        print("✅ Successfully assumed AWS role!")
        
        # Test the credentials by listing S3 buckets
        temp_creds = response['Credentials']
        s3 = boto3.client(
            's3',
            aws_access_key_id=temp_creds['AccessKeyId'],
            aws_secret_access_key=temp_creds['SecretAccessKey'],
            aws_session_token=temp_creds['SessionToken']
        )
        
        buckets = s3.list_buckets()
        print(f"🪣 Successfully accessed S3: {len(buckets['Buckets'])} buckets found")
        
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        print(f"❌ AWS Error: {error_code}")
        print(f"📝 Message: {error_message}")
        
        return False

def main():
    print("🧪 SIMPLE WIF TEST")
    print("=" * 40)
    
    # Configuration
    service_account_email = "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
    aws_role_arn = "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"
    
    # From your AWS OIDC provider screenshot, these are likely audience values
    # You need to tell me what the two audiences are in your AWS console
    audiences_to_try = [
        "104485534524932017",  # This might be one of your configured audiences
        "116910165246401197722",  # This might be the other one
        service_account_email,  # The service account email
        "sts.amazonaws.com"     # Standard AWS STS audience
    ]
    
    print("🎯 Will test these audiences:")
    for i, audience in enumerate(audiences_to_try, 1):
        print(f"  {i}. {audience}")
    
    for audience in audiences_to_try:
        print(f"\n{'='*50}")
        print(f"🧪 Testing Audience: {audience}")
        print('='*50)
        
        # Generate token
        token = generate_oidc_token(service_account_email, audience)
        if not token:
            continue
        
        print(f"✅ Generated token (length: {len(token)})")
        
        # Decode and inspect
        decoded = decode_token(token)
        if not decoded:
            continue
        
        # Test AWS role assumption
        success = test_aws_role_assumption(aws_role_arn, token)
        
        if success:
            print(f"\n🎉 SUCCESS! Working audience: {audience}")
            print("\n📋 CONFIGURATION FOR CLOUD COMPOSER:")
            print(f"   Service Account: {service_account_email}")
            print(f"   Audience: {audience}")
            print(f"   AWS Role: {aws_role_arn}")
            break
        else:
            print(f"❌ Failed with audience: {audience}")
    
    else:
        print(f"\n❌ All audiences failed!")
        print("\n💡 TROUBLESHOOTING:")
        print("1. Check the audiences configured in your AWS OIDC provider")
        print("2. Verify your role trust policy conditions")
        print("3. Ensure the service account has the right permissions")

if __name__ == "__main__":
    main()