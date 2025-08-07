#!/usr/bin/env python3
"""
Focused WIF Debugging - Let's get this working!
"""

import os
import json
import boto3
import google.auth
from google.auth.transport import requests
from google.oauth2 import service_account
from google.auth import impersonated_credentials
from google.auth.transport import requests as gauth_requests
import jwt
from botocore.exceptions import ClientError

def debug_current_auth():
    """Debug what authentication we currently have"""
    print("🔍 DEBUGGING CURRENT AUTHENTICATION")
    print("=" * 50)
    
    try:
        credentials, project = google.auth.default()
        print(f"✅ Project: {project}")
        print(f"📧 Credentials type: {type(credentials).__name__}")
        
        if hasattr(credentials, 'service_account_email'):
            print(f"🤖 Service Account: {credentials.service_account_email}")
            return credentials.service_account_email, project
        else:
            print("👤 User credentials detected")
            return None, project
            
    except Exception as e:
        print(f"❌ Auth error: {e}")
        return None, None

def try_direct_service_account_approach():
    """Try using service account directly instead of impersonation"""
    print("\n🎯 TRYING DIRECT SERVICE ACCOUNT APPROACH")
    print("=" * 50)
    
    # Check if we have service account key file
    sa_key_paths = [
        'service-account-key.json',
        'sa-key.json',
        os.path.expanduser('~/.config/gcloud/application_default_credentials.json')
    ]
    
    for key_path in sa_key_paths:
        if os.path.exists(key_path):
            print(f"📁 Found potential key file: {key_path}")
            try:
                with open(key_path, 'r') as f:
                    key_data = json.load(f)
                    if 'client_email' in key_data:
                        print(f"🔑 Service account: {key_data['client_email']}")
                        return key_path, key_data['client_email']
            except Exception as e:
                print(f"❌ Error reading {key_path}: {e}")
    
    print("💡 No service account key file found")
    return None, None

def generate_id_token_direct(service_account_email, audience, key_file=None):
    """Generate ID token using direct service account credentials"""
    print(f"\n🎯 GENERATING ID TOKEN DIRECTLY")
    print(f"   Service Account: {service_account_email}")
    print(f"   Audience: {audience}")
    
    try:
        if key_file:
            # Use service account key file
            credentials = service_account.Credentials.from_service_account_file(
                key_file,
                scopes=['https://www.googleapis.com/auth/cloud-platform']
            )
            print("🔑 Using service account key file")
        else:
            # Use default credentials but try to avoid impersonation
            credentials, _ = google.auth.default(
                scopes=['https://www.googleapis.com/auth/cloud-platform']
            )
            print("🔑 Using default credentials")
        
        # Create ID token request
        request = requests.Request()
        
        # Generate ID token using the IAM service
        from google.oauth2 import service_account as sa
        
        if isinstance(credentials, sa.Credentials):
            # Direct service account - can generate ID token directly
            print("✅ Direct service account detected")
            
            # Create ID token
            import google.auth.jwt as jwt_lib
            
            now = int(time.time())
            payload = {
                'iss': credentials.service_account_email,
                'aud': audience,
                'sub': credentials.service_account_email,
                'iat': now,
                'exp': now + 3600,  # 1 hour
                'email': credentials.service_account_email,
                'email_verified': True
            }
            
            # Sign the JWT
            token = jwt_lib.encode(credentials.signer, payload)
            return token
        else:
            print("⚠️  Not a direct service account, will try impersonation approach")
            return None
            
    except Exception as e:
        print(f"❌ Direct approach failed: {e}")
        return None

def test_different_audiences(service_account_email):
    """Test the most likely audience values systematically"""
    print("\n🎯 TESTING SYSTEMATIC AUDIENCES")
    print("=" * 50)
    
    # These are the most common audience patterns for Google→AWS WIF
    audiences = [
        # AWS STS standard audience
        "sts.amazonaws.com",
        
        # Service account unique ID (from your AWS console screenshot)
        "104485534524932017",
        "116910165246401197722", 
        
        # Service account email
        service_account_email,
        
        # AWS account ID
        "277108755423",
        
        # Role ARN as audience
        "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test",
        
        # Google's issuer URL
        "https://accounts.google.com",
        
        # Custom audiences that might be configured
        f"aws:iam::{277108755423}:oidc-provider/accounts.google.com"
    ]
    
    service_account_email = "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
    aws_role_arn = "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"
    
    for i, audience in enumerate(audiences, 1):
        print(f"\n🧪 Test {i}/{len(audiences)}: {audience}")
        print("-" * 60)
        
        # Try to generate token
        token = generate_token_any_way(service_account_email, audience)
        
        if token:
            print(f"✅ Generated token (length: {len(token)})")
            
            # Decode to see claims
            try:
                decoded = jwt.decode(token, options={"verify_signature": False})
                print("📋 Token claims:")
                for key, value in decoded.items():
                    print(f"  {key}: {value}")
            except:
                print("⚠️  Could not decode token")
            
            # Test with AWS
            success = test_aws_assume_role(aws_role_arn, token)
            if success:
                print(f"🎉 SUCCESS! Working audience: {audience}")
                return audience, token
            else:
                print("❌ AWS rejected this token")
        else:
            print("❌ Could not generate token")
    
    return None, None

def generate_token_any_way(service_account_email, audience):
    """Try multiple approaches to generate a token"""
    
    # Method 1: Try impersonation (what we've been doing)
    try:
        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        target_credentials = impersonated_credentials.Credentials(
            source_credentials=credentials,
            target_principal=service_account_email,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        id_token_credentials = impersonated_credentials.IDTokenCredentials(
            target_credentials,
            target_audience=audience,
            include_email=True
        )
        
        request = gauth_requests.Request()
        id_token_credentials.refresh(request)
        
        return id_token_credentials.token
        
    except Exception as e:
        print(f"  Impersonation failed: {e}")
    
    # Method 2: Try gcloud command
    try:
        import subprocess
        
        cmd = [
            'gcloud', 'auth', 'print-identity-token',
            f'--audiences={audience}',
            f'--impersonate-service-account={service_account_email}'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            token = result.stdout.strip()
            print(f"  ✅ gcloud generated token")
            return token
        else:
            print(f"  gcloud failed: {result.stderr}")
    
    except Exception as e:
        print(f"  gcloud approach failed: {e}")
    
    return None

def test_aws_assume_role(role_arn, token):
    """Test AWS role assumption"""
    try:
        sts = boto3.client('sts')
        
        response = sts.assume_role_with_web_identity(
            RoleArn=role_arn,
            RoleSessionName='wif-debug-session',
            WebIdentityToken=token
        )
        
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        print(f"  AWS Error: {error_code} - {error_message}")
        return False
    
    except Exception as e:
        print(f"  Unexpected error: {e}")
        return False

def main():
    print("🚀 FOCUSED WIF DEBUGGING - LET'S GET THIS WORKING!")
    print("=" * 70)
    
    # Step 1: Check current auth
    current_sa, project = debug_current_auth()
    
    # Step 2: Try direct service account approach
    key_file, sa_email = try_direct_service_account_approach()
    
    # Step 3: Use target service account
    target_sa = "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
    
    # Step 4: Test all possible audiences systematically
    working_audience, working_token = test_different_audiences(target_sa)
    
    if working_audience:
        print(f"\n🎉 WIF IS WORKING!")
        print(f"🎯 Working Audience: {working_audience}")
        print(f"📋 Use this in your Cloud Composer setup")
    else:
        print(f"\n❌ Still debugging needed")
        print(f"💡 Let's check your AWS OIDC provider configuration")

if __name__ == "__main__":
    import time
    main()