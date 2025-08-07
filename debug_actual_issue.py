#!/usr/bin/env python3
"""
Debug the actual WIF issue in production
Let's trace exactly what's happening with your token generation
"""

import os
import json
import google.auth
from google.auth import impersonated_credentials
from google.auth.transport import requests as gauth_requests
import jwt

def debug_token_generation():
    """Debug the exact token generation process"""
    print("🔍 DEBUGGING TOKEN GENERATION")
    print("=" * 60)
    
    target_sa = "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
    audience = "sts.amazonaws.com"
    
    try:
        # Step 1: Get current credentials
        credentials, project = google.auth.default()
        print(f"✅ Current project: {project}")
        print(f"📧 Credentials type: {type(credentials).__name__}")
        
        if hasattr(credentials, 'service_account_email'):
            print(f"🤖 Current SA: {credentials.service_account_email}")
        
        # Step 2: Create impersonated credentials
        print(f"\n🎭 Impersonating: {target_sa}")
        
        impersonated_creds = impersonated_credentials.Credentials(
            source_credentials=credentials,
            target_principal=target_sa,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        request = gauth_requests.Request()
        impersonated_creds.refresh(request)
        print("✅ Impersonation successful")
        
        # Step 3: Generate ID token with the EXACT audience Airflow will use
        print(f"\n🎯 Generating ID token with audience: {audience}")
        
        id_token_credentials = impersonated_credentials.IDTokenCredentials(
            impersonated_creds,
            target_audience=audience,
            include_email=True
        )
        
        id_token_credentials.refresh(request)
        token = id_token_credentials.token
        
        print(f"✅ Token generated (length: {len(token)})")
        
        # Step 4: Decode and analyze the token
        print(f"\n📋 TOKEN ANALYSIS")
        print("-" * 40)
        
        decoded = jwt.decode(token, options={"verify_signature": False})
        
        for key, value in decoded.items():
            if key == 'aud':
                print(f"🎯 {key}: {value} ← AUDIENCE (must match AWS expectation)")
            elif key == 'azp':
                print(f"🔑 {key}: {value} ← AUTHORIZED PARTY (SA unique ID)")
            elif key == 'sub':
                print(f"👤 {key}: {value} ← SUBJECT (should match azp)")
            elif key == 'iss':
                print(f"🏢 {key}: {value} ← ISSUER (must be accounts.google.com)")
            elif key == 'email':
                print(f"📧 {key}: {value} ← EMAIL")
            elif key in ['exp', 'iat']:
                from datetime import datetime
                print(f"⏰ {key}: {value} ({datetime.fromtimestamp(value)})")
            else:
                print(f"📝 {key}: {value}")
        
        return token, decoded
        
    except Exception as e:
        print(f"❌ Token generation failed: {e}")
        return None, None

def check_aws_role_trust_policy():
    """Check what AWS expects vs what we're sending"""
    print("\n🔍 AWS ROLE TRUST POLICY CHECK")
    print("=" * 60)
    
    role_arn = "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"
    
    print(f"🎭 Role: {role_arn}")
    print("\n📋 Expected Trust Policy Should Have:")
    print("✅ Principal: Federated = arn:aws:iam::277108755423:oidc-provider/accounts.google.com")
    print("✅ Action: sts:AssumeRoleWithWebIdentity")
    print("✅ Condition: StringEquals")
    print("   - accounts.google.com:aud = SERVICE_ACCOUNT_UNIQUE_ID")
    print("   - accounts.google.com:oaud = 'sts.amazonaws.com'")
    
    print("\n💡 CRITICAL: AWS maps claims differently:")
    print("   - accounts.google.com:aud checks the JWT's 'azp' claim")
    print("   - accounts.google.com:oaud checks the JWT's 'aud' claim")

def test_airflow_aws_provider():
    """Test how Airflow's AWS provider generates tokens"""
    print("\n🔍 TESTING AIRFLOW AWS PROVIDER BEHAVIOR")
    print("=" * 60)
    
    try:
        # Simulate what Airflow does
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
        from airflow.models.connection import Connection
        
        # Create a mock connection object with your exact config
        conn = Connection(
            conn_id='test_wif',
            conn_type='aws',
            extra=json.dumps({
                "role_arn": "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test",
                "assume_role_method": "assume_role_with_web_identity",
                "assume_role_with_web_identity_federation": "google",
                "assume_role_with_web_identity_federation_audience": "sts.amazonaws.com"
            })
        )
        
        print("✅ Mock connection created with your exact config")
        
        # This would be the internal process
        extra = conn.extra_dejson
        print(f"📋 Parsed Extra Config:")
        for key, value in extra.items():
            print(f"   {key}: {value}")
        
        # Check the specific parameters
        role_arn = extra.get('role_arn')
        audience = extra.get('assume_role_with_web_identity_federation_audience')
        federation = extra.get('assume_role_with_web_identity_federation')
        method = extra.get('assume_role_method')
        
        print(f"\n🎯 Key Parameters:")
        print(f"   Role ARN: {role_arn}")
        print(f"   Audience: {audience}")
        print(f"   Federation: {federation}")
        print(f"   Method: {method}")
        
        if all([role_arn, audience, federation == 'google', method == 'assume_role_with_web_identity']):
            print("✅ All required parameters present")
        else:
            print("❌ Missing required parameters")
            
    except ImportError:
        print("❌ Cannot import Airflow AWS provider")
    except Exception as e:
        print(f"❌ Error testing provider: {e}")

def analyze_token_vs_aws_expectations(token_claims):
    """Analyze if the token matches AWS expectations"""
    print("\n🔍 TOKEN VS AWS EXPECTATIONS")
    print("=" * 60)
    
    if not token_claims:
        print("❌ No token claims to analyze")
        return
    
    # Get the key claims
    token_aud = token_claims.get('aud')
    token_azp = token_claims.get('azp')
    token_sub = token_claims.get('sub')
    token_iss = token_claims.get('iss')
    
    print("📊 ANALYSIS:")
    
    # Check issuer
    if token_iss == "https://accounts.google.com":
        print("✅ Issuer correct: https://accounts.google.com")
    else:
        print(f"❌ Issuer wrong: {token_iss}")
    
    # Check audience
    if token_aud == "sts.amazonaws.com":
        print("✅ Audience correct: sts.amazonaws.com")
    else:
        print(f"❌ Audience wrong: {token_aud}")
    
    # Check azp vs sub
    if token_azp == token_sub:
        print(f"✅ azp matches sub: {token_azp}")
    else:
        print(f"❌ azp/sub mismatch: azp={token_azp}, sub={token_sub}")
    
    print(f"\n🎯 FOR AWS TRUST POLICY:")
    print(f"   accounts.google.com:aud should be: {token_azp}")
    print(f"   accounts.google.com:oaud should be: {token_aud}")
    
    # Check if this matches your service account
    expected_sa = "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
    if 'email' in token_claims and token_claims['email'] == expected_sa:
        print(f"✅ Email matches target SA: {expected_sa}")
    else:
        print(f"❌ Email mismatch or missing")

def main():
    print("🚀 COMPREHENSIVE WIF DEBUGGING")
    print("=" * 70)
    
    # Step 1: Generate and analyze token
    token, claims = debug_token_generation()
    
    # Step 2: Check AWS expectations
    check_aws_role_trust_policy()
    
    # Step 3: Test Airflow behavior
    test_airflow_aws_provider()
    
    # Step 4: Compare token vs expectations
    analyze_token_vs_aws_expectations(claims)
    
    print("\n" + "=" * 70)
    print("🎯 LIKELY ROOT CAUSES")
    print("=" * 70)
    
    if claims:
        print("✅ Token generation works")
        print("❌ Issue is likely in AWS trust policy configuration")
        print("\n💡 CHECK:")
        print("1. AWS OIDC Identity Provider audiences list")
        print("2. AWS Role trust policy conditions")
        print("3. Account ID consistency")
        
        azp = claims.get('azp')
        aud = claims.get('aud')
        
        print(f"\n🔧 REQUIRED AWS TRUST POLICY:")
        print(f"""{{
  "Version": "2012-10-17",
  "Statement": [
    {{
      "Effect": "Allow",
      "Principal": {{
        "Federated": "arn:aws:iam::277108755423:oidc-provider/accounts.google.com"
      }},
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {{
        "StringEquals": {{
          "accounts.google.com:aud": "{azp}",
          "accounts.google.com:oaud": "{aud}"
        }}
      }}
    }}
  ]
}}""")
    else:
        print("❌ Token generation failed")
        print("💡 Check GCP permissions and service account configuration")

if __name__ == "__main__":
    main()