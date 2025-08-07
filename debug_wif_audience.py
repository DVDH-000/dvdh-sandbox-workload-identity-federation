#!/usr/bin/env python3
"""
Debug script for Workload Identity Federation audience issues.
This script helps diagnose the "Incorrect token audience" error.
"""

import os
import json
import jwt
import google.auth
import google.oauth2.credentials
from google.auth.transport import requests
from google.auth import external_account
import boto3
from botocore.exceptions import ClientError

def debug_gcp_token():
    """Debug the GCP side - what token are we generating?"""
    print("=== GCP TOKEN ANALYSIS ===")
    
    # Load WIF credentials
    credentials_path = "/Users/davyvanderhorst/code/sandbox/dvdh-sandbox-workload-identity-federation/tina-wif-cred.json"
    
    if not os.path.exists(credentials_path):
        print(f"❌ WIF config file not found: {credentials_path}")
        return None
    
    with open(credentials_path, 'r') as f:
        wif_config = json.load(f)
    
    print(f"📄 WIF Config Audience: {wif_config.get('audience', 'NOT SET')}")
    print(f"📄 Subject Token Type: {wif_config.get('subject_token_type', 'NOT SET')}")
    print(f"📄 Service Account Impersonation: {wif_config.get('service_account_impersonation_url', 'NOT SET')}")
    
    # Try to get credentials
    try:
        creds, project = google.auth.default()
        print(f"✅ Successfully loaded credentials for project: {project}")
        
        # Refresh to get actual token
        creds.refresh(requests.Request())
        
        if hasattr(creds, '_source_credentials'):
            print("🔍 Found source credentials (WIF flow)")
            source_creds = creds._source_credentials
            if hasattr(source_creds, '_make_sts_request'):
                print("🎯 This uses STS token exchange")
        
        # Try to get the actual subject token if possible
        if isinstance(creds, external_account.Credentials):
            print("🆔 External account credentials detected")
            
            # Try to read the subject token file directly
            credential_source = wif_config.get('credential_source', {})
            token_file = credential_source.get('file')
            if token_file:
                try:
                    with open(token_file, 'r') as f:
                        subject_token = f.read().strip()
                    print(f"📋 Subject token length: {len(subject_token)}")
                    
                    # Try to decode the subject token if it's a JWT
                    if '.' in subject_token and len(subject_token.split('.')) == 3:
                        try:
                            decoded = jwt.decode(subject_token, options={"verify_signature": False})
                            print("🔓 Subject token claims:")
                            for key, value in decoded.items():
                                if key == 'aud':
                                    print(f"  🎯 aud (audience): {value}")
                                else:
                                    print(f"  📝 {key}: {value}")
                        except Exception as e:
                            print(f"❌ Failed to decode subject token: {e}")
                    else:
                        print("⚠️  Subject token doesn't look like a JWT")
                        
                except FileNotFoundError:
                    print(f"❌ Subject token file not found: {token_file}")
                except Exception as e:
                    print(f"❌ Error reading subject token: {e}")
        
        return creds
        
    except Exception as e:
        print(f"❌ Failed to load credentials: {e}")
        return None

def debug_aws_config():
    """Debug the AWS side - what audience does AWS expect?"""
    print("\n=== AWS OIDC CONFIGURATION ===")
    
    # You'll need to provide these values
    aws_account_id = "447027034964"  # From your WIF config audience
    oidc_provider_arn = None
    role_arn = os.getenv("ROLE_ARN")
    
    print(f"🔑 AWS Account ID: {aws_account_id}")
    print(f"🎭 Role ARN: {role_arn or 'NOT SET - Please set ROLE_ARN env var'}")
    
    if not role_arn:
        print("⚠️  Set ROLE_ARN environment variable to continue AWS debugging")
        return
    
    try:
        # Create IAM client to inspect the role
        iam = boto3.client('iam')
        
        # Extract role name from ARN
        role_name = role_arn.split('/')[-1]
        
        # Get role details
        role_response = iam.get_role(RoleName=role_name)
        assume_role_policy = role_response['Role']['AssumeRolePolicyDocument']
        
        print("📋 Role's Assume Role Policy:")
        print(json.dumps(assume_role_policy, indent=2))
        
        # Look for OIDC conditions
        for statement in assume_role_policy.get('Statement', []):
            if 'Condition' in statement:
                conditions = statement['Condition']
                print("\n🎯 Found conditions in assume role policy:")
                for condition_type, condition_value in conditions.items():
                    print(f"  {condition_type}: {condition_value}")
                    
                    # Look for audience-related conditions
                    if 'aud' in str(condition_value).lower():
                        print("  ⭐ This appears to be an audience condition!")
        
    except Exception as e:
        print(f"❌ Failed to analyze AWS role: {e}")
        print("💡 Make sure you have AWS credentials configured")

def test_sts_assume_role():
    """Test the actual STS assume role operation that's failing"""
    print("\n=== STS ASSUME ROLE TEST ===")
    
    role_arn = os.getenv("ROLE_ARN")
    if not role_arn:
        print("❌ ROLE_ARN environment variable not set")
        return
    
    # Try to read the subject token
    try:
        with open("dummy-token.txt", 'r') as f:
            web_identity_token = f.read().strip()
    except FileNotFoundError:
        print("❌ dummy-token.txt not found")
        return
    
    try:
        sts = boto3.client('sts')
        
        print(f"🎭 Attempting to assume role: {role_arn}")
        print(f"🆔 Using web identity token (length: {len(web_identity_token)})")
        
        response = sts.assume_role_with_web_identity(
            RoleArn=role_arn,
            RoleSessionName="debug-session",
            WebIdentityToken=web_identity_token
        )
        
        print("✅ Successfully assumed role!")
        print(f"🔑 Access Key: {response['Credentials']['AccessKeyId']}")
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        print(f"❌ STS Error: {error_code}")
        print(f"📝 Message: {error_message}")
        
        if error_code == "InvalidIdentityToken":
            print("\n💡 DIAGNOSIS:")
            print("   The token audience doesn't match what AWS expects.")
            print("   Check that:")
            print("   1. Your GCP WIF audience matches your AWS OIDC Identity Provider")
            print("   2. Your subject token has the correct 'aud' claim")
            print("   3. The AWS role trust policy expects the right audience")

def main():
    print("🔍 WIF AUDIENCE DEBUGGING TOOL")
    print("=" * 50)
    
    # Debug GCP side
    creds = debug_gcp_token()
    
    # Debug AWS side
    debug_aws_config()
    
    # Test the actual failing operation
    test_sts_assume_role()
    
    print("\n" + "=" * 50)
    print("🎯 NEXT STEPS:")
    print("1. Compare the 'aud' claim in your subject token with AWS expectations")
    print("2. Verify your AWS OIDC Identity Provider thumbprint and audience")
    print("3. Check your AWS role's trust policy conditions")
    print("4. Make sure your subject token is a valid JWT from the right issuer")

if __name__ == "__main__":
    main()