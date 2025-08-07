#!/usr/bin/env python3
"""
Comprehensive debug script for Airflow WIF impersonation issue
This identifies the exact problem with airflow SA -> tina-gcp-to-s3-sa -> AWS role chain
"""

import json
import base64
import sys
from datetime import datetime

def decode_service_account_info(sa_file_path):
    """Extract service account information"""
    try:
        with open(sa_file_path, 'r') as f:
            sa_data = json.load(f)
        
        print(f"📋 Service Account: {sa_data['client_email']}")
        print(f"🆔 Client ID: {sa_data['client_id']}")
        print(f"🏗️ Project ID: {sa_data['project_id']}")
        return sa_data
    except Exception as e:
        print(f"❌ Error reading SA file: {e}")
        return None

def check_impersonation_permissions():
    """Check if current credentials can impersonate target SA"""
    print("\n🎭 CHECKING IMPERSONATION PERMISSIONS")
    print("=" * 60)
    
    try:
        import google.auth
        from google.auth import impersonated_credentials
        from google.auth.transport import requests
        
        # Get current identity
        source_credentials, project = google.auth.default()
        print(f"✅ Current project: {project}")
        print(f"📧 Source credentials type: {type(source_credentials).__name__}")
        
        if hasattr(source_credentials, 'service_account_email'):
            print(f"🤖 Current SA: {source_credentials.service_account_email}")
        
        # Target service account to impersonate
        target_sa = "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
        
        print(f"\n🎯 Attempting to impersonate: {target_sa}")
        
        # Try to create impersonated credentials
        impersonated_creds = impersonated_credentials.Credentials(
            source_credentials=source_credentials,
            target_principal=target_sa,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        # Refresh to test permissions
        request = requests.Request()
        impersonated_creds.refresh(request)
        
        print("✅ Impersonation successful!")
        print(f"🎭 Impersonated SA: {impersonated_creds.service_account_email}")
        
        return impersonated_creds, target_sa
        
    except Exception as e:
        print(f"❌ Impersonation failed: {e}")
        
        # Check specific error types
        if "does not have permission" in str(e):
            print("\n💡 PERMISSION ISSUE DETECTED!")
            print("   Your current service account needs the role:")
            print("   roles/iam.serviceAccountTokenCreator")
            print("   on the target service account")
        
        return None, None

def generate_id_token_for_aws(impersonated_creds, target_sa):
    """Generate ID token for AWS with specific audience"""
    print("\n🎟️ GENERATING ID TOKEN FOR AWS")
    print("=" * 60)
    
    try:
        from google.auth import impersonated_credentials
        from google.auth.transport import requests
        import jwt
        
        # AWS audience - this is critical for the trust policy
        aws_audience = "sts.amazonaws.com"
        
        print(f"🎯 Target audience: {aws_audience}")
        
        # Create ID token credentials
        id_token_credentials = impersonated_credentials.IDTokenCredentials(
            impersonated_creds,
            target_audience=aws_audience,
            include_email=True
        )
        
        # Refresh to get the token
        request = requests.Request()
        id_token_credentials.refresh(request)
        
        token = id_token_credentials.token
        print(f"✅ ID token generated (length: {len(token)})")
        
        # Decode token to inspect claims
        decoded = jwt.decode(token, options={"verify_signature": False})
        
        print(f"\n📋 TOKEN CLAIMS:")
        critical_claims = ['iss', 'sub', 'aud', 'azp', 'email', 'exp', 'iat']
        for claim in critical_claims:
            if claim in decoded:
                value = decoded[claim]
                if claim == 'exp' or claim == 'iat':
                    # Convert timestamp to readable date
                    dt = datetime.fromtimestamp(value)
                    print(f"  🎯 {claim}: {value} ({dt})")
                else:
                    print(f"  🎯 {claim}: {value}")
        
        return token, decoded
        
    except Exception as e:
        print(f"❌ ID token generation failed: {e}")
        return None, None

def test_aws_assume_role(token, decoded_token):
    """Test AWS assume role with the generated token"""
    print("\n☁️ TESTING AWS ASSUME ROLE")
    print("=" * 60)
    
    try:
        import boto3
        from botocore.exceptions import ClientError
        
        # AWS role ARN from your setup
        role_arn = "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"
        
        print(f"🎯 Role ARN: {role_arn}")
        print(f"🎟️ Using token with sub: {decoded_token.get('sub')}")
        print(f"🎟️ Using token with azp: {decoded_token.get('azp')}")
        print(f"🎟️ Using token with aud: {decoded_token.get('aud')}")
        
        # Create STS client
        sts_client = boto3.client('sts')
        
        # Attempt to assume role
        response = sts_client.assume_role_with_web_identity(
            RoleArn=role_arn,
            RoleSessionName="airflow-debug-session",
            WebIdentityToken=token
        )
        
        print("✅ AWS assume role successful!")
        print(f"🆔 Assumed role ARN: {response['AssumedRoleUser']['Arn']}")
        print(f"⏰ Credentials expire: {response['Credentials']['Expiration']}")
        
        return response['Credentials']
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        print(f"❌ AWS assume role failed: {error_code}")
        print(f"📝 Error message: {error_message}")
        
        if "InvalidIdentityToken" in error_code:
            if "Incorrect token audience" in error_message:
                print("\n💡 AUDIENCE MISMATCH DETECTED!")
                print("   The AWS role trust policy expects a different audience.")
                analyze_audience_mismatch(decoded_token)
            else:
                print("\n💡 TOKEN VALIDATION ISSUE!")
                print("   The token itself is invalid or malformed.")
        
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None

def analyze_audience_mismatch(decoded_token):
    """Analyze the specific audience mismatch issue"""
    print("\n🔍 AUDIENCE MISMATCH ANALYSIS")
    print("=" * 60)
    
    print("📋 Expected AWS IAM Trust Policy Configuration:")
    print(f"""
    {{
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
              "accounts.google.com:aud": "{decoded_token.get('azp', 'MISSING')}"
            }}
          }}
        }}
      ]
    }}
    """)
    
    print("\n🎯 Key Points:")
    print(f"1. AWS maps 'accounts.google.com:aud' to the 'azp' claim: {decoded_token.get('azp')}")
    print(f"2. Your token's 'aud' claim is: {decoded_token.get('aud')}")
    print(f"3. Your token's 'azp' claim is: {decoded_token.get('azp')}")
    print("4. AWS trust policy should check 'azp' claim, not 'aud' claim")

def check_current_aws_trust_policy():
    """Show how to check the current AWS trust policy"""
    print("\n🔍 CHECK AWS TRUST POLICY")
    print("=" * 60)
    
    check_command = """
# Run this command to check your current AWS role trust policy:
aws iam get-role --role-name sandbox-dvdh-write-from-gcp-to-aws-test --query 'Role.AssumeRolePolicyDocument' --output json

# Or if you need to decode the URL-encoded policy:
aws iam get-role --role-name sandbox-dvdh-write-from-gcp-to-aws-test --query 'Role.AssumeRolePolicyDocument' --output text | python -c "import sys, urllib.parse, json; print(json.dumps(json.loads(urllib.parse.unquote(sys.stdin.read())), indent=2))"
"""
    
    print(check_command)

def main():
    """Main debugging workflow"""
    print("🚀 AIRFLOW WIF IMPERSONATION DEBUG")
    print("=" * 60)
    print("🎯 Goal: airflow SA -> impersonate tina-gcp-to-s3-sa -> assume AWS role")
    
    # Step 1: Check service account information
    print("\n📋 STEP 1: SERVICE ACCOUNT INFORMATION")
    print("-" * 40)
    
    airflow_sa = decode_service_account_info("/Users/davyvanderhorst/code/sandbox/dvdh-sandbox-workload-identity-federation/airflow-sa-key.json")
    tina_sa = decode_service_account_info("/Users/davyvanderhorst/code/sandbox/dvdh-sandbox-workload-identity-federation/tina-gcp-to-s3-sa-key.json")
    
    if not airflow_sa or not tina_sa:
        print("❌ Cannot proceed without service account information")
        return
    
    # Step 2: Check impersonation permissions
    print("\n📋 STEP 2: IMPERSONATION PERMISSIONS")
    print("-" * 40)
    
    impersonated_creds, target_sa = check_impersonation_permissions()
    
    if not impersonated_creds:
        print("\n💡 SOLUTION FOR IMPERSONATION ISSUE:")
        print("   Run this command to grant permissions:")
        print(f"   gcloud iam service-accounts add-iam-policy-binding {tina_sa['client_email']} \\")
        print(f"     --member='serviceAccount:{airflow_sa['client_email']}' \\")
        print("     --role='roles/iam.serviceAccountTokenCreator'")
        return
    
    # Step 3: Generate ID token
    print("\n📋 STEP 3: ID TOKEN GENERATION")
    print("-" * 40)
    
    token, decoded_token = generate_id_token_for_aws(impersonated_creds, target_sa)
    
    if not token:
        print("❌ Cannot proceed without valid ID token")
        return
    
    # Step 4: Test AWS assume role
    print("\n📋 STEP 4: AWS ASSUME ROLE")
    print("-" * 40)
    
    aws_credentials = test_aws_assume_role(token, decoded_token)
    
    # Step 5: Show policy check commands
    check_current_aws_trust_policy()
    
    # Summary
    print("\n" + "=" * 60)
    print("🎯 SUMMARY AND NEXT STEPS")
    print("=" * 60)
    
    if aws_credentials:
        print("✅ SUCCESS: The WIF chain is working correctly!")
        print("🎉 Your Airflow should be able to access AWS resources now.")
    else:
        print("❌ FAILURE: There's still an issue in the WIF chain.")
        print("📋 Most likely issues:")
        print("1. AWS IAM trust policy configuration")
        print("2. OIDC provider not set up correctly")
        print("3. Audience mismatch between GCP and AWS")

if __name__ == "__main__":
    main()