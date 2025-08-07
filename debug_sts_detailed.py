#!/usr/bin/env python3
"""
Detailed STS debugging for WIF issues
Test different STS endpoints and configurations
"""

import boto3
import json
import time
from botocore.exceptions import ClientError
from google.auth import impersonated_credentials
from google.auth.transport import requests
import google.auth
import jwt

def get_impersonated_token():
    """Get impersonated token for AWS"""
    try:
        # Get source credentials
        source_credentials, project = google.auth.default()
        
        # Target service account
        target_sa = "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
        
        # Create impersonated credentials
        impersonated_creds = impersonated_credentials.Credentials(
            source_credentials=source_credentials,
            target_principal=target_sa,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        # Generate ID token for AWS
        id_token_credentials = impersonated_credentials.IDTokenCredentials(
            impersonated_creds,
            target_audience="sts.amazonaws.com",
            include_email=True
        )
        
        request = requests.Request()
        id_token_credentials.refresh(request)
        
        return id_token_credentials.token
        
    except Exception as e:
        print(f"❌ Error getting token: {e}")
        return None

def test_sts_regional_endpoints(token):
    """Test different STS regional endpoints"""
    print("\n🌍 TESTING REGIONAL STS ENDPOINTS")
    print("=" * 60)
    
    # Try different regions
    regions = ['us-east-1', 'eu-central-1', 'us-west-2']
    role_arn = "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"
    
    for region in regions:
        print(f"\n🌍 Testing region: {region}")
        try:
            # Create STS client for specific region
            sts_client = boto3.client('sts', region_name=region)
            
            response = sts_client.assume_role_with_web_identity(
                RoleArn=role_arn,
                RoleSessionName=f"airflow-debug-{region}",
                WebIdentityToken=token
            )
            
            print(f"✅ SUCCESS in {region}!")
            print(f"   ARN: {response['AssumedRoleUser']['Arn']}")
            return response['Credentials'], region
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            print(f"❌ Failed in {region}: {error_code} - {error_message}")
        
        except Exception as e:
            print(f"❌ Unexpected error in {region}: {e}")
    
    return None, None

def test_with_different_audiences():
    """Test with different audience values"""
    print("\n🎯 TESTING DIFFERENT AUDIENCE VALUES")
    print("=" * 60)
    
    try:
        # Get source credentials
        source_credentials, project = google.auth.default()
        target_sa = "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
        
        # Create impersonated credentials once
        impersonated_creds = impersonated_credentials.Credentials(
            source_credentials=source_credentials,
            target_principal=target_sa,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        # Test different audiences
        audiences = [
            "sts.amazonaws.com",
            "https://sts.amazonaws.com",
            "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test",
            "277108755423",  # Account ID
        ]
        
        role_arn = "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"
        
        for audience in audiences:
            print(f"\n🎯 Testing audience: {audience}")
            
            try:
                # Generate token with this audience
                id_token_credentials = impersonated_credentials.IDTokenCredentials(
                    impersonated_creds,
                    target_audience=audience,
                    include_email=True
                )
                
                request = requests.Request()
                id_token_credentials.refresh(request)
                token = id_token_credentials.token
                
                # Decode to see claims
                decoded = jwt.decode(token, options={"verify_signature": False})
                print(f"   Token aud: {decoded.get('aud')}")
                print(f"   Token azp: {decoded.get('azp')}")
                
                # Test AWS assume role
                sts_client = boto3.client('sts')
                response = sts_client.assume_role_with_web_identity(
                    RoleArn=role_arn,
                    RoleSessionName=f"test-{audience.replace(':', '-').replace('/', '-')[:20]}",
                    WebIdentityToken=token
                )
                
                print(f"✅ SUCCESS with audience: {audience}")
                return response['Credentials'], audience
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                error_message = e.response['Error']['Message']
                print(f"❌ Failed: {error_code} - {error_message}")
            
            except Exception as e:
                print(f"❌ Error: {e}")
        
        return None, None
        
    except Exception as e:
        print(f"❌ Error in audience testing: {e}")
        return None, None

def check_sts_endpoint_configuration():
    """Check if STS endpoints are configured correctly"""
    print("\n🔧 CHECKING STS ENDPOINT CONFIGURATION")
    print("=" * 60)
    
    try:
        # Get current AWS region
        session = boto3.Session()
        region = session.region_name or 'us-east-1'
        print(f"📍 Current AWS region: {region}")
        
        # Test if STS is available
        sts_client = boto3.client('sts')
        identity = sts_client.get_caller_identity()
        print(f"✅ STS working, current identity: {identity['Arn']}")
        
        # Check if we can access STS
        sts_client = boto3.client('sts', region_name='us-east-1')
        identity_global = sts_client.get_caller_identity()
        print(f"✅ Global STS working: {identity_global['Arn']}")
        
        return True
        
    except Exception as e:
        print(f"❌ STS configuration error: {e}")
        return False

def test_direct_aws_cli():
    """Test using AWS CLI directly"""
    print("\n🔧 TESTING WITH AWS CLI DIRECTLY")
    print("=" * 60)
    
    try:
        # Get token
        token = get_impersonated_token()
        if not token:
            print("❌ Cannot get token")
            return
        
        # Write token to temporary file
        import tempfile
        import subprocess
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write(token)
            token_file = f.name
        
        role_arn = "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"
        
        # Try AWS CLI command
        cmd = [
            'aws', 'sts', 'assume-role-with-web-identity',
            '--role-arn', role_arn,
            '--role-session-name', 'cli-test',
            '--web-identity-token', f'file://{token_file}',
            '--output', 'json'
        ]
        
        print(f"🔄 Running: {' '.join(cmd)}")
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        # Clean up
        import os
        os.unlink(token_file)
        
        if result.returncode == 0:
            print("✅ AWS CLI succeeded!")
            response = json.loads(result.stdout)
            print(f"   ARN: {response['AssumedRoleUser']['Arn']}")
            return True
        else:
            print(f"❌ AWS CLI failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing AWS CLI: {e}")
        return False

def check_cloudtrail_events():
    """Show how to check CloudTrail for debugging"""
    print("\n📊 CLOUDTRAIL DEBUGGING")
    print("=" * 60)
    
    print("🔍 To debug further, check CloudTrail for:")
    print("1. AssumeRoleWithWebIdentity events (successful and failed)")
    print("2. Event source: sts.amazonaws.com")
    print("3. Filter by role ARN: arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test")
    
    print("\n📝 AWS CLI command to check recent events:")
    print("""
aws logs filter-log-events \\
  --log-group-name CloudTrail/STS \\
  --start-time $(date -d '1 hour ago' +%s)000 \\
  --filter-pattern '{ $.eventName = "AssumeRoleWithWebIdentity" }'
""")

def main():
    """Main debugging function"""
    print("🔍 DETAILED STS WIF DEBUGGING")
    print("=" * 60)
    
    # Step 1: Check STS endpoint
    if not check_sts_endpoint_configuration():
        print("❌ STS endpoint issues, cannot continue")
        return
    
    # Step 2: Get token
    token = get_impersonated_token()
    if not token:
        print("❌ Cannot get token, cannot continue")
        return
    
    print(f"✅ Got token (length: {len(token)})")
    
    # Step 3: Test regional endpoints
    creds_regional, success_region = test_sts_regional_endpoints(token)
    
    if creds_regional:
        print(f"\n🎉 SUCCESS: Working with {success_region} endpoint!")
    else:
        print("\n❌ No regional endpoint worked")
        
        # Step 4: Try different audiences
        creds_audience, success_audience = test_with_different_audiences()
        
        if creds_audience:
            print(f"\n🎉 SUCCESS: Working with audience {success_audience}!")
        else:
            print("\n❌ No audience worked")
            
            # Step 5: Test AWS CLI
            cli_success = test_direct_aws_cli()
            
            if not cli_success:
                print("\n❌ AWS CLI also failed")
                
                # Step 6: Show CloudTrail debugging
                check_cloudtrail_events()
    
    print("\n" + "=" * 60)
    print("🎯 DEBUGGING SUMMARY")
    print("=" * 60)
    
    if creds_regional or creds_audience:
        print("✅ WIF is working with the correct configuration!")
        if success_region:
            print(f"💡 Use region {success_region} in your Airflow configuration")
        if success_audience:
            print(f"💡 Use audience '{success_audience}' in your Airflow configuration")
    else:
        print("❌ WIF is still not working")
        print("💡 Possible remaining issues:")
        print("   1. IAM role permissions (not trust policy)")
        print("   2. STS service availability in your region")
        print("   3. Network connectivity issues")
        print("   4. Token expiration or format issues")

if __name__ == "__main__":
    main()