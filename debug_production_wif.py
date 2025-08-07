#!/usr/bin/env python3
"""
Production WIF Debugging - Check everything in your actual Cloud Composer environment
"""

import os
import json
import google.auth
from google.auth import impersonated_credentials
from google.auth.transport import requests as gauth_requests
import jwt

def check_environment():
    """Check the current environment and authentication"""
    print("🔍 ENVIRONMENT CHECK")
    print("=" * 50)
    
    # Check if we're in Cloud Composer
    composer_env = os.getenv('COMPOSER_ENVIRONMENT')
    if composer_env:
        print(f"✅ Running in Cloud Composer: {composer_env}")
    else:
        print("⚠️  Not in Cloud Composer environment")
    
    # Check current identity
    try:
        credentials, project = google.auth.default()
        print(f"✅ Authenticated to project: {project}")
        print(f"📧 Credentials type: {type(credentials).__name__}")
        
        # Try to get service account email
        if hasattr(credentials, 'service_account_email'):
            print(f"🤖 Current service account: {credentials.service_account_email}")
            return credentials.service_account_email, project
        else:
            print("👤 Using user credentials or ADC")
            return None, project
            
    except Exception as e:
        print(f"❌ Authentication error: {e}")
        return None, None

def check_service_account_permissions():
    """Check if we have the required permissions on the target service account"""
    print("\n🔑 PERMISSION CHECK")
    print("=" * 50)
    
    target_sa = "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
    
    # Test 1: Can we impersonate for access tokens?
    try:
        credentials, _ = google.auth.default()
        
        impersonated_creds = impersonated_credentials.Credentials(
            source_credentials=credentials,
            target_principal=target_sa,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        request = gauth_requests.Request()
        impersonated_creds.refresh(request)
        
        print(f"✅ Can impersonate {target_sa} for access tokens")
        
    except Exception as e:
        print(f"❌ Cannot impersonate for access tokens: {e}")
        return False
    
    # Test 2: Can we generate ID tokens?
    try:
        id_token_credentials = impersonated_credentials.IDTokenCredentials(
            impersonated_creds,
            target_audience="sts.amazonaws.com",
            include_email=True
        )
        
        id_token_credentials.refresh(request)
        
        print(f"✅ Can generate ID tokens for {target_sa}")
        print(f"🎯 Test token audience: sts.amazonaws.com")
        
        # Decode the token to check claims
        token = id_token_credentials.token
        decoded = jwt.decode(token, options={"verify_signature": False})
        
        print(f"\n📋 Generated Token Claims:")
        for key, value in decoded.items():
            if key in ['aud', 'azp', 'sub', 'iss', 'email']:
                print(f"  🎯 {key}: {value}")
        
        return True
        
    except Exception as e:
        print(f"❌ Cannot generate ID tokens: {e}")
        print("💡 You may need the 'serviceAccountOpenIdTokenCreator' role")
        return False

def check_airflow_connections():
    """Check Airflow connection configuration"""
    print("\n🔌 AIRFLOW CONNECTION CHECK")
    print("=" * 50)
    
    # Try to import Airflow and check connections
    try:
        from airflow.models import Connection
        from airflow.settings import Session
        
        session = Session()
        
        # Check for AWS connections
        aws_connections = session.query(Connection).filter(
            Connection.conn_type == 'aws'
        ).all()
        
        print(f"Found {len(aws_connections)} AWS connections:")
        
        for conn in aws_connections:
            print(f"\n🔗 Connection: {conn.conn_id}")
            print(f"   Type: {conn.conn_type}")
            print(f"   Host: {conn.host or 'None'}")
            print(f"   Login: {conn.login or 'None'}")
            
            if conn.extra:
                try:
                    extra = json.loads(conn.extra)
                    print(f"   Extra config:")
                    for key, value in extra.items():
                        if 'role' in key.lower() or 'assume' in key.lower() or 'audience' in key.lower():
                            print(f"     🎯 {key}: {value}")
                        else:
                            print(f"     📝 {key}: {value}")
                except:
                    print(f"   Extra: {conn.extra}")
        
        session.close()
        
    except ImportError:
        print("❌ Cannot import Airflow - not in Airflow environment")
    except Exception as e:
        print(f"❌ Error checking connections: {e}")

def test_specific_connection(conn_id):
    """Test a specific AWS connection"""
    print(f"\n🧪 TESTING CONNECTION: {conn_id}")
    print("=" * 50)
    
    try:
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
        
        # Create hook with the specific connection
        hook = AwsBaseHook(aws_conn_id=conn_id)
        
        print(f"✅ Hook created for connection: {conn_id}")
        
        # Try to get session
        session = hook.get_session()
        print(f"✅ Session created")
        
        # Try to create STS client and call get_caller_identity
        sts = session.client('sts')
        identity = sts.get_caller_identity()
        
        print(f"✅ STS call successful!")
        print(f"   Account: {identity.get('Account')}")
        print(f"   ARN: {identity.get('Arn')}")
        print(f"   UserId: {identity.get('UserId')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        
        # If it's the audience error, show details
        if "InvalidIdentityToken" in str(e) and "Incorrect token audience" in str(e):
            print("\n💡 DIAGNOSIS: Incorrect token audience error")
            print("   This means the OIDC token audience doesn't match AWS expectations")
            print("   Check your connection's 'assume_role_with_web_identity_federation_audience' setting")
        
        return False

def show_correct_configuration():
    """Show the correct configuration for the failing connection"""
    print("\n🔧 CORRECT CONFIGURATION")
    print("=" * 50)
    
    print("For connection 'gcp_to_aws_s3_sandbox_davy', the Extra field should be:")
    
    correct_config = {
        "role_arn": "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test",
        "assume_role_method": "assume_role_with_web_identity",
        "assume_role_with_web_identity_federation": "google",
        "assume_role_with_web_identity_federation_audience": "sts.amazonaws.com"
    }
    
    print(json.dumps(correct_config, indent=2))
    
    print("\n📝 Steps to fix:")
    print("1. Go to Airflow UI > Admin > Connections")
    print("2. Edit connection 'gcp_to_aws_s3_sandbox_davy'")
    print("3. Clear AWS Access Key ID and Secret Access Key fields")
    print("4. Replace the Extra field with the JSON above")
    print("5. Save the connection")

def main():
    print("🚀 PRODUCTION WIF DEBUGGING")
    print("=" * 70)
    
    # Step 1: Check environment
    current_sa, project = check_environment()
    
    # Step 2: Check permissions
    has_permissions = check_service_account_permissions()
    
    # Step 3: Check Airflow connections
    check_airflow_connections()
    
    # Step 4: Test the specific failing connection
    failing_conn = "gcp_to_aws_s3_sandbox_davy"
    test_result = test_specific_connection(failing_conn)
    
    # Step 5: Show correct configuration
    if not test_result:
        show_correct_configuration()
    
    print("\n" + "=" * 70)
    print("🎯 SUMMARY")
    print("=" * 70)
    
    if has_permissions and test_result:
        print("✅ Everything looks good! WIF should be working.")
    else:
        print("❌ Issues found:")
        if not has_permissions:
            print("   - Missing ID token generation permissions")
        if not test_result:
            print("   - AWS connection misconfigured")
        
        print("\n💡 Next steps:")
        print("1. Fix the connection configuration as shown above")
        print("2. Ensure Terraform has applied the OpenIdTokenCreator role")
        print("3. Retry your DAG task")

if __name__ == "__main__":
    main()