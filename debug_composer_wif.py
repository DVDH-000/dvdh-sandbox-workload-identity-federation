"""
Debug WIF specifically in Cloud Composer environment
This will run inside your Cloud Composer and show us exactly what's happening
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'debug',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 7),
    'email_on_failure': False,
    'retries': 0,
}

dag = DAG(
    'debug_composer_wif_issue',
    default_args=default_args,
    description='Debug WIF issue in Cloud Composer',
    schedule_interval=None,
    catchup=False,
    tags=['debug', 'wif']
)

def debug_connection_details():
    """Check the exact connection configuration in Composer"""
    print("🔍 DEBUGGING CONNECTION IN CLOUD COMPOSER")
    print("=" * 60)
    
    try:
        from airflow.models.connection import Connection
        from airflow.hooks.base import BaseHook
        import json
        
        conn_id = "gcp_to_aws_s3_sandbox_davy"
        
        # Get the connection
        conn = BaseHook.get_connection(conn_id)
        
        print(f"📋 Connection ID: {conn.conn_id}")
        print(f"🔗 Connection Type: {conn.conn_type}")
        print(f"🏠 Host: {conn.host or 'None'}")
        print(f"👤 Login: {conn.login or 'None'}")
        print(f"🔑 Password/Secret: {'SET' if conn.password else 'None'}")
        
        print(f"\n📝 Extra Configuration:")
        if conn.extra:
            try:
                extra = json.loads(conn.extra)
                for key, value in extra.items():
                    print(f"   {key}: {value}")
            except:
                print(f"   Raw Extra: {conn.extra}")
        else:
            print("   No extra configuration")
            
    except Exception as e:
        print(f"❌ Error getting connection: {e}")

def debug_token_generation_in_composer():
    """Debug token generation specifically in Cloud Composer"""
    print("\n🎯 DEBUGGING TOKEN GENERATION IN COMPOSER")
    print("=" * 60)
    
    try:
        import google.auth
        from google.auth import impersonated_credentials
        from google.auth.transport import requests as gauth_requests
        import jwt
        
        # Get current identity in Composer
        credentials, project = google.auth.default()
        print(f"✅ Current project: {project}")
        print(f"📧 Credentials type: {type(credentials).__name__}")
        
        if hasattr(credentials, 'service_account_email'):
            print(f"🤖 Current SA: {credentials.service_account_email}")
        
        # Target service account
        target_sa = "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
        audience = "sts.amazonaws.com"
        
        print(f"\n🎭 Impersonating: {target_sa}")
        print(f"🎯 Audience: {audience}")
        
        # Create impersonated credentials
        impersonated_creds = impersonated_credentials.Credentials(
            source_credentials=credentials,
            target_principal=target_sa,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        request = gauth_requests.Request()
        impersonated_creds.refresh(request)
        print("✅ Impersonation successful")
        
        # Generate ID token
        id_token_credentials = impersonated_credentials.IDTokenCredentials(
            impersonated_creds,
            target_audience=audience,
            include_email=True
        )
        
        id_token_credentials.refresh(request)
        token = id_token_credentials.token
        
        print(f"✅ Token generated (length: {len(token)})")
        
        # Decode token
        decoded = jwt.decode(token, options={"verify_signature": False})
        
        print(f"\n📋 TOKEN CLAIMS IN COMPOSER:")
        for key, value in decoded.items():
            if key in ['aud', 'azp', 'sub', 'iss', 'email']:
                print(f"  🎯 {key}: {value}")
        
        return token
        
    except Exception as e:
        print(f"❌ Token generation failed in Composer: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_aws_connection_step_by_step():
    """Test AWS connection step by step to see where it fails"""
    print("\n🧪 TESTING AWS CONNECTION STEP BY STEP")
    print("=" * 60)
    
    try:
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
        import boto3
        
        conn_id = "gcp_to_aws_s3_sandbox_davy"
        
        print(f"🔗 Creating hook for connection: {conn_id}")
        hook = AwsBaseHook(aws_conn_id=conn_id)
        
        print("✅ Hook created successfully")
        
        print("🎯 Getting session...")
        session = hook.get_session()
        
        print("✅ Session created successfully")
        
        print("🎭 Creating STS client...")
        sts = session.client('sts')
        
        print("✅ STS client created")
        
        print("👤 Calling get_caller_identity...")
        identity = sts.get_caller_identity()
        
        print("✅ AWS connection successful!")
        print(f"   Account: {identity['Account']}")
        print(f"   ARN: {identity['Arn']}")
        print(f"   User ID: {identity['UserId']}")
        
        return True
        
    except Exception as e:
        print(f"❌ AWS connection failed: {e}")
        print(f"   Error type: {type(e).__name__}")
        
        # Check if it's the audience error
        if "InvalidIdentityToken" in str(e) and "Incorrect token audience" in str(e):
            print("\n💡 AUDIENCE ERROR DETECTED!")
            print("   This means the token audience doesn't match AWS expectations")
            print("   Need to check what audience Airflow is actually sending")
        
        import traceback
        traceback.print_exc()
        return False

def compare_environments():
    """Compare what we think should happen vs what actually happens"""
    print("\n🔍 ENVIRONMENT COMPARISON")
    print("=" * 60)
    
    print("✅ EXPECTED CONFIGURATION:")
    print("   Service Account: tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com")
    print("   Token Audience: sts.amazonaws.com")
    print("   AWS Role: arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test")
    print("   Trust Policy Condition:")
    print("     accounts.google.com:aud = 111691016524440319722")
    print("     accounts.google.com:oaud = sts.amazonaws.com")
    
    print("\n❓ WHAT TO CHECK:")
    print("1. Is Airflow using a different audience than 'sts.amazonaws.com'?")
    print("2. Is there credential caching causing old tokens to be used?")
    print("3. Is the connection configuration actually what we think it is?")
    print("4. Are there multiple connections with similar names?")

def main_debug():
    """Main debugging function"""
    debug_connection_details()
    token = debug_token_generation_in_composer()
    test_result = test_aws_connection_step_by_step()
    compare_environments()
    
    print("\n" + "=" * 60)
    print("🎯 SUMMARY")
    print("=" * 60)
    
    if token and test_result:
        print("✅ Everything should be working - check for caching issues")
    elif token and not test_result:
        print("❌ Token generation works but AWS rejects it")
        print("💡 Trust policy or OIDC provider configuration issue")
    else:
        print("❌ Token generation failing in Composer environment")
        print("💡 Permission or configuration issue in Composer")

# Create the task
debug_task = PythonOperator(
    task_id='debug_composer_wif',
    python_callable=main_debug,
    dag=dag,
)