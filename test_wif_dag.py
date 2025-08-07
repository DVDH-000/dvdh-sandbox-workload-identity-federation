"""
Test DAG for WIF Connection
Deploy this to your Cloud Composer to test the AWS connection
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'test_wif_connection',
    default_args=default_args,
    description='Test WIF AWS connection',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'wif', 'aws']
)

def test_aws_connection_wif():
    """Test the AWS connection using WIF"""
    print("🧪 Testing AWS WIF Connection...")
    
    try:
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
        
        # Test the specific connection that's failing
        conn_id = "gcp_to_aws_s3_sandbox_davy"
        print(f"Testing connection: {conn_id}")
        
        # Create hook
        hook = AwsBaseHook(aws_conn_id=conn_id)
        session = hook.get_session()
        
        # Test STS call
        sts = session.client('sts')
        identity = sts.get_caller_identity()
        
        print("✅ SUCCESS! AWS WIF connection working!")
        print(f"Account: {identity['Account']}")
        print(f"ARN: {identity['Arn']}")
        print(f"User ID: {identity['UserId']}")
        
        # Test S3 access
        s3 = session.client('s3')
        buckets = s3.list_buckets()
        
        print(f"✅ S3 access working! Found {len(buckets['Buckets'])} buckets")
        for bucket in buckets['Buckets'][:3]:  # Show first 3
            print(f"  📦 {bucket['Name']}")
            
        return "SUCCESS"
        
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        
        # Specific guidance for common errors
        if "InvalidIdentityToken" in str(e):
            if "Incorrect token audience" in str(e):
                print("\n💡 DIAGNOSIS: Token audience mismatch")
                print("   Fix: Update connection Extra field with correct audience")
            elif "InvalidIdentityToken" in str(e):
                print("\n💡 DIAGNOSIS: Token validation failed")
                print("   Check: AWS role trust policy and OIDC provider config")
        
        raise

def test_service_account_impersonation():
    """Test that we can impersonate the target service account"""
    print("🤖 Testing Service Account Impersonation...")
    
    try:
        import google.auth
        from google.auth import impersonated_credentials
        from google.auth.transport import requests as gauth_requests
        import jwt
        
        # Get current credentials
        credentials, project = google.auth.default()
        print(f"Current project: {project}")
        
        # Target service account
        target_sa = "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
        
        # Test impersonation
        impersonated_creds = impersonated_credentials.Credentials(
            source_credentials=credentials,
            target_principal=target_sa,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        print(f"✅ Can impersonate: {target_sa}")
        
        # Test ID token generation
        id_token_credentials = impersonated_credentials.IDTokenCredentials(
            impersonated_creds,
            target_audience="sts.amazonaws.com",
            include_email=True
        )
        
        request = gauth_requests.Request()
        id_token_credentials.refresh(request)
        
        print("✅ Can generate ID tokens!")
        
        # Decode token to show claims
        token = id_token_credentials.token
        decoded = jwt.decode(token, options={"verify_signature": False})
        
        print("📋 Token claims:")
        for key, value in decoded.items():
            if key in ['aud', 'azp', 'sub', 'iss', 'email']:
                print(f"  🎯 {key}: {value}")
        
        return "SUCCESS"
        
    except Exception as e:
        print(f"❌ Impersonation test failed: {e}")
        raise

# Create tasks
test_aws_task = PythonOperator(
    task_id='test_aws_connection',
    python_callable=test_aws_connection_wif,
    dag=dag,
)

test_sa_task = PythonOperator(
    task_id='test_service_account_impersonation', 
    python_callable=test_service_account_impersonation,
    dag=dag,
)

# Set dependencies
test_sa_task >> test_aws_task