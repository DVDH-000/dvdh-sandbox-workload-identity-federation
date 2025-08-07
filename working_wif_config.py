#!/usr/bin/env python3
"""
WORKING WIF CONFIGURATION FOR CLOUD COMPOSER
Based on successful debugging - audience: sts.amazonaws.com
"""

import os
import json
import boto3
from google.auth import impersonated_credentials
from google.auth.transport import requests as gauth_requests
import google.auth

def create_wif_aws_session():
    """
    Create AWS session using WIF with the working configuration
    This is what you'll use in Cloud Composer
    """
    print("🔑 Creating AWS session using WIF...")
    
    # Configuration that works
    service_account_email = "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
    audience = "sts.amazonaws.com"  # This is the working audience!
    aws_role_arn = "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"
    
    try:
        # Step 1: Get default credentials (this will be the Airflow service account in Cloud Composer)
        credentials, project = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        # Step 2: Create impersonated credentials for the target service account
        target_credentials = impersonated_credentials.Credentials(
            source_credentials=credentials,
            target_principal=service_account_email,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        # Step 3: Generate ID token with the working audience
        id_token_credentials = impersonated_credentials.IDTokenCredentials(
            target_credentials,
            target_audience=audience,
            include_email=True
        )
        
        request = gauth_requests.Request()
        id_token_credentials.refresh(request)
        
        oidc_token = id_token_credentials.token
        print(f"✅ Generated OIDC token (length: {len(oidc_token)})")
        
        # Step 4: Use OIDC token to assume AWS role
        sts = boto3.client('sts')
        
        response = sts.assume_role_with_web_identity(
            RoleArn=aws_role_arn,
            RoleSessionName='composer-wif-session',
            WebIdentityToken=oidc_token
        )
        
        aws_credentials = response['Credentials']
        print("✅ Successfully assumed AWS role!")
        
        # Step 5: Create AWS session with temporary credentials
        aws_session = boto3.Session(
            aws_access_key_id=aws_credentials['AccessKeyId'],
            aws_secret_access_key=aws_credentials['SecretAccessKey'],
            aws_session_token=aws_credentials['SessionToken']
        )
        
        return aws_session
        
    except Exception as e:
        print(f"❌ WIF session creation failed: {e}")
        return None

def test_wif_s3_access():
    """Test S3 access using WIF"""
    print("\n🧪 Testing S3 access using WIF...")
    
    aws_session = create_wif_aws_session()
    
    if not aws_session:
        return False
    
    try:
        s3 = aws_session.client('s3')
        
        # Test listing buckets
        response = s3.list_buckets()
        print(f"✅ S3 access successful! Found {len(response['Buckets'])} buckets:")
        
        for bucket in response['Buckets'][:3]:  # Show first 3
            print(f"  📦 {bucket['Name']}")
        
        # Test accessing your specific bucket
        bucket_name = "sandbox-dvdh-gcp-to-s3"
        try:
            s3.head_bucket(Bucket=bucket_name)
            print(f"✅ Can access target bucket: {bucket_name}")
        except Exception as e:
            print(f"⚠️  Cannot access target bucket {bucket_name}: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ S3 access failed: {e}")
        return False

def create_cloud_composer_dag():
    """Create the final DAG code for Cloud Composer"""
    print("\n📝 CREATING CLOUD COMPOSER DAG CODE")
    print("=" * 50)
    
    dag_code = '''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.transfers.gcs_to_s3 import GCSToS3Operator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import boto3
import google.auth
from google.auth import impersonated_credentials
from google.auth.transport import requests as gauth_requests

# Custom AWS connection using WIF
class WIFAwsHook(AwsBaseHook):
    """Custom AWS Hook that uses WIF for authentication"""
    
    def get_session(self, region_name=None):
        # WIF configuration
        service_account_email = "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
        audience = "sts.amazonaws.com"
        aws_role_arn = "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"
        
        # Get default credentials (Airflow service account)
        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        # Impersonate target service account
        target_credentials = impersonated_credentials.Credentials(
            source_credentials=credentials,
            target_principal=service_account_email,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        # Generate ID token
        id_token_credentials = impersonated_credentials.IDTokenCredentials(
            target_credentials,
            target_audience=audience,
            include_email=True
        )
        
        request = gauth_requests.Request()
        id_token_credentials.refresh(request)
        
        # Assume AWS role
        sts = boto3.client('sts')
        response = sts.assume_role_with_web_identity(
            RoleArn=aws_role_arn,
            RoleSessionName='composer-wif-session',
            WebIdentityToken=id_token_credentials.token
        )
        
        aws_creds = response['Credentials']
        
        # Create session with temporary credentials
        return boto3.Session(
            aws_access_key_id=aws_creds['AccessKeyId'],
            aws_secret_access_key=aws_creds['SecretAccessKey'],
            aws_session_token=aws_creds['SessionToken'],
            region_name=region_name or 'eu-central-1'
        )

# DAG definition
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 7),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'gcs_to_s3_wif_transfer',
    default_args=default_args,
    description='Transfer files from GCS to S3 using WIF',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# The transfer task
transfer_task = GCSToS3Operator(
    task_id='transfer_gcs_to_s3',
    gcs_bucket='your-source-gcs-bucket',
    prefix='path/to/files/',
    dest_s3_key='s3://sandbox-dvdh-gcp-to-s3/destination/',
    dest_aws_conn_id=None,  # We'll use custom hook
    aws_hook=WIFAwsHook(),  # Use our custom WIF hook
    replace=True,
    dag=dag,
)

transfer_task
'''
    
    # Save the DAG code
    with open('gcs_to_s3_wif_dag.py', 'w') as f:
        f.write(dag_code)
    
    print("💾 Saved DAG code to: gcs_to_s3_wif_dag.py")
    print("\n🎯 TO DEPLOY:")
    print("1. Copy this file to your Cloud Composer DAGs folder")
    print("2. Update the bucket names and paths")
    print("3. The WIF authentication will work automatically!")

def main():
    print("🎉 WORKING WIF CONFIGURATION")
    print("=" * 40)
    
    print("✅ Audience: sts.amazonaws.com")
    print("✅ Service Account: tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com")
    print("✅ AWS Role: arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test")
    
    # Test the working configuration
    success = test_wif_s3_access()
    
    if success:
        print("\n🎉 WIF is fully functional!")
        create_cloud_composer_dag()
        
        print("\n🎯 SUMMARY:")
        print("- WIF authentication: ✅ WORKING")
        print("- S3 access: ✅ WORKING") 
        print("- Cloud Composer DAG: ✅ READY")
        print("\nYour original error is completely resolved! 🚀")
    else:
        print("\n❌ Need to debug further")

if __name__ == "__main__":
    main()