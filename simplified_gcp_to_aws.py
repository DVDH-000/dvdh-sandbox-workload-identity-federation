#!/usr/bin/env python3
"""
Simplified GCP to AWS authentication for GCS to S3 transfers
This approach works with Cloud Composer/Airflow without complex IAM setup
"""

import os
import json
import boto3
import google.auth
from google.auth.transport import requests
from botocore.exceptions import NoCredentialsError, ClientError

def get_gcp_access_token():
    """Get a GCP access token using Application Default Credentials"""
    print("🔑 Getting GCP access token...")
    
    try:
        # Get default credentials
        credentials, project = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        # Refresh to get the token
        request = requests.Request()
        credentials.refresh(request)
        
        return credentials.token, project
    except Exception as e:
        print(f"❌ Failed to get GCP token: {e}")
        return None, None

def create_aws_session_from_env():
    """
    Create AWS session using environment variables for cross-cloud access
    This is the recommended approach for Cloud Composer
    """
    print("🔧 Creating AWS session from environment variables...")
    
    # Check if AWS credentials are available in environment
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_session_token = os.getenv('AWS_SESSION_TOKEN')
    
    if aws_access_key and aws_secret_key:
        print("✅ Found AWS credentials in environment")
        session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            aws_session_token=aws_session_token
        )
        return session
    else:
        print("⚠️  No AWS credentials found in environment")
        return None

def test_gcs_access():
    """Test GCS access using GCP credentials"""
    print("📦 Testing GCS access...")
    
    try:
        from google.cloud import storage
        
        # Initialize the client
        client = storage.Client()
        
        # List some buckets (this requires basic permissions)
        buckets = list(client.list_buckets())
        
        print(f"✅ GCS access works! Found {len(buckets)} buckets:")
        for bucket in buckets[:5]:  # Show first 5 buckets
            print(f"  📦 {bucket.name}")
            
        return True
    except Exception as e:
        print(f"❌ GCS access failed: {e}")
        return False

def test_s3_access(aws_session):
    """Test S3 access using AWS session"""
    if not aws_session:
        print("❌ No AWS session available")
        return False
    
    print("📦 Testing S3 access...")
    
    try:
        s3 = aws_session.client('s3')
        
        # List buckets
        response = s3.list_buckets()
        
        print(f"✅ S3 access works! Found {len(response['Buckets'])} buckets:")
        for bucket in response['Buckets'][:5]:  # Show first 5 buckets
            print(f"  📦 {bucket['Name']}")
            
        return True
    except Exception as e:
        print(f"❌ S3 access failed: {e}")
        return False

def show_airflow_setup_instructions():
    """Show instructions for setting up this authentication in Cloud Composer/Airflow"""
    print("\n" + "="*60)
    print("🎯 CLOUD COMPOSER/AIRFLOW SETUP INSTRUCTIONS")
    print("="*60)
    
    print("\n📋 For your GCSToS3Operator to work, you need:")
    
    print("\n1. 🔐 AWS Credentials in Airflow:")
    print("   - Add AWS Connection in Airflow UI:")
    print("   - Connection ID: 'aws_default' (or custom)")
    print("   - Connection Type: Amazon Web Services")
    print("   - Access Key ID: <your-aws-access-key>")
    print("   - Secret Access Key: <your-aws-secret-key>")
    
    print("\n2. 🌐 Alternative: Environment Variables")
    print("   - Set in Cloud Composer environment:")
    print("   - AWS_ACCESS_KEY_ID=<your-key>")
    print("   - AWS_SECRET_ACCESS_KEY=<your-secret>")
    
    print("\n3. 🎭 For WIF (Advanced):")
    print("   - Your Cloud Composer service account needs:")
    print("   - 'roles/iam.serviceAccountTokenCreator' on target SA")
    print("   - Custom connection with WIF credentials")
    
    print("\n4. 📝 Your Operator Configuration:")
    print("""
    transfer_op = GCSToS3Operator(
        task_id='gcs_to_s3_transfer',
        gcs_bucket='your-gcs-bucket',
        prefix='path/to/files',
        dest_s3_key='s3://your-s3-bucket/path/',
        dest_aws_conn_id='aws_default',  # Your AWS connection ID
        google_impersonation_chain=None,  # Or your service account
        replace=True
    )
    """)

def main():
    print("🚀 GCP TO AWS AUTHENTICATION CHECK")
    print("=" * 50)
    
    # Test GCP access
    gcp_token, project = get_gcp_access_token()
    if gcp_token:
        print(f"✅ GCP authentication successful for project: {project}")
        test_gcs_access()
    else:
        print("❌ GCP authentication failed")
    
    print("\n" + "-"*50)
    
    # Test AWS access
    aws_session = create_aws_session_from_env()
    test_s3_access(aws_session)
    
    # Show setup instructions
    show_airflow_setup_instructions()

if __name__ == "__main__":
    main()