#!/usr/bin/env python3
"""
Custom AWS hook that bypasses Airflow's buggy WIF implementation
This generates the token correctly and creates boto3 sessions that work
"""

import boto3
import google.auth
from google.auth import impersonated_credentials
from google.auth.transport import requests as gauth_requests
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.configuration import conf
from airflow.utils.log.logging_mixin import LoggingMixin


class WorkingWifAwsHook(AwsBaseHook, LoggingMixin):
    """
    Custom AWS Hook that properly implements Google WIF
    Bypasses Airflow's broken WIF implementation
    """
    
    def __init__(self, aws_conn_id='aws_default', verify=None, region_name=None, client_type='boto3', **kwargs):
        super().__init__(aws_conn_id=aws_conn_id, verify=verify, region_name=region_name, client_type=client_type, **kwargs)
        self._cached_credentials = None
        self._cached_session = None
    
    def _get_wif_credentials(self):
        """Generate AWS credentials using Google WIF"""
        self.log.info("🎯 Generating AWS credentials using Google WIF")
        
        try:
            # Get connection config
            conn = self.get_connection(self.aws_conn_id)
            extra = conn.extra_dejson
            
            role_arn = extra.get('role_arn')
            if not role_arn:
                raise ValueError("role_arn must be specified in connection extra")
            
            # Target service account to impersonate
            target_sa = "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
            audience = "sts.amazonaws.com"
            
            self.log.info(f"🎭 Impersonating service account: {target_sa}")
            self.log.info(f"🎯 Using audience: {audience}")
            
            # Get source credentials (Composer's default SA)
            source_credentials, project = google.auth.default()
            
            # Create impersonated credentials
            impersonated_creds = impersonated_credentials.Credentials(
                source_credentials=source_credentials,
                target_principal=target_sa,
                target_scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            
            # Generate ID token for AWS
            id_token_credentials = impersonated_credentials.IDTokenCredentials(
                impersonated_creds,
                target_audience=audience,
                include_email=True
            )
            
            request = gauth_requests.Request()
            id_token_credentials.refresh(request)
            id_token = id_token_credentials.token
            
            self.log.info(f"✅ Generated ID token (length: {len(id_token)})")
            
            # Use STS to assume the AWS role
            sts_client = boto3.client('sts', region_name=self.region_name or 'us-east-1')
            
            response = sts_client.assume_role_with_web_identity(
                RoleArn=role_arn,
                RoleSessionName=f"airflow-wif-{hash(target_sa) % 1000000}",
                WebIdentityToken=id_token,
                DurationSeconds=3600  # 1 hour
            )
            
            credentials = response['Credentials']
            self.log.info(f"✅ Successfully assumed AWS role: {response['AssumedRoleUser']['Arn']}")
            
            return {
                'AccessKeyId': credentials['AccessKeyId'],
                'SecretAccessKey': credentials['SecretAccessKey'],
                'SessionToken': credentials['SessionToken'],
                'Expiration': credentials['Expiration']
            }
            
        except Exception as e:
            self.log.error(f"❌ WIF credential generation failed: {e}")
            raise
    
    def get_session(self, region_name=None):
        """Get boto3 session with WIF credentials"""
        if self._cached_session is None:
            self.log.info("🔧 Creating new boto3 session with WIF")
            
            # Get WIF credentials
            wif_creds = self._get_wif_credentials()
            
            # Create session with temporary credentials
            session = boto3.Session(
                aws_access_key_id=wif_creds['AccessKeyId'],
                aws_secret_access_key=wif_creds['SecretAccessKey'],
                aws_session_token=wif_creds['SessionToken'],
                region_name=region_name or self.region_name or 'us-east-1'
            )
            
            self._cached_session = session
            self.log.info("✅ Created boto3 session with WIF credentials")
        
        return self._cached_session
    
    def get_client_type(self, client_type=None, region_name=None, **kwargs):
        """Get AWS client with WIF credentials"""
        session = self.get_session(region_name=region_name)
        return session.client(client_type, **kwargs)
    
    def get_resource_type(self, resource_type, region_name=None, **kwargs):
        """Get AWS resource with WIF credentials"""
        session = self.get_session(region_name=region_name)
        return session.resource(resource_type, **kwargs)


def create_custom_wif_dag():
    """Create a DAG that uses the working WIF hook"""
    
    dag_code = '''"""
Working GCS to S3 transfer using custom WIF hook
This bypasses Airflow's buggy WIF implementation
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
import tempfile

# Import our custom hook
import sys
sys.path.append('/home/airflow/gcs/dags')
from custom_wif_hook import WorkingWifAwsHook

def transfer_gcs_to_s3():
    """Transfer files from GCS to S3 using working WIF"""
    
    # GCS setup
    gcs_hook = GCSHook()
    
    # AWS setup with our custom hook
    aws_hook = WorkingWifAwsHook(aws_conn_id='gcp_to_aws_s3_sandbox_davy')
    s3_client = aws_hook.get_client_type('s3')
    
    # Example transfer (adjust paths as needed)
    gcs_bucket = 'your-gcs-bucket-name'
    s3_bucket = 'sandbox-dvdh-gcp-to-s3'
    
    print(f"🔄 Starting transfer from gs://{gcs_bucket} to s3://{s3_bucket}")
    
    # List objects in GCS bucket
    objects = gcs_hook.list(bucket_name=gcs_bucket, prefix='', delimiter='')
    
    print(f"📋 Found {len(objects)} objects in GCS bucket")
    
    for obj_name in objects[:5]:  # Transfer first 5 objects as test
        print(f"📄 Transferring: {obj_name}")
        
        # Download from GCS to temp file
        with tempfile.NamedTemporaryFile() as temp_file:
            gcs_hook.download(
                bucket_name=gcs_bucket,
                object_name=obj_name,
                filename=temp_file.name
            )
            
            # Upload to S3
            s3_client.upload_file(
                temp_file.name,
                s3_bucket,
                obj_name
            )
            
            print(f"✅ Transferred: {obj_name}")
    
    print(f"🎉 Transfer completed successfully!")

def test_aws_connection():
    """Test AWS connection with custom WIF hook"""
    try:
        aws_hook = WorkingWifAwsHook(aws_conn_id='gcp_to_aws_s3_sandbox_davy')
        
        # Test STS
        sts_client = aws_hook.get_client_type('sts')
        identity = sts_client.get_caller_identity()
        print(f"✅ AWS Identity: {identity['Arn']}")
        
        # Test S3
        s3_client = aws_hook.get_client_type('s3')
        buckets = s3_client.list_buckets()
        print(f"✅ Found {len(buckets['Buckets'])} S3 buckets")
        
        return True
    except Exception as e:
        print(f"❌ Test failed: {e}")
        raise

default_args = {
    'owner': 'wif-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 7),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'working_gcs_to_s3_wif',
    default_args=default_args,
    description='Working GCS to S3 transfer with custom WIF hook',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['wif', 'gcs', 's3', 'working']
)

# Test connection first
test_task = PythonOperator(
    task_id='test_aws_connection',
    python_callable=test_aws_connection,
    dag=dag
)

# Transfer task
transfer_task = PythonOperator(
    task_id='transfer_gcs_to_s3',
    python_callable=transfer_gcs_to_s3,
    dag=dag
)

test_task >> transfer_task
'''
    
    return dag_code


def main():
    """Show how to use the custom WIF hook"""
    print("🚀 CUSTOM WIF HOOK SOLUTION")
    print("=" * 60)
    print("🎯 This bypasses Airflow's buggy WIF implementation")
    
    print("\n📋 SETUP INSTRUCTIONS:")
    print("=" * 40)
    print("1. Upload custom_wif_hook.py to your DAGs folder")
    print("2. Keep your existing connection config:")
    
    connection_config = {
        "role_arn": "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"
    }
    
    print("   Connection Extra JSON:")
    print(f"   {connection_config}")
    
    print("\n3. Use the custom hook in your DAGs instead of standard AWS hooks")
    
    print("\n📝 EXAMPLE DAG:")
    print("=" * 40)
    dag_code = create_custom_wif_dag()
    print("Save this as working_gcs_to_s3_wif.py in your DAGs folder:")
    print(dag_code)
    
    print("\n✅ ADVANTAGES:")
    print("=" * 40)
    print("1. Uses the EXACT same WIF logic that works manually")
    print("2. Bypasses Airflow's buggy AWS provider")
    print("3. Still uses proper service account impersonation")
    print("4. No static credentials needed")
    print("5. Works with all AWS services (S3, STS, etc.)")
    
    print("\n🔧 HOW IT WORKS:")
    print("=" * 40)
    print("1. Gets Composer's default service account credentials")
    print("2. Impersonates tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com")
    print("3. Generates OIDC token with audience: sts.amazonaws.com")
    print("4. Uses STS AssumeRoleWithWebIdentity to get temp AWS credentials")
    print("5. Creates boto3 session with temp credentials")
    print("6. All AWS operations use this working session")
    
    print("\n🎉 RESULT:")
    print("=" * 40)
    print("Your GCS to S3 transfers will work with proper WIF!")
    print("No static credentials, no Airflow provider bugs!")

if __name__ == "__main__":
    main()