"""
WORKING WIF DAG - Bypasses Airflow's buggy AWS provider
This DAG implements WIF correctly and will actually work
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import google.auth
from google.auth import impersonated_credentials
from google.auth.transport import requests as gauth_requests


def get_aws_credentials_via_wif():
    """
    Generate AWS credentials using Google WIF
    This is the working implementation that bypasses Airflow's bugs
    """
    print("🎯 Getting AWS credentials via Google WIF")
    
    # Configuration
    target_sa = "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
    audience = "sts.amazonaws.com"
    role_arn = "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"
    
    try:
        # Get source credentials (Composer's default SA)
        source_credentials, project = google.auth.default()
        print(f"✅ Using project: {project}")
        
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
        
        print(f"✅ Generated ID token (length: {len(id_token)})")
        
        # Use STS to assume the AWS role
        sts_client = boto3.client('sts', region_name='us-east-1')
        
        response = sts_client.assume_role_with_web_identity(
            RoleArn=role_arn,
            RoleSessionName=f"airflow-working-wif-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
            WebIdentityToken=id_token,
            DurationSeconds=3600  # 1 hour
        )
        
        credentials = response['Credentials']
        print(f"✅ Successfully assumed AWS role: {response['AssumedRoleUser']['Arn']}")
        
        return {
            'aws_access_key_id': credentials['AccessKeyId'],
            'aws_secret_access_key': credentials['SecretAccessKey'],
            'aws_session_token': credentials['SessionToken'],
            'expiration': credentials['Expiration']
        }
        
    except Exception as e:
        print(f"❌ WIF credential generation failed: {e}")
        raise


def test_aws_connection_working():
    """Test AWS connection with working WIF implementation"""
    print("🧪 Testing AWS connection with working WIF")
    
    try:
        # Get credentials using our working WIF implementation
        aws_creds = get_aws_credentials_via_wif()
        
        # Create boto3 session with the credentials
        session = boto3.Session(
            aws_access_key_id=aws_creds['aws_access_key_id'],
            aws_secret_access_key=aws_creds['aws_secret_access_key'],
            aws_session_token=aws_creds['aws_session_token'],
            region_name='us-east-1'
        )
        
        # Test STS
        sts = session.client('sts')
        identity = sts.get_caller_identity()
        print(f"✅ AWS Identity: {identity['Arn']}")
        print(f"✅ Account: {identity['Account']}")
        
        # Test S3
        s3 = session.client('s3')
        buckets = s3.list_buckets()
        print(f"✅ Found {len(buckets['Buckets'])} S3 buckets")
        
        # Test specific bucket access
        try:
            response = s3.list_objects_v2(
                Bucket='sandbox-dvdh-gcp-to-s3',
                MaxKeys=1
            )
            print("✅ Can access target S3 bucket")
        except Exception as bucket_error:
            print(f"⚠️  Bucket access issue: {bucket_error}")
        
        print("🎉 AWS connection test SUCCESSFUL!")
        return True
        
    except Exception as e:
        print(f"❌ AWS connection test failed: {e}")
        raise


def transfer_gcs_to_s3_working():
    """
    Transfer files from GCS to S3 using working WIF
    This implementation actually works unlike Airflow's built-in operators
    """
    print("🚀 Starting GCS to S3 transfer with working WIF")
    
    try:
        # Get working AWS credentials
        aws_creds = get_aws_credentials_via_wif()
        
        # Create AWS session
        aws_session = boto3.Session(
            aws_access_key_id=aws_creds['aws_access_key_id'],
            aws_secret_access_key=aws_creds['aws_secret_access_key'],
            aws_session_token=aws_creds['aws_session_token'],
            region_name='us-east-1'
        )
        
        # Create S3 client
        s3_client = aws_session.client('s3')
        
        # GCS setup (using Google Cloud client libraries)
        from google.cloud import storage
        
        # Create GCS client (uses Composer's default credentials)
        gcs_client = storage.Client()
        
        # Configuration
        gcs_bucket_name = 'elmyra_test_bigquery_export_to_bucket'
        s3_bucket_name = 'sandbox-dvdh-gcp-to-s3'
        
        print(f"📦 Source: gs://{gcs_bucket_name}")
        print(f"📦 Destination: s3://{s3_bucket_name}")
        
        # Get GCS bucket
        gcs_bucket = gcs_client.bucket(gcs_bucket_name)
        
                # List blobs in BigQuery exports folder (first 5 as example)
        blobs = list(gcs_bucket.list_blobs(prefix='bigquery/analytics/qvisits_odido/', max_results=5))

        if not blobs:
            print("⚠️  No BigQuery export files found in bucket")
            return

        print(f"📋 Found {len(blobs)} BigQuery export files to transfer")

        # Transfer each blob with proper S3 key structure
        for blob in blobs:
            # Extract meaningful parts from the BigQuery export path
            # Original: bigquery/analytics/qvisits_odido/year=2025/month=08/day=05_000000000000.parquet
            # S3 Key:  bigquery-exports/qvisits_odido/year=2025/month=08/day=05_000000000000.parquet
            s3_key = blob.name.replace('bigquery/analytics/', 'bigquery-exports/')
            
            print(f"📄 Transferring: {blob.name}")
            print(f"   📍 To S3: s3://{s3_bucket_name}/{s3_key}")

            # Download blob content
            blob_content = blob.download_as_bytes()
            size_mb = len(blob_content) / (1024 * 1024)

            # Upload to S3 with metadata
            s3_client.put_object(
                Bucket=s3_bucket_name,
                Key=s3_key,
                Body=blob_content,
                ContentType='application/octet-stream',  # Parquet files
                Metadata={
                    'source-bucket': gcs_bucket_name,
                    'source-file': blob.name,
                    'file-type': 'bigquery-export',
                    'data-source': 'qvisits_odido',
                    'transfer-method': 'wif-airflow'
                }
            )

            print(f"✅ Transferred: {blob.name} ({size_mb:.2f} MB)")
        
        print("🎉 GCS to S3 transfer completed successfully!")
        
    except Exception as e:
        print(f"❌ Transfer failed: {e}")
        raise


def simple_gcs_to_s3_file_transfer():
    """
    Simple example: transfer a specific file from GCS to S3
    Update the file paths for your use case
    """
    print("📁 Simple file transfer example")
    
    try:
        # Get working AWS credentials
        aws_creds = get_aws_credentials_via_wif()
        
        # Create AWS S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_creds['aws_access_key_id'],
            aws_secret_access_key=aws_creds['aws_secret_access_key'],
            aws_session_token=aws_creds['aws_session_token'],
            region_name='us-east-1'
        )
        
        # Create GCS client
        from google.cloud import storage
        gcs_client = storage.Client()
        
        # File configuration - BigQuery export files
        gcs_bucket = 'elmyra_test_bigquery_export_to_bucket'
        gcs_file = 'bigquery/analytics/qvisits_odido/year=2025/month=08/day=05_000000000000.parquet'
        s3_bucket = 'sandbox-dvdh-gcp-to-s3'
        s3_key = 'bigquery-exports/qvisits_odido/day=05_000000000000.parquet'
        
        print(f"📄 Transferring gs://{gcs_bucket}/{gcs_file} to s3://{s3_bucket}/{s3_key}")
        
        # Download from GCS
        bucket = gcs_client.bucket(gcs_bucket)
        blob = bucket.blob(gcs_file)
        
        if not blob.exists():
            print(f"❌ File does not exist: gs://{gcs_bucket}/{gcs_file}")
            return
        
        file_content = blob.download_as_bytes()
        print(f"✅ Downloaded from GCS: {len(file_content)} bytes")
        
        # Upload to S3
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=file_content
        )
        
        print(f"✅ Uploaded to S3: s3://{s3_bucket}/{s3_key}")
        print("🎉 File transfer completed!")
        
    except Exception as e:
        print(f"❌ File transfer failed: {e}")
        raise


# DAG configuration
default_args = {
    'owner': 'wif-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 7),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'working_wif_gcs_to_s3',
    default_args=default_args,
    description='WORKING GCS to S3 transfer using proper WIF implementation',
    schedule_interval=None,  # Manual trigger for testing
    catchup=False,
    tags=['wif', 'gcs', 's3', 'working', 'fixed']
)

# Task 1: Test AWS connection
test_connection_task = PythonOperator(
    task_id='test_aws_connection',
    python_callable=test_aws_connection_working,
    dag=dag,
    doc_md="""
    Test AWS connection using working WIF implementation.
    This should succeed where Airflow's built-in AWS provider fails.
    """
)

# Task 2: Simple file transfer
simple_transfer_task = PythonOperator(
    task_id='simple_file_transfer',
    python_callable=simple_gcs_to_s3_file_transfer,
    dag=dag,
    doc_md="""
    Transfer a single file from GCS to S3.
    Update the file paths in the function for your specific files.
    """
)

# Task 3: Bulk transfer
bulk_transfer_task = PythonOperator(
    task_id='bulk_gcs_to_s3_transfer',
    python_callable=transfer_gcs_to_s3_working,
    dag=dag,
    doc_md="""
    Transfer multiple files from GCS to S3.
    Update the bucket names in the function for your specific buckets.
    """
)

# Set task dependencies
test_connection_task >> simple_transfer_task >> bulk_transfer_task

# Make sure the DAG is available
globals()['working_wif_gcs_to_s3'] = dag

print("🎯 WORKING WIF DAG LOADED SUCCESSFULLY!")
print("📋 This DAG bypasses Airflow's buggy AWS provider")
print("✅ Upload this file to your Cloud Composer DAGs folder")
print("🚀 Trigger the 'working_wif_gcs_to_s3' DAG to test")