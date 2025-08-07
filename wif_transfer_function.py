#!/usr/bin/env python3
"""
WIF Transfer Function for BigQuery Export DAG
This replaces the problematic GCSToS3Operator with our working WIF implementation
"""

import boto3
import google.auth
from google.auth import impersonated_credentials
from google.auth.transport import requests as gauth_requests
from google.cloud import storage
from datetime import datetime


def get_aws_credentials_via_wif(service_account_to_impersonate):
    """
    Generate AWS credentials using Google WIF with configurable service account
    """
    print("🎯 Getting AWS credentials via Google WIF")
    
    # Configuration - these should match your setup
    target_sa = service_account_to_impersonate
    audience = "sts.amazonaws.com"
    role_arn = "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"
    
    try:
        # Get source credentials (Composer's default SA)
        source_credentials, project = google.auth.default()
        print(f"✅ Using project: {project}")
        
        # Log source service account details
        if hasattr(source_credentials, 'service_account_email'):
            source_sa = source_credentials.service_account_email
            print(f"🔑 Source service account: {source_sa}")
        elif hasattr(source_credentials, '_service_account_email'):
            source_sa = source_credentials._service_account_email
            print(f"🔑 Source service account: {source_sa}")
        else:
            print(f"🔑 Source credential type: {type(source_credentials).__name__}")

        print(f"🎭 Target service account for impersonation: {target_sa}")
        print(f"🎯 AWS role to assume: {role_arn}")

        # Create impersonated credentials
        print("🔄 Creating impersonated credentials...")
        impersonated_creds = impersonated_credentials.Credentials(
            source_credentials=source_credentials,
            target_principal=target_sa,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        print(f"✅ Successfully created impersonated credentials for: {target_sa}")

        # Generate ID token for AWS
        print("🪪 Generating ID token for AWS STS...")
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
        print("🔄 Creating STS client and assuming AWS role...")
        sts_client = boto3.client('sts', region_name='us-east-1')
        
        session_name = f"airflow-bigquery-export-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        print(f"🏷️  Session name: {session_name}")

        response = sts_client.assume_role_with_web_identity(
            RoleArn=role_arn,
            RoleSessionName=session_name,
            WebIdentityToken=id_token,
            DurationSeconds=3600  # 1 hour
        )

        credentials = response['Credentials']
        assumed_role_arn = response['AssumedRoleUser']['Arn']
        
        print(f"✅ Successfully assumed AWS role!")
        print(f"🎭 Assumed role ARN: {assumed_role_arn}")
        print(f"🔑 Access key ID: {credentials['AccessKeyId']}")

        return {
            'aws_access_key_id': credentials['AccessKeyId'],
            'aws_secret_access_key': credentials['SecretAccessKey'],
            'aws_session_token': credentials['SessionToken'],
            'expiration': credentials['Expiration']
        }

    except Exception as e:
        print(f"❌ WIF credential generation failed: {e}")
        raise


def transfer_gcs_to_s3_working(gcs_bucket, gcs_prefix, s3_bucket, s3_prefix, service_account_to_impersonate, replace=True):
    """
    Transfer files from GCS to S3 using working WIF implementation
    This replaces the problematic GCSToS3Operator
    
    Args:
        gcs_bucket: Source GCS bucket name
        gcs_prefix: Prefix pattern to match files in GCS
        s3_bucket: Destination S3 bucket name  
        s3_prefix: Destination prefix in S3
        service_account_to_impersonate: Service account for AWS access
        replace: Whether to overwrite existing files
    """
    print(f"🚀 Starting GCS to S3 transfer with working WIF")
    print(f"📦 Source: gs://{gcs_bucket}/{gcs_prefix}")
    print(f"📦 Destination: s3://{s3_bucket}/{s3_prefix}")
    
    try:
        # Get working AWS credentials
        aws_creds = get_aws_credentials_via_wif(service_account_to_impersonate)

        # Create AWS session
        aws_session = boto3.Session(
            aws_access_key_id=aws_creds['aws_access_key_id'],
            aws_secret_access_key=aws_creds['aws_secret_access_key'],
            aws_session_token=aws_creds['aws_session_token'],
            region_name='us-east-1'
        )

        # Create S3 client
        s3_client = aws_session.client('s3')

        # Create GCS client (uses Composer's default credentials)
        gcs_client = storage.Client()

        # Get GCS bucket
        gcs_bucket_obj = gcs_client.bucket(gcs_bucket)

        # List blobs matching the prefix
        blobs = list(gcs_bucket_obj.list_blobs(prefix=gcs_prefix))

        if not blobs:
            print(f"⚠️  No files found with prefix: {gcs_prefix}")
            return

        print(f"📋 Found {len(blobs)} files to transfer")

        transferred_count = 0
        total_size = 0

        # Transfer each blob
        for blob in blobs:
            # Create S3 key by replacing GCS prefix with S3 prefix
            relative_path = blob.name[len(gcs_prefix):].lstrip('/')
            s3_key = f"{s3_prefix.rstrip('/')}/{relative_path}" if relative_path else s3_prefix.rstrip('/')
            
            print(f"📄 Transferring: {blob.name}")
            print(f"   📍 To S3: s3://{s3_bucket}/{s3_key}")

            # Check if file exists in S3 and skip if replace=False
            if not replace:
                try:
                    s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
                    print(f"   ⏭️  File exists in S3, skipping (replace=False)")
                    continue
                except s3_client.exceptions.NoSuchKey:
                    pass  # File doesn't exist, proceed with transfer

            # Download blob content
            blob_content = blob.download_as_bytes()
            size_mb = len(blob_content) / (1024 * 1024)
            total_size += len(blob_content)

            # Determine content type
            content_type = blob.content_type or 'application/octet-stream'
            
            # For BigQuery exports, set appropriate content types
            if blob.name.endswith('.parquet'):
                content_type = 'application/octet-stream'
            elif blob.name.endswith('.json'):
                content_type = 'application/json'
            elif blob.name.endswith('.csv'):
                content_type = 'text/csv'
            elif blob.name.endswith('.avro'):
                content_type = 'application/avro'

            # Upload to S3 with metadata
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=blob_content,
                ContentType=content_type,
                Metadata={
                    'source-bucket': gcs_bucket,
                    'source-file': blob.name,
                    'transfer-method': 'wif-airflow',
                    'transfer-time': datetime.now().isoformat(),
                    'file-size': str(len(blob_content)),
                    'bigquery-export': 'true'
                }
            )

            print(f"   ✅ Transferred: {size_mb:.2f} MB")
            transferred_count += 1

        total_size_mb = total_size / (1024 * 1024)
        print(f"🎉 GCS to S3 transfer completed successfully!")
        print(f"📊 Transferred {transferred_count} files, total size: {total_size_mb:.2f} MB")

    except Exception as e:
        print(f"❌ Transfer failed: {e}")
        raise


def create_wif_transfer_task(task_id, gcs_bucket, gcs_prefix, s3_bucket, s3_prefix, service_account, replace=True):
    """
    Create a PythonOperator task that uses our working WIF implementation
    This replaces GCSToS3Operator
    """
    from airflow.operators.python import PythonOperator
    
    def transfer_task():
        transfer_gcs_to_s3_working(
            gcs_bucket=gcs_bucket,
            gcs_prefix=gcs_prefix, 
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            service_account_to_impersonate=service_account,
            replace=replace
        )
    
    return PythonOperator(
        task_id=task_id,
        python_callable=transfer_task,
        doc_md=f"""
        Transfer files from GCS to S3 using working WIF implementation.
        
        Source: gs://{gcs_bucket}/{gcs_prefix}
        Destination: s3://{s3_bucket}/{s3_prefix}
        Service Account: {service_account}
        Replace existing: {replace}
        
        This task bypasses Airflow's buggy GCSToS3Operator and AWS provider.
        """
    )