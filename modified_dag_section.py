#!/usr/bin/env python3
"""
Modified section of your BigQuery export DAG to use working WIF implementation
Replace the create_transfer_operators function with this version
"""

# Add these imports at the top of your DAG file
from airflow.operators.python import PythonOperator
import boto3
import google.auth
from google.auth import impersonated_credentials
from google.auth.transport import requests as gauth_requests
from google.cloud import storage
from datetime import datetime

# Add the WIF functions (copy from wif_transfer_function.py)
def get_aws_credentials_via_wif(service_account_to_impersonate):
    """Generate AWS credentials using Google WIF with configurable service account"""
    print("🎯 Getting AWS credentials via Google WIF")
    
    target_sa = service_account_to_impersonate
    audience = "sts.amazonaws.com"
    role_arn = "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"
    
    try:
        source_credentials, project = google.auth.default()
        print(f"✅ Using project: {project}")
        
        if hasattr(source_credentials, 'service_account_email'):
            source_sa = source_credentials.service_account_email
            print(f"🔑 Source service account: {source_sa}")
        elif hasattr(source_credentials, '_service_account_email'):
            source_sa = source_credentials._service_account_email
            print(f"🔑 Source service account: {source_sa}")

        print(f"🎭 Target service account for impersonation: {target_sa}")
        print(f"🎯 AWS role to assume: {role_arn}")

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
        
        session_name = f"airflow-bigquery-export-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

        response = sts_client.assume_role_with_web_identity(
            RoleArn=role_arn,
            RoleSessionName=session_name,
            WebIdentityToken=id_token,
            DurationSeconds=3600
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


def transfer_gcs_to_s3_working(gcs_bucket, gcs_prefix, s3_bucket, s3_prefix, service_account_to_impersonate, replace=True):
    """Transfer files from GCS to S3 using working WIF implementation"""
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

        s3_client = aws_session.client('s3')
        gcs_client = storage.Client()
        gcs_bucket_obj = gcs_client.bucket(gcs_bucket)

        # List blobs matching the prefix
        blobs = list(gcs_bucket_obj.list_blobs(prefix=gcs_prefix))

        if not blobs:
            print(f"⚠️  No files found with prefix: {gcs_prefix}")
            return

        print(f"📋 Found {len(blobs)} files to transfer")

        transferred_count = 0
        total_size = 0

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
                    pass

            # Download and upload
            blob_content = blob.download_as_bytes()
            size_mb = len(blob_content) / (1024 * 1024)
            total_size += len(blob_content)

            # Set appropriate content type for BigQuery exports
            content_type = 'application/octet-stream'
            if blob.name.endswith('.json'):
                content_type = 'application/json'
            elif blob.name.endswith('.csv'):
                content_type = 'text/csv'
            elif blob.name.endswith('.parquet'):
                content_type = 'application/octet-stream'

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
                    'bigquery-export': 'true'
                }
            )

            print(f"   ✅ Transferred: {size_mb:.2f} MB")
            transferred_count += 1

        total_size_mb = total_size / (1024 * 1024)
        print(f"🎉 Transfer completed! {transferred_count} files, {total_size_mb:.2f} MB total")

    except Exception as e:
        print(f"❌ Transfer failed: {e}")
        raise


# REPLACE your existing create_transfer_operators function with this:
def create_transfer_operators(tables, dest_type, path_pref, bucket, temp_bucket, service_account, aws_connection_id):
    """Create transfer operators using working WIF implementation instead of GCSToS3Operator"""
    if dest_type != "s3":
        return []
        
    transfer_operators = []
    
    for idx, table_conf in enumerate(tables):
        source_dataset = table_conf["source_dataset"]
        source_table = table_conf["source_table"]
        export_format = table_conf.get("export_format", "NEWLINE_DELIMITED_JSON")
        overwrite = table_conf.get("overwrite", True)

        # Set file extension
        format_extensions = {
            "NEWLINE_DELIMITED_JSON": "json",
            "CSV": "csv",
            "AVRO": "avro",
            "PARQUET": "parquet"
        }
        file_extension = format_extensions[export_format]

        # Construct destination path
        table_path = table_conf.get("destination_path", f"{source_dataset}/{source_table}")
        if path_pref:
            full_destination_path = f"{path_pref}/{table_path}"
        else:
            full_destination_path = table_path

        # Create unique task ID
        task_id_base = f"transfer_{source_dataset}_{source_table}_{idx}"
        task_id = sanitize_task_id(task_id_base)
        
        # Create transfer task using our working WIF implementation
        def create_transfer_task(gcs_bucket=temp_bucket, 
                               gcs_prefix=f"{full_destination_path}_",
                               s3_bucket=bucket, 
                               s3_prefix=f"{full_destination_path}_",
                               sa=service_account,
                               replace_files=overwrite):
            def transfer_task():
                transfer_gcs_to_s3_working(
                    gcs_bucket=gcs_bucket,
                    gcs_prefix=gcs_prefix,
                    s3_bucket=s3_bucket, 
                    s3_prefix=s3_prefix,
                    service_account_to_impersonate=sa,
                    replace=replace_files
                )
            return transfer_task
        
        # Create PythonOperator instead of GCSToS3Operator
        transfer_op = PythonOperator(
            task_id=task_id,
            python_callable=create_transfer_task(
                gcs_bucket=temp_bucket,
                gcs_prefix=f"{full_destination_path}_",
                s3_bucket=bucket,
                s3_prefix=f"{full_destination_path}_", 
                sa=service_account,
                replace_files=overwrite
            ),
            doc_md=f"""
            Transfer BigQuery export files from GCS to S3 using working WIF implementation.
            
            Source: gs://{temp_bucket}/{full_destination_path}_*
            Destination: s3://{bucket}/{full_destination_path}_*
            Format: {export_format}
            Service Account: {service_account}
            
            This task bypasses Airflow's buggy GCSToS3Operator.
            """
        )
        
        transfer_operators.append(transfer_op)
    
    return transfer_operators


# ALTERNATIVELY, if you want to keep it simpler, just replace the GCSToS3Operator creation with:
"""
# Instead of this (which fails):
transfer_op = GCSToS3Operator(
    task_id=task_id,
    gcs_bucket=temp_bucket,
    prefix=f"{full_destination_path}_",
    dest_s3_key=f"s3://{bucket}/{full_destination_path}_",
    dest_aws_conn_id=aws_connection_id,
    google_impersonation_chain=[service_account] if service_account else None,
    replace=overwrite,
)

# Use this (which works):
def transfer_task():
    transfer_gcs_to_s3_working(
        gcs_bucket=temp_bucket,
        gcs_prefix=f"{full_destination_path}_",
        s3_bucket=bucket,
        s3_prefix=f"{full_destination_path}_",
        service_account_to_impersonate=service_account,
        replace=overwrite
    )

transfer_op = PythonOperator(
    task_id=task_id,
    python_callable=transfer_task,
)
"""