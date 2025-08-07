#!/usr/bin/env python3
"""
Check what files are in the BigQuery export bucket
This will help us configure the DAG with real file paths
"""

import os
from google.cloud import storage

def setup_airflow_credentials():
    """Use the airflow service account"""
    airflow_key_path = os.path.join(os.getcwd(), "airflow-sa-key.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = airflow_key_path
    print(f"🔐 Using: airflow@elmyra-test.iam.gserviceaccount.com")

def explore_bigquery_bucket():
    """Explore the BigQuery export bucket"""
    print("🗂️  EXPLORING BIGQUERY EXPORT BUCKET")
    print("=" * 60)
    
    bucket_name = 'elmyra_test_bigquery_export_to_bucket'
    
    try:
        # Create GCS client
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        
        print(f"📦 Bucket: gs://{bucket_name}")
        
        # List first 20 objects
        blobs = list(bucket.list_blobs(max_results=20))
        
        if not blobs:
            print("❌ No files found in bucket")
            return None
        
        print(f"📋 Found {len(blobs)} files (showing first 20):")
        print("-" * 50)
        
        sample_files = []
        for i, blob in enumerate(blobs, 1):
            size_mb = blob.size / (1024 * 1024) if blob.size else 0
            print(f"{i:2d}. {blob.name}")
            print(f"    📊 Size: {size_mb:.2f} MB")
            print(f"    📅 Modified: {blob.time_created}")
            print(f"    📝 Type: {blob.content_type}")
            print()
            
            # Collect some sample files for the DAG
            if len(sample_files) < 3:
                sample_files.append({
                    'name': blob.name,
                    'size': blob.size,
                    'content_type': blob.content_type
                })
        
        return sample_files
        
    except Exception as e:
        print(f"❌ Error exploring bucket: {e}")
        return None

def suggest_dag_configuration(sample_files):
    """Suggest DAG configuration based on actual files"""
    if not sample_files:
        print("⚠️  No files found - cannot suggest configuration")
        return
    
    print("🎯 SUGGESTED DAG CONFIGURATION")
    print("=" * 60)
    
    print("For simple_gcs_to_s3_file_transfer() function:")
    print("-" * 40)
    first_file = sample_files[0]
    print(f"gcs_bucket = 'elmyra_test_bigquery_export_to_bucket'")
    print(f"gcs_file = '{first_file['name']}'")
    print(f"s3_bucket = 'sandbox-dvdh-gcp-to-s3'")
    print(f"s3_key = 'bigquery-exports/{first_file['name'].split('/')[-1]}'")
    
    print(f"\nThis will transfer:")
    print(f"  📥 From: gs://elmyra_test_bigquery_export_to_bucket/{first_file['name']}")
    print(f"  📤 To:   s3://sandbox-dvdh-gcp-to-s3/bigquery-exports/{first_file['name'].split('/')[-1]}")
    
    print(f"\n📊 File details:")
    print(f"  💾 Size: {first_file['size'] / (1024*1024):.2f} MB" if first_file['size'] else "  💾 Size: Unknown")
    print(f"  📝 Type: {first_file['content_type']}")

def test_single_file_transfer(sample_files):
    """Test transferring one actual file"""
    if not sample_files:
        print("⚠️  No sample files to test")
        return False
    
    print(f"\n🧪 TESTING SINGLE FILE TRANSFER")
    print("=" * 60)
    
    try:
        # Import our WIF functions
        import sys
        sys.path.append(os.path.join(os.getcwd(), "airflow_local", "dags"))
        from working_wif_dag import get_aws_credentials_via_wif
        
        import boto3
        
        # Get AWS credentials
        print("🔐 Getting AWS credentials...")
        aws_creds = get_aws_credentials_via_wif()
        
        # Create clients
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_creds['aws_access_key_id'],
            aws_secret_access_key=aws_creds['aws_secret_access_key'],
            aws_session_token=aws_creds['aws_session_token'],
            region_name='us-east-1'
        )
        
        gcs_client = storage.Client()
        
        # Use first sample file
        sample_file = sample_files[0]
        gcs_bucket_name = 'elmyra_test_bigquery_export_to_bucket'
        s3_bucket_name = 'sandbox-dvdh-gcp-to-s3'
        
        print(f"📄 Testing file: {sample_file['name']}")
        
        # Download from GCS
        bucket = gcs_client.bucket(gcs_bucket_name)
        blob = bucket.blob(sample_file['name'])
        
        print(f"📥 Downloading from GCS...")
        file_content = blob.download_as_bytes()
        print(f"✅ Downloaded {len(file_content)} bytes")
        
        # Upload to S3
        s3_key = f"bigquery-exports-test/{sample_file['name'].split('/')[-1]}"
        print(f"📤 Uploading to S3: s3://{s3_bucket_name}/{s3_key}")
        
        s3_client.put_object(
            Bucket=s3_bucket_name,
            Key=s3_key,
            Body=file_content,
            ContentType=sample_file['content_type'] or 'application/octet-stream',
            Metadata={
                'source-bucket': gcs_bucket_name,
                'source-file': sample_file['name'],
                'test-type': 'bigquery-export-transfer',
                'original-size': str(sample_file['size']) if sample_file['size'] else '0'
            }
        )
        
        print(f"✅ Successfully transferred BigQuery export file!")
        print(f"🔗 S3 location: s3://{s3_bucket_name}/{s3_key}")
        
        return True
        
    except Exception as e:
        print(f"❌ Test transfer failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function"""
    print("🚀 BIGQUERY EXPORT BUCKET ANALYSIS")
    print("=" * 80)
    print("🎯 Preparing DAG configuration for BigQuery exports")
    
    # Setup credentials
    setup_airflow_credentials()
    
    # Explore bucket
    sample_files = explore_bigquery_bucket()
    
    if sample_files:
        # Suggest configuration
        suggest_dag_configuration(sample_files)
        
        # Test actual transfer
        success = test_single_file_transfer(sample_files)
        
        print(f"\n{'='*80}")
        print("🎯 FINAL RESULTS")
        print(f"{'='*80}")
        
        if success:
            print("🎉 SUCCESS: BigQuery export transfer works!")
            print("✅ Found BigQuery export files")
            print("✅ WIF authentication works")
            print("✅ File transfer works")
            print("🚀 DAG is ready for BigQuery export processing!")
        else:
            print("⚠️  PARTIAL SUCCESS: Files found but transfer had issues")
            print("✅ Found BigQuery export files")
            print("❌ Transfer test failed")
    else:
        print("❌ No files found in BigQuery export bucket")
        print("🔧 Check bucket permissions or wait for BigQuery exports to run")

if __name__ == "__main__":
    main()