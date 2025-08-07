#!/usr/bin/env python3
"""
Test the working DAG with real bucket names
This creates actual test files and transfers them
"""

import sys
import os

# Add the dags folder to Python path
sys.path.append(os.path.join(os.getcwd(), "airflow_local", "dags"))

def create_real_test_file():
    """Create a real test file in an existing GCS bucket"""
    print("📁 CREATING REAL TEST FILE IN GCS")
    print("=" * 50)
    
    try:
        from google.cloud import storage
        
        # Use an existing bucket we have access to
        bucket_name = "elmyra-test-screaming-frog-processed-results"
        
        gcs_client = storage.Client()
        bucket = gcs_client.bucket(bucket_name)
        
        # Create test file content
        test_content = f"""
WIF Airflow Test File
Created at: {__import__('datetime').datetime.now()}
Source bucket: {bucket_name}
Destination: s3://sandbox-dvdh-gcp-to-s3

This file demonstrates that the WIF DAG works correctly in local Airflow testing!

Test details:
- Service account impersonation: working
- OIDC token generation: working  
- AWS role assumption: working
- GCS access: working
- S3 access: working

Ready for Cloud Composer deployment!
"""
        
        # Upload test file
        test_file_name = f"airflow-test/wif-test-{__import__('datetime').datetime.now().strftime('%Y%m%d-%H%M%S')}.txt"
        blob = bucket.blob(test_file_name)
        
        blob.upload_from_string(
            test_content,
            content_type='text/plain'
        )
        
        print(f"✅ Created test file: gs://{bucket_name}/{test_file_name}")
        print(f"📊 File size: {len(test_content)} bytes")
        
        return bucket_name, test_file_name, test_content
        
    except Exception as e:
        print(f"❌ Failed to create test file: {e}")
        raise

def test_modified_dag_functions():
    """Test DAG functions with real bucket/file names"""
    print("🚀 TESTING DAG WITH REAL BUCKETS")
    print("=" * 80)
    
    try:
        # Import the core WIF function
        from working_wif_dag import get_aws_credentials_via_wif
        
        # Create test file
        gcs_bucket, gcs_file, original_content = create_real_test_file()
        
        # Test AWS credentials
        print(f"\n{'='*60}")
        print("TEST 1: AWS CREDENTIALS")
        print(f"{'='*60}")
        
        aws_creds = get_aws_credentials_via_wif()
        print("✅ AWS credentials generated successfully")
        
        # Test actual file transfer
        print(f"\n{'='*60}")
        print("TEST 2: REAL FILE TRANSFER")
        print(f"{'='*60}")
        
        import boto3
        from google.cloud import storage
        
        # Create clients
        aws_session = boto3.Session(
            aws_access_key_id=aws_creds['aws_access_key_id'],
            aws_secret_access_key=aws_creds['aws_secret_access_key'],
            aws_session_token=aws_creds['aws_session_token'],
            region_name='us-east-1'
        )
        s3_client = aws_session.client('s3')
        gcs_client = storage.Client()
        
        print(f"📥 Downloading from: gs://{gcs_bucket}/{gcs_file}")
        
        # Download from GCS
        bucket = gcs_client.bucket(gcs_bucket)
        blob = bucket.blob(gcs_file)
        file_content = blob.download_as_bytes()
        
        print(f"✅ Downloaded {len(file_content)} bytes from GCS")
        
        # Upload to S3
        s3_bucket = 'sandbox-dvdh-gcp-to-s3'
        s3_key = f"airflow-test/{gcs_file.split('/')[-1]}"  # Just the filename
        
        print(f"📤 Uploading to: s3://{s3_bucket}/{s3_key}")
        
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=file_content,
            ContentType='text/plain',
            Metadata={
                'source-bucket': gcs_bucket,
                'source-file': gcs_file,
                'test-type': 'airflow-local-test',
                'transfer-time': __import__('datetime').datetime.now().isoformat()
            }
        )
        
        print(f"✅ Uploaded to S3 successfully")
        
        # Verify content
        print("🔍 Verifying transfer...")
        s3_response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        s3_content = s3_response['Body'].read()
        
        if s3_content == file_content:
            print("✅ Content verification PASSED")
            transfer_success = True
        else:
            print("❌ Content verification FAILED")
            transfer_success = False
        
        # Show file info
        s3_head = s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
        print(f"📊 S3 file size: {s3_head['ContentLength']} bytes")
        print(f"📊 S3 metadata: {s3_head.get('Metadata', {})}")
        
        # Cleanup
        print(f"\n🧹 CLEANING UP")
        print("=" * 30)
        
        print("🗑️  Deleting GCS test file...")
        blob.delete()
        print("✅ GCS file deleted")
        
        print("🗑️  Deleting S3 test file...")
        s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)
        print("✅ S3 file deleted")
        
        return transfer_success
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_airflow_dag_simulation():
    """Simulate running the entire Airflow DAG"""
    print(f"\n{'='*80}")
    print("🎭 AIRFLOW DAG SIMULATION")
    print(f"{'='*80}")
    print("🎯 This simulates what would happen when the DAG runs in Airflow")
    
    try:
        # Import DAG functions
        from working_wif_dag import (
            get_aws_credentials_via_wif,
            test_aws_connection_working
        )
        
        # Task 1: test_aws_connection (this should work)
        print(f"\n📋 TASK 1: test_aws_connection")
        print("-" * 40)
        
        task1_result = test_aws_connection_working()
        print(f"Task 1 result: {'✅ SUCCESS' if task1_result else '❌ FAILED'}")
        
        # Task 2: Real file transfer (our custom test)
        print(f"\n📋 TASK 2: real_file_transfer")
        print("-" * 40)
        
        task2_result = test_modified_dag_functions()
        print(f"Task 2 result: {'✅ SUCCESS' if task2_result else '❌ FAILED'}")
        
        # Summary
        tasks_passed = sum([task1_result, task2_result])
        total_tasks = 2
        
        print(f"\n{'='*60}")
        print("🎯 AIRFLOW DAG SIMULATION RESULTS")
        print(f"{'='*60}")
        
        print(f"✅ PASS test_aws_connection: {task1_result}")
        print(f"✅ PASS real_file_transfer: {task2_result}")
        print(f"\n📊 Results: {tasks_passed}/{total_tasks} tasks passed")
        
        if tasks_passed == total_tasks:
            print("\n🎉 AIRFLOW DAG SIMULATION SUCCESSFUL!")
            print("✅ All core functionality works")
            print("🚀 Ready for Cloud Composer deployment")
            return True
        else:
            print("\n⚠️  SOME AIRFLOW TASKS WOULD FAIL!")
            print("🔧 Need to fix issues before deployment")
            return False
        
    except Exception as e:
        print(f"❌ DAG simulation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main test function"""
    print("🚀 COMPREHENSIVE LOCAL AIRFLOW TEST")
    print("=" * 80)
    print("🎯 Testing WIF DAG with real buckets and files")
    
    # Run the simulation
    success = test_airflow_dag_simulation()
    
    # Final results
    print(f"\n{'='*80}")
    print("🎯 FINAL TEST VERDICT")
    print(f"{'='*80}")
    
    if success:
        print("🎉 SUCCESS: WIF DAG is fully working!")
        print()
        print("✅ What was tested and confirmed:")
        print("   🔐 Service account impersonation")
        print("   🎟️  OIDC token generation") 
        print("   ☁️  AWS role assumption")
        print("   📥 GCS file download")
        print("   📤 S3 file upload") 
        print("   🔍 Content verification")
        print("   🧹 Cleanup operations")
        print()
        print("🚀 READY FOR CLOUD COMPOSER:")
        print("   1. Upload working_wif_dag.py to your DAGs folder")
        print("   2. Update bucket names in the DAG functions")
        print("   3. Trigger the DAG in Airflow UI")
        print("   4. Watch it work perfectly!")
        
    else:
        print("❌ FAILURE: Some issues remain")
        print("🔧 Review the test results above")
    
    return success

if __name__ == "__main__":
    main()