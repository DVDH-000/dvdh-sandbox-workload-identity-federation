#!/usr/bin/env python3
"""
Full local test that actually transfers files from GCS to S3
This is a complete end-to-end test of the working WIF implementation
"""

import boto3
import google.auth
from google.auth import impersonated_credentials
from google.auth.transport import requests as gauth_requests
from google.cloud import storage
from datetime import datetime
import tempfile
import os
import json
import time


def get_aws_credentials_via_wif():
    """Generate AWS credentials using Google WIF"""
    print("🎯 Getting AWS credentials via Google WIF")
    
    # Configuration
    target_sa = "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
    audience = "sts.amazonaws.com"
    role_arn = "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"
    
    try:
        # Get source credentials
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
            RoleSessionName=f"full-local-test-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
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


def create_test_file_in_gcs():
    """Create a test file in GCS for transfer testing"""
    print("\n📁 CREATING TEST FILE IN GCS")
    print("=" * 50)
    
    try:
        # Create GCS client
        gcs_client = storage.Client()
        
        # Choose a bucket that exists and we have access to
        bucket_name = "elmyra-test-screaming-frog-processed-results"  # From our bucket list
        bucket = gcs_client.bucket(bucket_name)
        
        # Create test file content
        test_content = f"""
This is a test file for WIF transfer testing.
Created at: {datetime.now()}
Source: GCS bucket {bucket_name}
Destination: S3 bucket sandbox-dvdh-gcp-to-s3
Method: Google Cloud Service Account Impersonation + WIF

Test data:
- Project: elmyra-test
- Service Account: tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com
- AWS Role: arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test

This file demonstrates that the WIF chain is working correctly!
"""
        
        # Upload test file to GCS
        test_file_name = f"wif-test-{datetime.now().strftime('%Y%m%d-%H%M%S')}.txt"
        blob = bucket.blob(test_file_name)
        
        blob.upload_from_string(
            test_content,
            content_type='text/plain'
        )
        
        print(f"✅ Created test file: gs://{bucket_name}/{test_file_name}")
        print(f"📊 File size: {len(test_content)} bytes")
        
        return bucket_name, test_file_name, test_content
        
    except Exception as e:
        print(f"❌ Failed to create test file in GCS: {e}")
        raise


def transfer_file_gcs_to_s3(gcs_bucket, gcs_file, test_content):
    """Transfer the test file from GCS to S3"""
    print(f"\n🚀 TRANSFERRING FILE: gs://{gcs_bucket}/{gcs_file} → s3://sandbox-dvdh-gcp-to-s3/{gcs_file}")
    print("=" * 80)
    
    try:
        # Get AWS credentials via WIF
        aws_creds = get_aws_credentials_via_wif()
        
        # Create AWS session
        aws_session = boto3.Session(
            aws_access_key_id=aws_creds['aws_access_key_id'],
            aws_secret_access_key=aws_creds['aws_secret_access_key'],
            aws_session_token=aws_creds['aws_session_token'],
            region_name='us-east-1'
        )
        
        # Create clients
        s3_client = aws_session.client('s3')
        gcs_client = storage.Client()
        
        # Download from GCS
        print("📥 Downloading from GCS...")
        gcs_bucket_obj = gcs_client.bucket(gcs_bucket)
        blob = gcs_bucket_obj.blob(gcs_file)
        
        file_content = blob.download_as_bytes()
        print(f"✅ Downloaded {len(file_content)} bytes from GCS")
        
        # Verify content matches
        if file_content.decode('utf-8') == test_content:
            print("✅ GCS content verification passed")
        else:
            print("⚠️  GCS content verification failed")
        
        # Upload to S3
        print("📤 Uploading to S3...")
        s3_bucket = 'sandbox-dvdh-gcp-to-s3'
        s3_key = f"transferred-files/{gcs_file}"
        
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=file_content,
            ContentType='text/plain',
            Metadata={
                'source-bucket': gcs_bucket,
                'source-file': gcs_file,
                'transfer-method': 'wif-local-test',
                'transfer-time': datetime.now().isoformat()
            }
        )
        
        print(f"✅ Uploaded to s3://{s3_bucket}/{s3_key}")
        
        # Verify S3 upload
        print("🔍 Verifying S3 upload...")
        s3_response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        s3_content = s3_response['Body'].read()
        
        if s3_content == file_content:
            print("✅ S3 content verification passed")
        else:
            print("⚠️  S3 content verification failed")
        
        # Get file info
        s3_head = s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
        print(f"📊 S3 file size: {s3_head['ContentLength']} bytes")
        print(f"📊 S3 last modified: {s3_head['LastModified']}")
        print(f"📊 S3 metadata: {s3_head.get('Metadata', {})}")
        
        return s3_bucket, s3_key
        
    except Exception as e:
        print(f"❌ File transfer failed: {e}")
        import traceback
        traceback.print_exc()
        raise


def verify_transfer_end_to_end(gcs_bucket, gcs_file, s3_bucket, s3_key, original_content):
    """Verify the entire transfer worked correctly"""
    print(f"\n🔍 END-TO-END VERIFICATION")
    print("=" * 50)
    
    try:
        # Get AWS credentials
        aws_creds = get_aws_credentials_via_wif()
        
        # Create clients
        aws_session = boto3.Session(
            aws_access_key_id=aws_creds['aws_access_key_id'],
            aws_secret_access_key=aws_creds['aws_secret_access_key'],
            aws_session_token=aws_creds['aws_session_token'],
            region_name='us-east-1'
        )
        s3_client = aws_session.client('s3')
        gcs_client = storage.Client()
        
        # Read from both sources
        print("📥 Reading from GCS...")
        gcs_bucket_obj = gcs_client.bucket(gcs_bucket)
        gcs_blob = gcs_bucket_obj.blob(gcs_file)
        gcs_content = gcs_blob.download_as_text()
        
        print("📥 Reading from S3...")
        s3_response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        s3_content = s3_response['Body'].read().decode('utf-8')
        
        # Compare all three versions
        print("\n🔍 Content comparison:")
        print(f"   Original length: {len(original_content)}")
        print(f"   GCS length: {len(gcs_content)}")
        print(f"   S3 length: {len(s3_content)}")
        
        if original_content == gcs_content == s3_content:
            print("✅ ALL CONTENT MATCHES PERFECTLY!")
            return True
        else:
            print("❌ Content mismatch detected")
            return False
            
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False


def cleanup_test_files(gcs_bucket, gcs_file, s3_bucket, s3_key):
    """Clean up test files"""
    print(f"\n🧹 CLEANING UP TEST FILES")
    print("=" * 50)
    
    try:
        # Clean up GCS
        print(f"🗑️  Deleting from GCS: gs://{gcs_bucket}/{gcs_file}")
        gcs_client = storage.Client()
        bucket = gcs_client.bucket(gcs_bucket)
        blob = bucket.blob(gcs_file)
        blob.delete()
        print("✅ GCS file deleted")
        
        # Clean up S3
        print(f"🗑️  Deleting from S3: s3://{s3_bucket}/{s3_key}")
        aws_creds = get_aws_credentials_via_wif()
        aws_session = boto3.Session(
            aws_access_key_id=aws_creds['aws_access_key_id'],
            aws_secret_access_key=aws_creds['aws_secret_access_key'],
            aws_session_token=aws_creds['aws_session_token'],
            region_name='us-east-1'
        )
        s3_client = aws_session.client('s3')
        s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)
        print("✅ S3 file deleted")
        
        return True
        
    except Exception as e:
        print(f"⚠️  Cleanup failed: {e}")
        return False


def test_multiple_file_formats():
    """Test transferring different file formats"""
    print(f"\n📋 TESTING MULTIPLE FILE FORMATS")
    print("=" * 50)
    
    test_files = [
        ("test-json.json", json.dumps({"test": "data", "timestamp": datetime.now().isoformat()}), "application/json"),
        ("test-csv.csv", "name,value,timestamp\ntest,123," + datetime.now().isoformat(), "text/csv"),
        ("test-binary.dat", b"Binary test data: " + os.urandom(100), "application/octet-stream")
    ]
    
    results = []
    
    try:
        # Get credentials once
        aws_creds = get_aws_credentials_via_wif()
        aws_session = boto3.Session(
            aws_access_key_id=aws_creds['aws_access_key_id'],
            aws_secret_access_key=aws_creds['aws_secret_access_key'],
            aws_session_token=aws_creds['aws_session_token'],
            region_name='us-east-1'
        )
        s3_client = aws_session.client('s3')
        gcs_client = storage.Client()
        
        gcs_bucket_name = "elmyra-test-screaming-frog-processed-results"
        s3_bucket_name = "sandbox-dvdh-gcp-to-s3"
        
        for filename, content, content_type in test_files:
            print(f"\n📄 Testing {filename} ({content_type})")
            
            try:
                # Upload to GCS
                bucket = gcs_client.bucket(gcs_bucket_name)
                blob = bucket.blob(f"multi-test/{filename}")
                
                if isinstance(content, str):
                    blob.upload_from_string(content, content_type=content_type)
                else:
                    blob.upload_from_string(content, content_type=content_type)
                
                # Download from GCS
                downloaded_content = blob.download_as_bytes()
                
                # Upload to S3
                s3_key = f"multi-format-test/{filename}"
                s3_client.put_object(
                    Bucket=s3_bucket_name,
                    Key=s3_key,
                    Body=downloaded_content,
                    ContentType=content_type
                )
                
                # Verify
                s3_response = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_key)
                s3_content = s3_response['Body'].read()
                
                if downloaded_content == s3_content:
                    print(f"✅ {filename}: Transfer successful")
                    results.append(True)
                else:
                    print(f"❌ {filename}: Content mismatch")
                    results.append(False)
                
                # Cleanup
                blob.delete()
                s3_client.delete_object(Bucket=s3_bucket_name, Key=s3_key)
                
            except Exception as e:
                print(f"❌ {filename}: Failed - {e}")
                results.append(False)
        
        success_rate = sum(results) / len(results) * 100
        print(f"\n📊 Multi-format test results: {sum(results)}/{len(results)} successful ({success_rate:.1f}%)")
        return all(results)
        
    except Exception as e:
        print(f"❌ Multi-format test failed: {e}")
        return False


def performance_test():
    """Test transfer performance with a larger file"""
    print(f"\n⚡ PERFORMANCE TEST")
    print("=" * 50)
    
    try:
        # Create a larger test file (1MB)
        large_content = "Performance test data\n" * 50000  # ~1MB
        
        print(f"📊 Test file size: {len(large_content):,} bytes ({len(large_content)/1024/1024:.1f} MB)")
        
        # Get credentials
        aws_creds = get_aws_credentials_via_wif()
        aws_session = boto3.Session(
            aws_access_key_id=aws_creds['aws_access_key_id'],
            aws_secret_access_key=aws_creds['aws_secret_access_key'],
            aws_session_token=aws_creds['aws_session_token'],
            region_name='us-east-1'
        )
        s3_client = aws_session.client('s3')
        gcs_client = storage.Client()
        
        gcs_bucket_name = "elmyra-test-screaming-frog-processed-results"
        s3_bucket_name = "sandbox-dvdh-gcp-to-s3"
        filename = f"perf-test-{datetime.now().strftime('%Y%m%d-%H%M%S')}.txt"
        
        # Upload to GCS and measure time
        start_time = time.time()
        bucket = gcs_client.bucket(gcs_bucket_name)
        blob = bucket.blob(f"perf-test/{filename}")
        blob.upload_from_string(large_content, content_type='text/plain')
        gcs_upload_time = time.time() - start_time
        
        # Download from GCS and measure time
        start_time = time.time()
        downloaded_content = blob.download_as_bytes()
        gcs_download_time = time.time() - start_time
        
        # Upload to S3 and measure time
        start_time = time.time()
        s3_key = f"perf-test/{filename}"
        s3_client.put_object(
            Bucket=s3_bucket_name,
            Key=s3_key,
            Body=downloaded_content,
            ContentType='text/plain'
        )
        s3_upload_time = time.time() - start_time
        
        # Download from S3 and measure time
        start_time = time.time()
        s3_response = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_key)
        s3_content = s3_response['Body'].read()
        s3_download_time = time.time() - start_time
        
        # Calculate speeds
        file_size_mb = len(large_content) / 1024 / 1024
        
        print(f"📊 Performance results:")
        print(f"   GCS upload: {gcs_upload_time:.2f}s ({file_size_mb/gcs_upload_time:.1f} MB/s)")
        print(f"   GCS download: {gcs_download_time:.2f}s ({file_size_mb/gcs_download_time:.1f} MB/s)")
        print(f"   S3 upload: {s3_upload_time:.2f}s ({file_size_mb/s3_upload_time:.1f} MB/s)")
        print(f"   S3 download: {s3_download_time:.2f}s ({file_size_mb/s3_download_time:.1f} MB/s)")
        print(f"   Total transfer time: {gcs_download_time + s3_upload_time:.2f}s")
        
        # Verify content
        success = downloaded_content == s3_content
        print(f"✅ Content verification: {'PASSED' if success else 'FAILED'}")
        
        # Cleanup
        blob.delete()
        s3_client.delete_object(Bucket=s3_bucket_name, Key=s3_key)
        
        return success
        
    except Exception as e:
        print(f"❌ Performance test failed: {e}")
        return False


def main():
    """Run the full local test suite"""
    print("🚀 FULL LOCAL TEST SUITE - WIF GCS TO S3 TRANSFER")
    print("=" * 80)
    print("🎯 This will create real files and transfer them between GCS and S3")
    
    test_results = []
    
    try:
        # Test 1: Basic file transfer
        print(f"\n{'='*80}")
        print("TEST 1: BASIC FILE TRANSFER")
        print(f"{'='*80}")
        
        gcs_bucket, gcs_file, original_content = create_test_file_in_gcs()
        s3_bucket, s3_key = transfer_file_gcs_to_s3(gcs_bucket, gcs_file, original_content)
        verification_result = verify_transfer_end_to_end(gcs_bucket, gcs_file, s3_bucket, s3_key, original_content)
        
        test_results.append(("Basic Transfer", verification_result))
        
        if verification_result:
            print("🎉 BASIC TRANSFER TEST PASSED!")
        else:
            print("❌ BASIC TRANSFER TEST FAILED!")
        
        # Test 2: Multiple file formats
        print(f"\n{'='*80}")
        print("TEST 2: MULTIPLE FILE FORMATS")
        print(f"{'='*80}")
        
        multi_format_result = test_multiple_file_formats()
        test_results.append(("Multi-format Transfer", multi_format_result))
        
        # Test 3: Performance test
        print(f"\n{'='*80}")
        print("TEST 3: PERFORMANCE TEST")
        print(f"{'='*80}")
        
        performance_result = performance_test()
        test_results.append(("Performance Test", performance_result))
        
        # Cleanup main test files
        print(f"\n{'='*80}")
        print("CLEANUP")
        print(f"{'='*80}")
        
        cleanup_result = cleanup_test_files(gcs_bucket, gcs_file, s3_bucket, s3_key)
        
    except Exception as e:
        print(f"❌ Test suite failed: {e}")
        import traceback
        traceback.print_exc()
    
    # Final results
    print(f"\n{'='*80}")
    print("🎯 FINAL TEST RESULTS")
    print(f"{'='*80}")
    
    for test_name, result in test_results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
    
    passed_tests = sum(1 for _, result in test_results if result)
    total_tests = len(test_results)
    success_rate = passed_tests / total_tests * 100 if total_tests > 0 else 0
    
    print(f"\n📊 Overall Results: {passed_tests}/{total_tests} tests passed ({success_rate:.1f}%)")
    
    if passed_tests == total_tests:
        print("\n🎉 ALL TESTS PASSED!")
        print("✅ The WIF implementation is fully working")
        print("🚀 Ready for production use in Airflow")
        print("📁 Upload working_wif_dag.py to Cloud Composer")
    else:
        print("\n⚠️  SOME TESTS FAILED!")
        print("🔧 Review the failures above")
    
    return passed_tests == total_tests


if __name__ == "__main__":
    main()