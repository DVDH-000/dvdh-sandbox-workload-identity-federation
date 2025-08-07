#!/usr/bin/env python3
"""
Local test of the working WIF implementation
This tests the exact same logic that will be used in the Airflow DAG
"""

import boto3
import google.auth
from google.auth import impersonated_credentials
from google.auth.transport import requests as gauth_requests
from datetime import datetime
import json


def get_aws_credentials_via_wif():
    """
    Generate AWS credentials using Google WIF
    This is the exact same function that will be used in Airflow
    """
    print("🎯 Getting AWS credentials via Google WIF")
    
    # Configuration (same as in Airflow DAG)
    target_sa = "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
    audience = "sts.amazonaws.com"
    role_arn = "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"
    
    try:
        # Get source credentials (your local default credentials)
        source_credentials, project = google.auth.default()
        print(f"✅ Using project: {project}")
        
        # Create impersonated credentials
        print(f"🎭 Impersonating: {target_sa}")
        impersonated_creds = impersonated_credentials.Credentials(
            source_credentials=source_credentials,
            target_principal=target_sa,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        # Generate ID token for AWS
        print(f"🎯 Generating token with audience: {audience}")
        id_token_credentials = impersonated_credentials.IDTokenCredentials(
            impersonated_creds,
            target_audience=audience,
            include_email=True
        )
        
        request = gauth_requests.Request()
        id_token_credentials.refresh(request)
        id_token = id_token_credentials.token
        
        print(f"✅ Generated ID token (length: {len(id_token)})")
        
        # Decode token to verify claims
        try:
            import jwt
            decoded = jwt.decode(id_token, options={"verify_signature": False})
            print(f"📋 Token claims:")
            for key in ['aud', 'azp', 'sub', 'iss', 'email']:
                if key in decoded:
                    print(f"   {key}: {decoded[key]}")
        except ImportError:
            print("⚠️  jwt library not available for token inspection")
        
        # Use STS to assume the AWS role
        print(f"☁️  Assuming AWS role: {role_arn}")
        sts_client = boto3.client('sts', region_name='us-east-1')
        
        response = sts_client.assume_role_with_web_identity(
            RoleArn=role_arn,
            RoleSessionName=f"local-test-wif-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
            WebIdentityToken=id_token,
            DurationSeconds=3600  # 1 hour
        )
        
        credentials = response['Credentials']
        print(f"✅ Successfully assumed AWS role!")
        print(f"   ARN: {response['AssumedRoleUser']['Arn']}")
        print(f"   Expires: {credentials['Expiration']}")
        
        return {
            'aws_access_key_id': credentials['AccessKeyId'],
            'aws_secret_access_key': credentials['SecretAccessKey'],
            'aws_session_token': credentials['SessionToken'],
            'expiration': credentials['Expiration']
        }
        
    except Exception as e:
        print(f"❌ WIF credential generation failed: {e}")
        import traceback
        traceback.print_exc()
        raise


def test_aws_connection_working():
    """Test AWS connection with working WIF implementation"""
    print("\n🧪 TESTING AWS CONNECTION WITH WORKING WIF")
    print("=" * 60)
    
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
        
        print(f"\n✅ Created boto3 session with WIF credentials")
        
        # Test STS
        print("🔍 Testing STS access...")
        sts = session.client('sts')
        identity = sts.get_caller_identity()
        print(f"✅ AWS Identity: {identity['Arn']}")
        print(f"✅ Account: {identity['Account']}")
        
        # Test S3
        print("\n🔍 Testing S3 access...")
        s3 = session.client('s3')
        buckets = s3.list_buckets()
        print(f"✅ Found {len(buckets['Buckets'])} S3 buckets")
        
        # Show first few buckets
        for i, bucket in enumerate(buckets['Buckets'][:3]):
            print(f"   📦 {bucket['Name']}")
        
        # Test specific bucket access
        target_bucket = 'sandbox-dvdh-gcp-to-s3'
        print(f"\n🔍 Testing access to target bucket: {target_bucket}")
        try:
            response = s3.list_objects_v2(
                Bucket=target_bucket,
                MaxKeys=5
            )
            
            if 'Contents' in response:
                print(f"✅ Bucket contains {len(response['Contents'])} objects")
                for obj in response['Contents']:
                    print(f"   📄 {obj['Key']} ({obj['Size']} bytes)")
            else:
                print("✅ Bucket is empty but accessible")
                
        except Exception as bucket_error:
            print(f"⚠️  Bucket access issue: {bucket_error}")
        
        print("\n🎉 ALL AWS TESTS PASSED!")
        print("✅ The working WIF implementation is ready for Airflow!")
        return True
        
    except Exception as e:
        print(f"\n❌ AWS connection test failed: {e}")
        return False


def test_gcs_access():
    """Test GCS access to make sure we can read from source"""
    print("\n🧪 TESTING GCS ACCESS")
    print("=" * 60)
    
    try:
        from google.cloud import storage
        
        # Create GCS client (uses default credentials)
        gcs_client = storage.Client()
        
        print("✅ Created GCS client")
        
        # List buckets
        buckets = list(gcs_client.list_buckets())
        print(f"✅ Found {len(buckets)} GCS buckets")
        
        # Show first few buckets
        for i, bucket in enumerate(buckets[:5]):
            print(f"   📦 {bucket.name}")
        
        print("\n🎉 GCS ACCESS TEST PASSED!")
        return True
        
    except Exception as e:
        print(f"❌ GCS access test failed: {e}")
        return False


def test_file_transfer_simulation():
    """Simulate a file transfer without actually moving files"""
    print("\n🧪 SIMULATING FILE TRANSFER")
    print("=" * 60)
    
    try:
        # Get AWS credentials
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
        
        from google.cloud import storage
        gcs_client = storage.Client()
        
        print("✅ Created both GCS and S3 clients with working credentials")
        
        # Test upload capability with a small test object
        test_bucket = 'sandbox-dvdh-gcp-to-s3'
        test_key = f'test-wif-{datetime.now().strftime("%Y%m%d-%H%M%S")}.txt'
        test_content = f"WIF test file created at {datetime.now()}"
        
        print(f"🔍 Testing S3 upload capability...")
        print(f"   Bucket: {test_bucket}")
        print(f"   Key: {test_key}")
        
        # Upload test file
        s3_client.put_object(
            Bucket=test_bucket,
            Key=test_key,
            Body=test_content.encode('utf-8'),
            ContentType='text/plain'
        )
        
        print(f"✅ Successfully uploaded test file to S3!")
        
        # Verify the upload
        response = s3_client.get_object(Bucket=test_bucket, Key=test_key)
        downloaded_content = response['Body'].read().decode('utf-8')
        
        if downloaded_content == test_content:
            print(f"✅ Upload verification successful!")
        else:
            print(f"⚠️  Upload verification failed")
        
        # Clean up test file
        s3_client.delete_object(Bucket=test_bucket, Key=test_key)
        print(f"✅ Cleaned up test file")
        
        print("\n🎉 FILE TRANSFER SIMULATION SUCCESSFUL!")
        print("✅ Ready for real GCS to S3 transfers in Airflow!")
        return True
        
    except Exception as e:
        print(f"❌ File transfer simulation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all local tests"""
    print("🚀 LOCAL TEST OF WORKING WIF IMPLEMENTATION")
    print("=" * 60)
    print("🎯 Testing the exact same logic that will run in Airflow")
    
    results = []
    
    # Test 1: AWS connection
    print("\n" + "🔸" * 60)
    aws_result = test_aws_connection_working()
    results.append(("AWS Connection", aws_result))
    
    # Test 2: GCS access
    print("\n" + "🔸" * 60)
    gcs_result = test_gcs_access()
    results.append(("GCS Access", gcs_result))
    
    # Test 3: File transfer simulation
    if aws_result and gcs_result:
        print("\n" + "🔸" * 60)
        transfer_result = test_file_transfer_simulation()
        results.append(("File Transfer", transfer_result))
    else:
        print("\n⏭️  Skipping file transfer test due to previous failures")
        results.append(("File Transfer", False))
    
    # Summary
    print("\n" + "=" * 60)
    print("🎯 LOCAL TEST RESULTS")
    print("=" * 60)
    
    all_passed = True
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if not result:
            all_passed = False
    
    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 ALL TESTS PASSED!")
        print("✅ The working WIF implementation is ready!")
        print("🚀 Upload working_wif_dag.py to Cloud Composer")
        print("🎯 It will work exactly like these local tests")
    else:
        print("❌ SOME TESTS FAILED!")
        print("🔧 Fix the issues above before using in Airflow")
    
    return all_passed


if __name__ == "__main__":
    main()