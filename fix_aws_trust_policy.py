#!/usr/bin/env python3
"""
Fix AWS trust policy for GCP to AWS WIF
This corrects the trust policy based on the debug findings
"""

import json
import boto3
from botocore.exceptions import ClientError

def get_current_trust_policy():
    """Get the current trust policy"""
    try:
        iam = boto3.client('iam')
        role_name = "sandbox-dvdh-write-from-gcp-to-aws-test"
        
        response = iam.get_role(RoleName=role_name)
        return response['Role']['AssumeRolePolicyDocument']
    except Exception as e:
        print(f"❌ Error getting trust policy: {e}")
        return None

def create_correct_trust_policy():
    """Create the correct trust policy for GCP to AWS WIF"""
    
    # Key information from our debug:
    # - Service account client ID: 111691016524440319722
    # - Correct OIDC provider ARN: arn:aws:iam::277108755423:oidc-provider/accounts.google.com
    
    correct_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Federated": "arn:aws:iam::277108755423:oidc-provider/accounts.google.com"
                },
                "Action": "sts:AssumeRoleWithWebIdentity",
                "Condition": {
                    "StringEquals": {
                        # AWS maps 'accounts.google.com:aud' to the 'azp' claim in Google tokens
                        # The 'azp' claim contains the service account client ID
                        "accounts.google.com:aud": "111691016524440319722",
                        # Optional: Also check the original audience claim
                        "accounts.google.com:oaud": "sts.amazonaws.com"
                    }
                }
            }
        ]
    }
    
    return correct_policy

def update_trust_policy():
    """Update the AWS role trust policy"""
    print("🔧 FIXING AWS TRUST POLICY")
    print("=" * 60)
    
    try:
        iam = boto3.client('iam')
        role_name = "sandbox-dvdh-write-from-gcp-to-aws-test"
        
        # Get current policy
        current_policy = get_current_trust_policy()
        if current_policy:
            print("📋 Current trust policy:")
            print(json.dumps(current_policy, indent=2))
        
        # Create correct policy
        correct_policy = create_correct_trust_policy()
        print("\n✅ Correct trust policy:")
        print(json.dumps(correct_policy, indent=2))
        
        # Update the policy
        print(f"\n🔄 Updating trust policy for role: {role_name}")
        
        iam.update_assume_role_policy(
            RoleName=role_name,
            PolicyDocument=json.dumps(correct_policy)
        )
        
        print("✅ Trust policy updated successfully!")
        return True
        
    except ClientError as e:
        print(f"❌ AWS error updating trust policy: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def verify_oidc_provider():
    """Verify the OIDC provider configuration"""
    print("\n🔍 VERIFYING OIDC PROVIDER")
    print("=" * 60)
    
    try:
        iam = boto3.client('iam')
        
        # List OIDC providers
        response = iam.list_open_id_connect_providers()
        
        google_provider = None
        for provider in response['OpenIDConnectProviderList']:
            if 'accounts.google.com' in provider['Arn']:
                google_provider = provider['Arn']
                break
        
        if google_provider:
            print(f"✅ Google OIDC provider found: {google_provider}")
            
            # Get provider details
            provider_arn = google_provider
            provider_response = iam.get_open_id_connect_provider(
                OpenIDConnectProviderArn=provider_arn
            )
            
            print(f"📋 Provider URL: {provider_response['Url']}")
            print(f"📋 Client IDs: {provider_response['ClientIDList']}")
            print(f"📋 Thumbprints: {provider_response['ThumbprintList']}")
            
            # Check if our service account client ID is in the audience list
            target_client_id = "111691016524440319722"
            if target_client_id in provider_response['ClientIDList']:
                print(f"✅ Service account client ID {target_client_id} is in audience list")
            else:
                print(f"⚠️  Service account client ID {target_client_id} is NOT in audience list")
                print("💡 You may need to add it to the OIDC provider")
            
            return True
        else:
            print("❌ Google OIDC provider not found!")
            print("💡 You need to create an OIDC provider for accounts.google.com")
            return False
            
    except Exception as e:
        print(f"❌ Error checking OIDC provider: {e}")
        return False

def test_updated_policy():
    """Test the updated policy with our debug script"""
    print("\n🧪 TESTING UPDATED POLICY")
    print("=" * 60)
    
    try:
        import subprocess
        import sys
        
        print("🔄 Running debug script to test the fix...")
        
        # Run our debug script again
        result = subprocess.run([
            sys.executable, 
            "debug_airflow_impersonation.py"
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            # Look for success indicators in the output
            if "AWS assume role successful!" in result.stdout:
                print("✅ SUCCESS: AWS assume role is now working!")
                return True
            else:
                print("⚠️  Test completed but AWS assume role may still have issues")
                print("📝 Debug output:")
                print(result.stdout)
                return False
        else:
            print("❌ Debug script failed to run")
            print(f"Error: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error running test: {e}")
        return False

def show_airflow_configuration():
    """Show the correct Airflow configuration"""
    print("\n🎯 AIRFLOW CONFIGURATION")
    print("=" * 60)
    
    print("📋 For your Airflow AWS connection, use this configuration:")
    
    airflow_config = {
        "role_arn": "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test",
        "assume_role_method": "assume_role_with_web_identity",
        "assume_role_with_web_identity_federation": "google",
        "assume_role_with_web_identity_federation_audience": "sts.amazonaws.com"
    }
    
    print("🔧 In Airflow UI > Admin > Connections:")
    print("   Connection ID: gcp_to_aws_s3_sandbox_davy")
    print("   Connection Type: Amazon Web Services")
    print("   Extra field (JSON):")
    print(json.dumps(airflow_config, indent=2))
    
    print("\n📝 Your GCSToS3Operator should work with:")
    print("""
    transfer_op = GCSToS3Operator(
        task_id='gcs_to_s3_transfer',
        gcs_bucket='your-gcs-bucket',
        prefix='path/to/files',
        dest_s3_key='s3://sandbox-dvdh-gcp-to-s3/destination/path/',
        dest_aws_conn_id='gcp_to_aws_s3_sandbox_davy',
        google_impersonation_chain=['tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com'],
        replace=True
    )
    """)

def main():
    """Main function to fix the AWS trust policy"""
    print("🚀 FIXING AWS TRUST POLICY FOR GCP TO AWS WIF")
    print("=" * 60)
    
    # Step 1: Verify OIDC provider
    provider_ok = verify_oidc_provider()
    if not provider_ok:
        print("\n❌ OIDC provider issues detected. Fix those first.")
        return
    
    # Step 2: Update trust policy
    policy_updated = update_trust_policy()
    if not policy_updated:
        print("\n❌ Failed to update trust policy")
        return
    
    # Step 3: Test the fix
    print("\n⏳ Waiting a moment for AWS to propagate the policy changes...")
    import time
    time.sleep(5)
    
    test_successful = test_updated_policy()
    
    # Step 4: Show Airflow configuration
    show_airflow_configuration()
    
    # Summary
    print("\n" + "=" * 60)
    print("🎯 SUMMARY")
    print("=" * 60)
    
    if test_successful:
        print("✅ SUCCESS: Your WIF configuration is now working!")
        print("🎉 Airflow should be able to transfer from GCS to S3")
    else:
        print("⚠️  Trust policy updated, but there may still be issues")
        print("💡 Check the test output above for remaining problems")
    
    print("\n📋 Next steps:")
    print("1. Update your Airflow connection with the configuration shown above")
    print("2. Test your GCSToS3Operator in Airflow")
    print("3. Monitor CloudTrail for AssumeRoleWithWebIdentity events")

if __name__ == "__main__":
    main()