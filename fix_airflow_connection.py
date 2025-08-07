#!/usr/bin/env python3
"""
Fix Airflow connection configuration for WIF
Based on the debugging, the issue is likely in how Airflow AWS provider handles the token
"""

import json

def show_current_issue():
    """Show the current issue based on logs"""
    print("🔍 CURRENT ISSUE ANALYSIS")
    print("=" * 60)
    
    print("✅ WHAT'S WORKING:")
    print("1. Service account impersonation in Composer")
    print("2. OIDC token generation with correct claims:")
    print("   - aud: sts.amazonaws.com")
    print("   - azp: 111691016524440319722")
    print("   - sub: 111691016524440319722")
    print("   - iss: https://accounts.google.com")
    print()
    print("❌ WHAT'S FAILING:")
    print("1. Airflow's AwsBaseHook credential refresh")
    print("2. AssumeRoleWithWebIdentity operation")
    print("3. Error: 'Incorrect token audience'")
    print()
    print("🤔 THE MYSTERY:")
    print("Same token works manually but fails in Airflow AWS provider")

def show_airflow_connection_fixes():
    """Show different Airflow connection configurations to try"""
    print("\n🔧 AIRFLOW CONNECTION FIXES TO TRY")
    print("=" * 60)
    
    print("📋 FIX 1: Remove audience parameter (let Airflow decide)")
    fix1 = {
        "role_arn": "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test",
        "assume_role_method": "assume_role_with_web_identity",
        "assume_role_with_web_identity_federation": "google",
        "region_name": "us-east-1"
    }
    print("Connection Extra JSON:")
    print(json.dumps(fix1, indent=2))
    
    print("\n📋 FIX 2: Use role ARN as audience")
    fix2 = {
        "role_arn": "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test",
        "assume_role_method": "assume_role_with_web_identity",
        "assume_role_with_web_identity_federation": "google",
        "assume_role_with_web_identity_federation_audience": "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test",
        "region_name": "us-east-1"
    }
    print("Connection Extra JSON:")
    print(json.dumps(fix2, indent=2))
    
    print("\n📋 FIX 3: Use different STS configuration")
    fix3 = {
        "role_arn": "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test",
        "assume_role_method": "assume_role_with_web_identity",
        "assume_role_with_web_identity_federation": "google",
        "assume_role_with_web_identity_federation_audience": "sts.amazonaws.com",
        "region_name": "us-east-1",
        "sts_regional_endpoints": "regional"
    }
    print("Connection Extra JSON:")
    print(json.dumps(fix3, indent=2))
    
    print("\n📋 FIX 4: Use service account email as audience")
    fix4 = {
        "role_arn": "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test",
        "assume_role_method": "assume_role_with_web_identity",
        "assume_role_with_web_identity_federation": "google",
        "assume_role_with_web_identity_federation_audience": "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com",
        "region_name": "us-east-1"
    }
    print("Connection Extra JSON:")
    print(json.dumps(fix4, indent=2))

def show_alternative_aws_trust_policies():
    """Show alternative AWS trust policies to try"""
    print("\n🔧 ALTERNATIVE AWS TRUST POLICIES")
    print("=" * 60)
    
    print("📋 OPTION 1: Accept multiple audiences")
    policy1 = {
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
                        "accounts.google.com:aud": "111691016524440319722"
                    },
                    "StringLike": {
                        "accounts.google.com:oaud": [
                            "sts.amazonaws.com",
                            "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"
                        ]
                    }
                }
            }
        ]
    }
    print("AWS Trust Policy:")
    print(json.dumps(policy1, indent=2))
    
    print("\n📋 OPTION 2: More permissive audience check")
    policy2 = {
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
                        "accounts.google.com:aud": "111691016524440319722",
                        "accounts.google.com:sub": "111691016524440319722"
                    }
                }
            }
        ]
    }
    print("AWS Trust Policy:")
    print(json.dumps(policy2, indent=2))

def show_test_dag():
    """Show a test DAG to verify the fix"""
    print("\n📝 TEST DAG TO VERIFY FIX")
    print("=" * 60)
    
    test_dag = '''"""
Test DAG to verify WIF fix
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

def test_aws_connection():
    """Test AWS connection with WIF"""
    try:
        hook = AwsBaseHook(aws_conn_id='gcp_to_aws_s3_sandbox_davy')
        session = hook.get_session()
        sts = session.client('sts')
        identity = sts.get_caller_identity()
        
        print(f"✅ SUCCESS: Connected as {identity['Arn']}")
        return True
    except Exception as e:
        print(f"❌ FAILED: {e}")
        raise

def test_s3_access():
    """Test S3 access with WIF"""
    try:
        hook = AwsBaseHook(aws_conn_id='gcp_to_aws_s3_sandbox_davy')
        session = hook.get_session()
        s3 = session.client('s3')
        
        # Try to list objects in the target bucket
        response = s3.list_objects_v2(
            Bucket='sandbox-dvdh-gcp-to-s3',
            MaxKeys=1
        )
        
        print(f"✅ SUCCESS: Can access S3 bucket")
        return True
    except Exception as e:
        print(f"❌ FAILED: {e}")
        raise

default_args = {
    'owner': 'debug',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 7),
    'retries': 0
}

dag = DAG(
    'test_wif_connection',
    default_args=default_args,
    description='Test WIF connection fix',
    schedule_interval=None,
    catchup=False,
    tags=['debug', 'wif', 'test']
)

test_conn = PythonOperator(
    task_id='test_connection',
    python_callable=test_aws_connection,
    dag=dag
)

test_s3 = PythonOperator(
    task_id='test_s3',
    python_callable=test_s3_access,
    dag=dag
)

test_conn >> test_s3
'''
    
    print("Save this as test_wif_connection.py in your DAGs folder:")
    print(test_dag)

def show_step_by_step_troubleshooting():
    """Show step-by-step troubleshooting approach"""
    print("\n🔍 STEP-BY-STEP TROUBLESHOOTING")
    print("=" * 60)
    
    print("📋 Phase 1: Try connection fixes")
    print("1. Start with FIX 1 (remove audience) - test in Airflow")
    print("2. If still fails, try FIX 2 (role ARN as audience)")
    print("3. If still fails, try FIX 3 (regional STS)")
    print("4. If still fails, try FIX 4 (service account email)")
    print()
    print("📋 Phase 2: Try AWS trust policy changes")
    print("1. If connection fixes don't work, try OPTION 1 trust policy")
    print("2. If still fails, try OPTION 2 trust policy")
    print()
    print("📋 Phase 3: Check Airflow version compatibility")
    print("1. Check your Airflow version")
    print("2. Check apache-airflow-providers-amazon version")
    print("3. Some versions have bugs with Google WIF")
    print()
    print("📋 Phase 4: Alternative approaches")
    print("1. Use static AWS credentials temporarily")
    print("2. Use cross-account roles instead of WIF")
    print("3. Use Airflow's native Google Cloud integration")

def main():
    """Main function to show all fixes"""
    print("🚀 AIRFLOW WIF CONNECTION FIXES")
    print("=" * 60)
    print("🎯 Goal: Fix 'Incorrect token audience' in Airflow AWS provider")
    
    show_current_issue()
    show_airflow_connection_fixes()
    show_alternative_aws_trust_policies()
    show_test_dag()
    show_step_by_step_troubleshooting()
    
    print("\n" + "=" * 60)
    print("🎯 RECOMMENDED APPROACH")
    print("=" * 60)
    print("1. ✅ Start with FIX 1 - remove the audience parameter")
    print("2. ✅ Use the test DAG to verify each fix")
    print("3. ✅ If none work, the issue may be Airflow provider version")
    print("4. ✅ Consider using static AWS credentials as fallback")
    print()
    print("💡 The manual token generation works, so the AWS trust policy")
    print("   is correct. The issue is how Airflow's AWS provider handles")
    print("   the token internally.")

if __name__ == "__main__":
    main()