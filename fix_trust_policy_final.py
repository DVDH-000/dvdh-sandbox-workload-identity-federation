#!/usr/bin/env python3
"""
Fix the AWS trust policy one final time
The policy got reverted to the broken configuration
"""

import json
import boto3

def fix_trust_policy():
    """Fix the AWS trust policy with the correct configuration"""
    print("🔧 FIXING AWS TRUST POLICY (FINAL)")
    print("=" * 60)
    
    try:
        iam = boto3.client('iam')
        role_name = "sandbox-dvdh-write-from-gcp-to-aws-test"
        
        # Get current policy
        current_response = iam.get_role(RoleName=role_name)
        current_policy = current_response['Role']['AssumeRolePolicyDocument']
        
        print("❌ Current (broken) trust policy:")
        print(json.dumps(current_policy, indent=2))
        
        # The CORRECT trust policy based on our analysis
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
                            "accounts.google.com:aud": "111691016524440319722",
                            # Optional: Also check the original audience claim
                            "accounts.google.com:oaud": "sts.amazonaws.com"
                        }
                    }
                }
            ]
        }
        
        print("\n✅ Correct trust policy:")
        print(json.dumps(correct_policy, indent=2))
        
        print(f"\n🔄 Updating trust policy for role: {role_name}")
        
        iam.update_assume_role_policy(
            RoleName=role_name,
            PolicyDocument=json.dumps(correct_policy)
        )
        
        print("✅ Trust policy updated successfully!")
        
        # Verify the update
        updated_response = iam.get_role(RoleName=role_name)
        updated_policy = updated_response['Role']['AssumeRolePolicyDocument']
        
        print("\n🔍 Verification - Updated policy:")
        print(json.dumps(updated_policy, indent=2))
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to update trust policy: {e}")
        return False

def explain_the_mapping():
    """Explain the critical AWS <-> Google claim mapping"""
    print("\n📋 CRITICAL UNDERSTANDING:")
    print("=" * 60)
    print("🔍 AWS IAM condition key mapping for Google tokens:")
    print("   'accounts.google.com:aud' maps to JWT 'azp' claim")
    print("   'accounts.google.com:oaud' maps to JWT 'aud' claim")
    print()
    print("🎯 Our token has:")
    print("   azp: 111691016524440319722 (service account client ID)")
    print("   aud: sts.amazonaws.com (target audience)")
    print()
    print("✅ Correct AWS trust policy should check:")
    print("   'accounts.google.com:aud': '111691016524440319722' (azp claim)")
    print("   'accounts.google.com:oaud': 'sts.amazonaws.com' (aud claim)")
    print()
    print("❌ The broken policy was checking:")
    print("   'accounts.google.com:aud': 'sts.amazonaws.com' (wrong!)")
    print("   'accounts.google.com:sub': '111691016524440319722' (redundant)")

def main():
    """Main function to fix the trust policy"""
    print("🚀 FINAL TRUST POLICY FIX")
    print("=" * 60)
    
    explain_the_mapping()
    
    success = fix_trust_policy()
    
    if success:
        print("\n🎉 SUCCESS!")
        print("✅ Trust policy is now correct")
        print("🚀 Run the local test again - it should work now")
    else:
        print("\n❌ FAILED!")
        print("🔧 Check AWS permissions and try again")

if __name__ == "__main__":
    main()