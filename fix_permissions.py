#!/usr/bin/env python3
"""
Script to identify current identity and show how to fix WIF permissions
"""

import google.auth
from google.auth.transport import requests

def get_current_identity():
    """Get information about the current authenticated identity"""
    print("🔍 Checking current identity...")
    
    try:
        credentials, project = google.auth.default()
        
        # Refresh credentials to ensure they're valid
        request = requests.Request()
        credentials.refresh(request)
        
        print(f"✅ Authenticated as project: {project}")
        
        # Try to get more details about the current identity
        if hasattr(credentials, 'service_account_email'):
            print(f"📧 Service Account: {credentials.service_account_email}")
            current_identity = credentials.service_account_email
        elif hasattr(credentials, '_service_account_email'):
            print(f"📧 Service Account: {credentials._service_account_email}")
            current_identity = credentials._service_account_email
        else:
            print("👤 User Account (not service account)")
            # For user accounts, we can't easily get the email from credentials
            current_identity = "your-user-account@gmail.com"  # User needs to replace this
        
        return current_identity, project
        
    except Exception as e:
        print(f"❌ Failed to get current identity: {e}")
        return None, None

def show_permission_fix_instructions(current_identity, target_sa):
    """Show instructions for fixing the permission issue"""
    print("\n" + "="*60)
    print("🔧 HOW TO FIX THE PERMISSION ISSUE")
    print("="*60)
    
    print(f"\n📋 Current Identity: {current_identity}")
    print(f"🎯 Target Service Account: {target_sa}")
    
    print(f"\n🔑 SOLUTION: Grant Token Creator Role")
    print("You need to run this gcloud command:")
    
    gcloud_command = f"""
gcloud iam service-accounts add-iam-policy-binding \\
    {target_sa} \\
    --member="serviceAccount:{current_identity}" \\
    --role="roles/iam.serviceAccountTokenCreator"
"""
    
    if "gmail.com" in current_identity or "your-user-account" in current_identity:
        print("\n⚠️  If you're using a user account (not service account):")
        gcloud_command = f"""
gcloud iam service-accounts add-iam-policy-binding \\
    {target_sa} \\
    --member="user:YOUR-EMAIL@gmail.com" \\
    --role="roles/iam.serviceAccountTokenCreator"
"""
        print("Replace YOUR-EMAIL@gmail.com with your actual email")
    
    print(gcloud_command)
    
    print("\n📝 Alternative: Using Google Cloud Console")
    print("1. Go to IAM & Admin > Service Accounts")
    print(f"2. Find service account: {target_sa}")
    print("3. Click on it, then go to 'PERMISSIONS' tab")
    print("4. Click 'GRANT ACCESS'")
    print(f"5. Add principal: {current_identity}")
    print("6. Select role: 'Service Account Token Creator'")
    print("7. Click 'Save'")
    
    print("\n🎯 What this permission allows:")
    print("- Generate ID tokens for the service account")
    print("- Essential for Workload Identity Federation")
    print("- Allows impersonation for token generation")

def show_cloud_composer_instructions():
    """Show instructions specific to Cloud Composer"""
    print("\n" + "="*60)
    print("🎵 CLOUD COMPOSER SPECIFIC SETUP")
    print("="*60)
    
    print("\n📋 For Cloud Composer, you have two options:")
    
    print("\n🔵 Option 1: Grant permissions to Composer Service Account")
    print("1. Find your Cloud Composer environment")
    print("2. Note the Composer service account (usually ends with @cloudcomposer-accounts.iam.gserviceaccount.com)")
    print("3. Grant that service account the 'Service Account Token Creator' role on your target SA")
    
    print("\n🔵 Option 2: Use the target service account directly")
    print("1. Configure your Cloud Composer to run as the target service account")
    print("2. This eliminates the need for impersonation")
    
    print("\n🔵 Option 3: Simple AWS Credentials (Recommended for testing)")
    print("1. Create AWS access keys for your AWS role")
    print("2. Add them as Airflow connections in Cloud Composer")
    print("3. Use dest_aws_conn_id in your GCSToS3Operator")

def main():
    print("🚀 WIF PERMISSION DIAGNOSTIC")
    print("=" * 40)
    
    # Get current identity
    current_identity, project = get_current_identity()
    
    if not current_identity:
        print("❌ Cannot determine current identity")
        return
    
    # Target service account
    target_sa = "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
    
    # Show fix instructions
    show_permission_fix_instructions(current_identity, target_sa)
    
    # Show Cloud Composer specific instructions
    show_cloud_composer_instructions()
    
    print("\n" + "="*60)
    print("🎯 RECOMMENDED NEXT STEPS")
    print("="*60)
    print("1. ✅ Grant the Service Account Token Creator role (as shown above)")
    print("2. ✅ Re-run the WIF test script")
    print("3. ✅ Configure your GCSToS3Operator with working credentials")
    print("4. ✅ Test the complete GCS to S3 transfer")

if __name__ == "__main__":
    main()