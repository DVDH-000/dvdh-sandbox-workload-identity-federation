#!/usr/bin/env python3
"""
Debug what token Airflow is actually generating vs what we expect
This will intercept the actual token Airflow sends to AWS
"""

import json
import base64

def decode_jwt_payload(token):
    """Decode JWT payload without verification"""
    try:
        # Split the token
        parts = token.split('.')
        if len(parts) != 3:
            return None
        
        # Decode the payload (middle part)
        payload = parts[1]
        
        # Add padding if needed
        missing_padding = len(payload) % 4
        if missing_padding:
            payload += '=' * (4 - missing_padding)
        
        # Decode base64
        decoded_bytes = base64.urlsafe_b64decode(payload)
        decoded_str = decoded_bytes.decode('utf-8')
        
        # Parse JSON
        return json.loads(decoded_str)
        
    except Exception as e:
        print(f"Error decoding JWT: {e}")
        return None

def analyze_airflow_token_from_logs():
    """Analyze the token from the Airflow logs"""
    print("🔍 ANALYZING AIRFLOW TOKEN FROM LOGS")
    print("=" * 60)
    
    # This is the token from your latest log
    airflow_token = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImJhNjNiNDM2ODM2YTkzOWI3OTViNDEyMmQzZjRkMGQyMjVkMWM3MDAiLCJ0eXAiOiJKV1QifQ.eyJhdWQiOiJzdHMuYW1hem9uYXdzLmNvbSIsImF6cCI6IjExMTY5MTAxNjUyNDQ0MDMxOTcyMiIsImVtYWlsIjoidGluYS1nY3AtdG8tczMtc2FAZWxteXJhLXRlc3QuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiZXhwIjoxNzU0NTc3NTEzLCJpYXQiOjE3NTQ1NzM5MTMsImlzcyI6Imh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbSIsInN1YiI6IjExMTY5MTAxNjUyNDQ0MDMxOTcyMiJ9.Ylnnu2n1ZzJ3ZfFjz5obDkh4Fdn_lurCToR6R8CZkszTdQ-80Io9ko80SHuClb5EXM8Gk2St01j2cLUfWKO-ec93lC2TmFgVEDFGJt6XNsHtvPLdOqfznXu03XSvJ-SozuZTMBxstrZzD6EBmt3xRYgioD93gDNMPK7UsfAZrkx1wPZtssnIm_CLhRM4hbexVkN8KdYgMhy32p41-50LVU6PhJtgoLe0zy0dpQiXisJeq5Io3p_7DAMPpsnr5zZMI6S9J0iQLUNayRs_BeloGnZ1o49lRuH_d5r4ft_z-LFbICLHV4Mn9-6_XIluLB8UxhFllf_x4R6O6NvQv-ZAkg"
    
    print(f"📝 Token length: {len(airflow_token)}")
    
    # Decode the token
    claims = decode_jwt_payload(airflow_token)
    
    if claims:
        print("\n📋 AIRFLOW TOKEN CLAIMS:")
        for key, value in claims.items():
            print(f"  🎯 {key}: {value}")
    else:
        print("❌ Could not decode token")
        return
    
    print("\n🔍 ANALYSIS:")
    print("=" * 40)
    
    # Check the critical claims
    aud = claims.get('aud')
    azp = claims.get('azp')
    sub = claims.get('sub')
    iss = claims.get('iss')
    
    print(f"✅ aud (audience): {aud}")
    print(f"✅ azp (authorized party): {azp}")
    print(f"✅ sub (subject): {sub}")
    print(f"✅ iss (issuer): {iss}")
    
    # This token looks correct! So why is it failing?
    print("\n🤔 TOKEN ANALYSIS:")
    print("   - This token has the CORRECT claims")
    print("   - aud: sts.amazonaws.com (matches our expectation)")
    print("   - azp: 111691016524440319722 (service account client ID)")
    print("   - sub: 111691016524440319722 (same as azp)")
    print("   - iss: https://accounts.google.com (correct issuer)")
    
    return claims

def test_token_manually():
    """Test the exact same token manually to see if it works"""
    print("\n🧪 TESTING AIRFLOW'S TOKEN MANUALLY")
    print("=" * 60)
    
    # Use the exact token from Airflow logs
    airflow_token = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImJhNjNiNDM2ODM2YTkzOWI3OTViNDEyMmQzZjRkMGQyMjVkMWM3MDAiLCJ0eXAiOiJKV1QifQ.eyJhdWQiOiJzdHMuYW1hem9uYXdzLmNvbSIsImF6cCI6IjExMTY5MTAxNjUyNDQ0MDMxOTcyMiIsImVtYWlsIjoidGluYS1nY3AtdG8tczMtc2FAZWxteXJhLXRlc3QuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiZXhwIjoxNzU0NTc3NTEzLCJpYXQiOjE3NTQ1NzM5MTMsImlzcyI6Imh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbSIsInN1YiI6IjExMTY5MTAxNjUyNDQ0MDMxOTcyMiJ9.Ylnnu2n1ZzJ3ZfFjz5obDkh4Fdn_lurCToR6R8CZkszTdQ-80Io9ko80SHuClb5EXM8Gk2St01j2cLUfWKO-ec93lC2TmFgVEDFGJt6XNsHtvPLdOqfznXu03XSvJ-SozuZTMBxstrZzD6EBmt3xRYgioD93gDNMPK7UsfAZrkx1wPZtssnIm_CLhRM4hbexVkN8KdYgMhy32p41-50LVU6PhJtgoLe0zy0dpQiXisJeq5Io3p_7DAMPpsnr5zZMI6S9J0iQLUNayRs_BeloGnZ1o49lRuH_d5r4ft_z-LFbICLHV4Mn9-6_XIluLB8UxhFllf_x4R6O6NvQv-ZAkg"
    
    try:
        import boto3
        from botocore.exceptions import ClientError
        
        # Test this exact token
        role_arn = "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"
        
        print(f"🎯 Testing token with role: {role_arn}")
        
        sts_client = boto3.client('sts')
        response = sts_client.assume_role_with_web_identity(
            RoleArn=role_arn,
            RoleSessionName="manual-test-airflow-token",
            WebIdentityToken=airflow_token
        )
        
        print("✅ SUCCESS! The token works manually!")
        print(f"   ARN: {response['AssumedRoleUser']['Arn']}")
        
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        print(f"❌ FAILED: {error_code}")
        print(f"   Message: {error_message}")
        
        if "InvalidIdentityToken" in error_code:
            print("\n💡 Same error as Airflow! This confirms the token is the issue.")
        
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def check_token_expiration():
    """Check if the token might be expired"""
    print("\n⏰ CHECKING TOKEN EXPIRATION")
    print("=" * 60)
    
    airflow_token = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImJhNjNiNDM2ODM2YTkzOWI3OTViNDEyMmQzZjRkMGQyMjVkMWM3MDAiLCJ0eXAiOiJKV1QifQ.eyJhdWQiOiJzdHMuYW1hem9uYXdzLmNvbSIsImF6cCI6IjExMTY5MTAxNjUyNDQ0MDMxOTcyMiIsImVtYWlsIjoidGluYS1nY3AtdG8tczMtc2FAZWxteXJhLXRlc3QuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiZXhwIjoxNzU0NTc3NTEzLCJpYXQiOjE3NTQ1NzM5MTMsImlzcyI6Imh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbSIsInN1YiI6IjExMTY5MTAxNjUyNDQ0MDMxOTcyMiJ9.Ylnnu2n1ZzJ3ZfFjz5obDkh4Fdn_lurCToR6R8CZkszTdQ-80Io9ko80SHuClb5EXM8Gk2St01j2cLUfWKO-ec93lC2TmFgVEDFGJt6XNsHtvPLdOqfznXu03XSvJ-SozuZTMBxstrZzD6EBmt3xRYgioD93gDNMPK7UsfAZrkx1wPZtssnIm_CLhRM4hbexVkN8KdYgMhy32p41-50LVU6PhJtgoLe0zy0dpQiXisJeq5Io3p_7DAMPpsnr5zZMI6S9J0iQLUNayRs_BeloGnZ1o49lRuH_d5r4ft_z-LFbICLHV4Mn9-6_XIluLB8UxhFllf_x4R6O6NvQv-ZAkg"
    
    claims = decode_jwt_payload(airflow_token)
    
    if claims:
        import time
        from datetime import datetime
        
        exp = claims.get('exp')
        iat = claims.get('iat')
        now = int(time.time())
        
        print(f"⏰ Token issued at (iat): {iat} ({datetime.fromtimestamp(iat)})")
        print(f"⏰ Token expires at (exp): {exp} ({datetime.fromtimestamp(exp)})")
        print(f"⏰ Current time: {now} ({datetime.fromtimestamp(now)})")
        
        if now > exp:
            print("❌ TOKEN IS EXPIRED!")
            return False
        else:
            time_left = exp - now
            print(f"✅ Token is valid for {time_left} more seconds")
            return True
    
    return False

def show_real_solution():
    """Show what the real issue might be"""
    print("\n💡 THE REAL ISSUE ANALYSIS")
    print("=" * 60)
    
    print("🔍 OBSERVATIONS:")
    print("1. ✅ Token generation in debug script works")
    print("2. ✅ Token has correct claims (aud, azp, sub, iss)")
    print("3. ❌ Same token fails when used by Airflow AWS provider")
    print("4. ❌ Manual testing also fails (needs verification)")
    
    print("\n🤔 POSSIBLE CAUSES:")
    print("1. 🕐 Token expiration timing issue")
    print("2. 🔄 Credential caching in Airflow")
    print("3. 🌐 Network/endpoint differences")
    print("4. 📦 Airflow provider version bug")
    print("5. 🎭 Different session/credential context")
    
    print("\n🔧 IMMEDIATE FIXES TO TRY:")
    print("1. Clear Airflow's credential cache")
    print("2. Update Airflow AWS provider version")
    print("3. Use environment variables instead of connections")
    print("4. Use static AWS credentials temporarily")
    print("5. Check if there's a timing issue with token refresh")

def main():
    """Main analysis function"""
    print("🚀 AIRFLOW TOKEN DEEP ANALYSIS")
    print("=" * 60)
    print("🎯 Goal: Figure out why the same token works manually but fails in Airflow")
    
    # Step 1: Analyze the token from logs
    claims = analyze_airflow_token_from_logs()
    
    if not claims:
        print("❌ Cannot analyze token")
        return
    
    # Step 2: Check expiration
    is_valid = check_token_expiration()
    
    if not is_valid:
        print("❌ Token expiration issue detected")
        return
    
    # Step 3: Test manually
    manual_success = test_token_manually()
    
    # Step 4: Show analysis
    show_real_solution()
    
    print("\n" + "=" * 60)
    print("🎯 VERDICT")
    print("=" * 60)
    
    if manual_success:
        print("✅ Token works manually - issue is in Airflow's usage")
        print("💡 Likely causes: caching, provider version, or timing")
    else:
        print("❌ Token fails both manually and in Airflow")
        print("💡 The token itself has an issue")
    
    print("\n🔧 NEXT ACTIONS:")
    print("1. Try static AWS credentials in Airflow temporarily")
    print("2. Update apache-airflow-providers-amazon")
    print("3. Clear any cached credentials in Composer")
    print("4. Check CloudTrail for AssumeRoleWithWebIdentity events")

if __name__ == "__main__":
    main()