import os
import json
import jwt
import google.auth
from google.auth.transport import requests

def main():
    # Ensure this environment variable points to your WIF credential config file
    credentials_path = "/Users/davyvanderhorst/code/sandbox/dvdh-sandbox-workload-identity-federation/tina-wif-cred.json"
    if not credentials_path or not os.path.isfile(credentials_path):
        print("[ERROR] Set GOOGLE_APPLICATION_CREDENTIALS to your WIF config JSON file.")
        return

    print(f"Using credentials file: {credentials_path}\n")

    creds, project = google.auth.default()
    creds.refresh(requests.Request())

    # For access tokens (OAuth 2.0), they are typically opaque and cannot be decoded
    access_token = creds.token
    if not access_token:
        print("[ERROR] Failed to retrieve access token.")
        return

    print(f"Access Token: {access_token[:50]}...")
    print("Note: Access tokens are typically opaque and cannot be decoded like JWTs.")
    
    # Check if this is actually a JWT (has 3 parts separated by dots)
    token_parts = access_token.split('.')
    if len(token_parts) != 3:
        print(f"[INFO] This access token has {len(token_parts)} parts, not 3 like a JWT.")
        print("[INFO] Access tokens from Google are typically opaque strings, not JWTs.")
        print("[INFO] If you need to inspect claims, you might need to use the ID token instead.")
        return
    
    try:
        # Decode JWT without signature verification for inspection
        decoded = jwt.decode(access_token, options={"verify_signature": False})
    except jwt.exceptions.DecodeError as e:
        print(f"[ERROR] Failed to decode token as JWT: {e}")
        print("[INFO] This confirms the token is an opaque access token, not a JWT.")
        return

    print("Decoded OIDC Token Claims:")
    print(json.dumps(decoded, indent=2))

    # Extract and display 'aud' if present
    aud = decoded.get("aud")
    if aud:
        print(f"\n• Token 'aud' claim: {aud}")
    else:
        print("\n[WARNING] No 'aud' claim found in token.")

if __name__ == "__main__":
    main()