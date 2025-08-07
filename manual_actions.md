# MANUAL ACTIONS REQUIRED FOR WIF SETUP

## 1. GET SERVICE ACCOUNT UNIQUE ID
Run this command to get the unique ID needed for AWS OIDC configuration:

```bash
gcloud iam service-accounts describe tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com \
    --format="value(uniqueId)"
```

Replace `111691016524440319722` in the Terraform configuration with this actual value.

## 2. VERIFY AWS OIDC PROVIDER AUDIENCES
In AWS Console:
1. Go to IAM > Identity providers
2. Click on `accounts.google.com` provider
3. Verify the audience list includes your service account unique ID
4. If not, add it to the audience list

## 3. UPDATE AWS ROLE TRUST POLICY
Ensure your AWS role trust policy includes both conditions:

```json
{
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
          "accounts.google.com:aud": "ACTUAL_SERVICE_ACCOUNT_UNIQUE_ID",
          "accounts.google.com:oaud": "sts.amazonaws.com"
        }
      }
    }
  ]
}
```

## 4. CONFIGURE AIRFLOW CONNECTION
Choose one method:

### Method A: Airflow UI
1. Admin > Connections
2. Edit/Create `aws_default`
3. Type: Amazon Web Services
4. Extra: `{"role_arn": "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test", "assume_role_method": "assume_role_with_web_identity", "assume_role_with_web_identity_federation": "google", "assume_role_with_web_identity_federation_audience": "sts.amazonaws.com"}`

### Method B: Cloud Composer Environment Variable
Set: `AIRFLOW_CONN_AWS_DEFAULT` to the connection string

## 5. TEST THE SETUP
Run this test script in Cloud Composer to verify:

```python
from airflow.operators.python import PythonOperator

def test_wif_connection():
    import boto3
    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
    
    # Test AWS connection
    hook = AwsBaseHook(aws_conn_id="aws_default")
    session = hook.get_session()
    s3 = session.client('s3')
    
    # List buckets to test
    response = s3.list_buckets()
    print(f"✅ WIF working! Found {len(response['Buckets'])} buckets")
    
test_task = PythonOperator(
    task_id='test_wif',
    python_callable=test_wif_connection
)
```

## 6. VALIDATION CHECKLIST
- [ ] Service account unique ID obtained
- [ ] AWS OIDC provider configured with correct audience
- [ ] AWS role trust policy updated with both conditions
- [ ] Airflow connection configured with WIF parameters
- [ ] OpenID Token Creator role granted to Airflow SA
- [ ] Test task runs successfully
- [ ] GCSToS3Operator works without errors

## TROUBLESHOOTING
If you still get "Incorrect token audience":
1. Verify the service account unique ID matches in AWS and GCP
2. Check the audience in the JWT token matches "sts.amazonaws.com"
3. Ensure both `accounts.google.com:aud` and `accounts.google.com:oaud` conditions are in the trust policy
4. Verify the Airflow service account has both TokenCreator and OpenIdTokenCreator roles