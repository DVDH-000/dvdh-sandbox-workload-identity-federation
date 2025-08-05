## Workload Identity Overview

Workload Identity allows your workloads to access Google Cloud **without** using Service Account keys.

### Setup Steps

1. **Create a Workload Identity Pool**  
   Organizes and manages external identities. IAM lets you grant access to identities in the pool.

2. **Connect an Identity Provider**  
   Add an AWS or OpenID Connect (OIDC) provider to your pool.

3. **Configure Provider Mapping**  
   Map attributes and claims from your provider to IAM.

4. **Grant Access**  
   Use a service account to allow pool identities to access Google Cloud resources.



Things we did
* Configure the provider attributes: 
    * Simply keep as default.

Things we need from the AWS Side:
* AWS Account ID (2 digit number without dashes)


Nevermind...
The entire "federation" setup happens on the AWS side.. Ahaaaaa got it.


They need to:
Create an OpenID Connect (OIDC) identity provider in IAM:
1. Go to IAM/Identity Providers
2. Provider URL should be: https://accounts.google.com
3. They need the Service Account's OAuth 2 client ID we can see within the Google Cloud Console.
4. Create / Use bucket
4. Now they can give access to what the account is allowed to do within Policies. Example:
```json
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "VisualEditor0",
			"Effect": "Allow",
			"Action": [
				"s3:ListAccessPointsForObjectLambda",
				"s3:GetAccessPoint",
				"s3:PutAccountPublicAccessBlock",
				"s3:ListAccessPoints",
				"s3:CreateStorageLensGroup",
				"s3:ListJobs",
				"s3:PutStorageLensConfiguration",
				"s3:ListMultiRegionAccessPoints",
				"s3:ListStorageLensGroups",
				"s3:ListStorageLensConfigurations",
				"s3:GetAccountPublicAccessBlock",
				"s3:ListAllMyBuckets",
				"s3:ListAccessGrantsInstances",
				"s3:PutAccessPointPublicAccessBlock",
				"s3:CreateJob"
			],
			"Resource": "*"
		},
		{
			"Sid": "VisualEditor1",
			"Effect": "Allow",
			"Action": "s3:*",
			"Resource": [
				"arn:aws:s3:::sandbox-dvdh-gcp-to-s3",
				"arn:aws:s3:::sandbox-dvdh-gcp-to-s3/*"
			]
		}
	]
}
```
5. Create AWS IAM Role (Web Identity)
6. Provider Google
7. Web Identity is the OAuth 2 client ID


On Our End: 
GCP / IAM
* Create Dedicated Service Account for GCP to S3 (TINA).
* Grant the main Cloud Composer service account the Service Account Token Creator role on the dedicated service account.
* Grant the roles: BigQuery User, Storage Object Admin, Service Account Token Creator

Airflow UI Configuration:
* We need the Role ARN from their eind.
* Create a Distinct Connection ID.
* Leave Password Blank
* Extra (JSON):
```json
{
  "role_arn": "arn:aws:iam::123456789012:role/GCP-BigQuery-to-S3-Transfer-Role"
}
```

Aiflow Code Configuration:
```python
from airflow.providers.amazon.aws.transfers.gcs_to_s3 import GCSToS3Operator

transfer_gcs_to_s3 = GCSToS3Operator(
    task_id="transfer_gcs_to_s3",
    
    # --- GCP Configuration ---
    gcp_conn_id="google_cloud_default",
    bucket="your-gcp-staging-bucket",  # The source GCS bucket
    prefix="path/to/your/data/",      # The source "folder" in GCS
    impersonation_chain="bq-to-s3-transfer-sa@your-gcp-project-id.iam.gserviceaccount.com",

    # --- AWS Configuration ---
    aws_conn_id="aws_s3_main",
    dest_aws_bucket_name="your-aws-destination-bucket", # The destination S3 bucket
    dest_s3_key="path/in/s3/",                          # The destination "folder" in S3
    replace=True,
)
```




Note:
OAuth 2 client ID = Audience = Client ID