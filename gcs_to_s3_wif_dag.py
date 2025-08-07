
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.transfers.gcs_to_s3 import GCSToS3Operator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import boto3
import google.auth
from google.auth import impersonated_credentials
from google.auth.transport import requests as gauth_requests

# Custom AWS connection using WIF
class WIFAwsHook(AwsBaseHook):
    """Custom AWS Hook that uses WIF for authentication"""
    
    def get_session(self, region_name=None):
        # WIF configuration
        service_account_email = "tina-gcp-to-s3-sa@elmyra-test.iam.gserviceaccount.com"
        audience = "sts.amazonaws.com"
        aws_role_arn = "arn:aws:iam::277108755423:role/sandbox-dvdh-write-from-gcp-to-aws-test"
        
        # Get default credentials (Airflow service account)
        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        # Impersonate target service account
        target_credentials = impersonated_credentials.Credentials(
            source_credentials=credentials,
            target_principal=service_account_email,
            target_scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        # Generate ID token
        id_token_credentials = impersonated_credentials.IDTokenCredentials(
            target_credentials,
            target_audience=audience,
            include_email=True
        )
        
        request = gauth_requests.Request()
        id_token_credentials.refresh(request)
        
        # Assume AWS role
        sts = boto3.client('sts')
        response = sts.assume_role_with_web_identity(
            RoleArn=aws_role_arn,
            RoleSessionName='composer-wif-session',
            WebIdentityToken=id_token_credentials.token
        )
        
        aws_creds = response['Credentials']
        
        # Create session with temporary credentials
        return boto3.Session(
            aws_access_key_id=aws_creds['AccessKeyId'],
            aws_secret_access_key=aws_creds['SecretAccessKey'],
            aws_session_token=aws_creds['SessionToken'],
            region_name=region_name or 'eu-central-1'
        )

# DAG definition
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 7),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'gcs_to_s3_wif_transfer',
    default_args=default_args,
    description='Transfer files from GCS to S3 using WIF',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# The transfer task
transfer_task = GCSToS3Operator(
    task_id='transfer_gcs_to_s3',
    gcs_bucket='your-source-gcs-bucket',
    prefix='path/to/files/',
    dest_s3_key='s3://sandbox-dvdh-gcp-to-s3/destination/',
    dest_aws_conn_id=None,  # We'll use custom hook
    aws_hook=WIFAwsHook(),  # Use our custom WIF hook
    replace=True,
    dag=dag,
)

transfer_task
