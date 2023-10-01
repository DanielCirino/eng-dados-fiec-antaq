import os

import boto3
from airflow.models import Variable

AWS_ENDPOINT = Variable.get("AWS_ENDPOINT", os.getenv("S3_ENDPOINT_URL",""))
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID", os.getenv("S3_AWS_ACCESS_KEY_ID",""))
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY", os.getenv("S3_AWS_SECRET_ACCESS_KEY",""))
AWS_REGION = Variable.get("AWS_REGION", os.getenv("S3_AWS_REGION_NAME",""))


clientS3 = boto3.client('s3',
                        endpoint_url=AWS_ENDPOINT,
                        aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                        aws_session_token=None,
                        config=boto3.session.Config(signature_version='s3v4'),
                        verify=False,
                        region_name=AWS_REGION
                        )