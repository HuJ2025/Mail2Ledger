import boto3
from botocore.exceptions import NoCredentialsError
import os
from dotenv import load_dotenv
import re
import uuid

# ✅ 載入 .env 環境變數
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID').strip('"')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY').strip('"')
AWS_REGION = os.getenv('AWS_REGION').strip('"')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME').strip('"')

# Set up the S3 client with your AWS credentials
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def upload_to_s3(file_obj, file_name, mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", bucket=S3_BUCKET_NAME):
    """
    Uploads an in-memory file object (like BytesIO) to S3 and returns the file's URL.
    """
    # Generate unique file name if not provided
    # sanitize the file_name first
    sanitized_name = re.sub(r"\s+", "-", file_name)
    unique_name = f"{uuid.uuid4().hex}_{sanitized_name}"

    try:
        s3.put_object(Bucket=bucket, Key=unique_name, Body=file_obj.getvalue(), ContentType=mime_type)
    except NoCredentialsError:
        print("AWS credentials not available")
        return None
    
    # Generate the S3 URL
    url = f"https://{bucket}.s3.{AWS_REGION}.amazonaws.com/{unique_name}"
    return url