import boto3
from botocore.exceptions import NoCredentialsError, ClientError, BotoCoreError
from typing import List
import pandas as pd
import awswrangler as wr

class S3_Client:
    def __init__(self, s3:boto3.client):
        """
        Initialize an S3 Client.
        """
        self.s3 = s3
    def list_files(self, bucket: str, prefix: str = ""):
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            keys = []
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                keys.extend(obj["Key"] for obj in page.get("Contents", []))
            return keys
        except ClientError as e:
            print(f"Error listing files: {e}")
            return []
    
    def list_buckets(self) -> List[str]:
        """List all S3 bucket names in the AWS account."""
        try:
            print("Listing all S3 buckets")
            response = self.s3.list_buckets()
            
            # Extract just the bucket names
            bucket_names = [bucket['Name'] for bucket in response.get('Buckets', [])]
            
            print(f"Found {len(bucket_names)} buckets")
            return bucket_names
            
        except (ClientError, BotoCoreError) as e:
            print(f"Error listing buckets: {e}")
            return []
        except Exception as e:
            print(f"Unexpected error listing buckets: {e}")
            return []

    
    def read_parquet_to_dataframe(self, bucket: str, key: str) -> pd.DataFrame:
        """Read a Parquet file from S3 directly into a pandas DataFrame."""
        try:
            path = f"s3://{bucket}/{key}"
            df = wr.s3.read_parquet(path, map_types = False)
            print(f"✅ Loaded s3://{bucket}/{key} into DataFrame")
            return df
        except (NoCredentialsError, ClientError) as e:
            print(f"❌ Error reading s3://{bucket}/{key}: {e}")
            return pd.DataFrame()  # return empty DF if something fails       
    
    def write_df_to_parquet(self, df: pd.DataFrame, bucket: str, key: str, compression: str = 'snappy', index: bool = False):
        """
        Write a pandas DataFrame to a Parquet file in S3.
        
        Args:
            df (pd.DataFrame): DataFrame to write
            bucket (str): S3 bucket name
            key (str): S3 key/path for the parquet file
            compression (str): Compression type ('snappy', 'gzip', 'brotli', 'lz4', 'zstd')
            index (bool): Whether to include the DataFrame index
        """
        try:
            path = f"s3://{bucket}/{key}"
            
            # Use awswrangler to write parquet file
            wr.s3.to_parquet(
                df=df,
                path=path,
                index=index,
                compression=compression,
                boto3_session=None  # Uses default session
            )
            
            print(f"✅ Written DataFrame to parquet: s3://{bucket}/{key}")
            print(f"   - Rows: {len(df)}, Columns: {len(df.columns)}")
            print(f"   - Compression: {compression}")
            
        except (NoCredentialsError, ClientError) as e:
            print(f"❌ Error writing DataFrame to parquet s3://{bucket}/{key}: {e}")
        except Exception as e:
            print(f"❌ Unexpected error writing parquet file: {e}")

    def upload_file(self, local_path: str, bucket: str, key: str):
        """Upload a local file to S3."""
        try:
            self.s3.upload_file(local_path, bucket, key)
            print(f"Uploaded {local_path} to s3://{bucket}/{key}")
        except (NoCredentialsError, ClientError) as e:
            print(f"Error uploading file: {e}")
            
    def get_latest_parquet_file_key(self, bucket, company, partition_date):
        """
        Get the single most recently modified parquet file for a given company and partition date.
        
        Args:
            bucket (str): S3 bucket name
            company (str): company name, e.g. "ariccione"
            partition_date (str): partition, e.g. "2025-07-01"
        
        Returns:
            str | None: S3 URI of the latest parquet file, or None if none found
        """
    

        prefix = f"{company}/silver/estratto_fatture/PARTITION_DATE={partition_date}/"

        paginator = self.s3.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

        latest_file = None
        latest_time = None
        df = None

        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    if obj["Key"].endswith(".parquet"):
                        if latest_time is None or obj["LastModified"] > latest_time:
                            latest_time = obj["LastModified"]
                            latest_file = obj["Key"]

        if latest_file:
            return latest_file    
            
    def check_file_exists(self, bucket: str, full_path: str) -> bool:
        """
        Check if a file exists in S3.
        
        Args:
            bucket (str): S3 bucket name
            full_path (str): Full S3 path/key to the file
            
        Returns:
            bool: True if file exists, False otherwise
        """
        try:
            self.s3.head_object(Bucket=bucket, Key=full_path)
            print(f"✅ File exists: s3://{bucket}/{full_path}")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"❌ File not found: s3://{bucket}/{full_path}")
                return False
            else:
                print(f"❌ Error checking file existence: {e}")
                return False
        except Exception as e:
            print(f"❌ Unexpected error checking file: {e}")
            return False
    
    
    
    def create_file(self, bucket: str, full_path: str, name: str, content: str = "", content_type: str = "text/plain"):
        """
        Create a file in S3 with optional content.
        
        Args:
            bucket (str): S3 bucket name
            full_path (str): Full S3 path where to create the file
            name (str): File name
            content (str): File content (default: empty string)
            content_type (str): MIME type of the file (default: text/plain)
            
        Returns:
            str: Full S3 key of created file, or None if failed
        """
        try:
            # Ensure full_path ends with / if not empty
            if full_path and not full_path.endswith('/'):
                full_path += '/'
            
            file_key = f"{full_path}{name}"
            
            # Create file with content
            self.s3.put_object(
                Bucket=bucket,
                Key=file_key,
                Body=content,
                ContentType=content_type
            )
            
            print(f"✅ Created file: s3://{bucket}/{file_key}")
            return file_key
            
        except ClientError as e:
            print(f"❌ Error creating file: {e}")
            return None
        except Exception as e:
            print(f"❌ Unexpected error creating file: {e}")
            return None