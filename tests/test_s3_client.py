import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError, BotoCoreError
from datetime import datetime
import boto3
import sys
import os
from s3_client import S3_Client

# Import the class to test
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestS3Client:
    
    @pytest.fixture
    def mock_s3_client(self):
        """Create a mock S3 client for testing."""
        mock = Mock()
        # Pre-configure common methods that S3 client should have
        mock.get_paginator = Mock()
        mock.list_buckets = Mock()
        mock.head_object = Mock()
        mock.put_object = Mock()
        mock.upload_file = Mock()
        return mock
    
    @pytest.fixture
    def s3_client_instance(self, mock_s3_client):
        """Create an S3_Client instance with mocked S3 client."""
        return S3_Client(mock_s3_client)
    
    def test_init(self, mock_s3_client):
        """Test S3_Client initialization."""
        client = S3_Client(mock_s3_client)
        assert client.s3 == mock_s3_client
    
    def test_list_files_success(self, s3_client_instance, mock_s3_client):
        """Test successful file listing."""
        mock_paginator = Mock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"Contents": [{"Key": "file1.txt"}, {"Key": "file2.txt"}]},
            {"Contents": [{"Key": "file3.txt"}]}
        ]
        
        result = s3_client_instance.list_files("test-bucket", "prefix/")
        
        assert result == ["file1.txt", "file2.txt", "file3.txt"]
        mock_s3_client.get_paginator.assert_called_once_with("list_objects_v2")
        mock_paginator.paginate.assert_called_once_with(Bucket="test-bucket", Prefix="prefix/")
    
    def test_list_files_no_contents(self, s3_client_instance, mock_s3_client):
        """Test file listing when no contents are found."""
        mock_paginator = Mock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{}]
        
        result = s3_client_instance.list_files("test-bucket")
        
        assert result == []
    
    def test_list_files_client_error(self, s3_client_instance, mock_s3_client, capsys):
        """Test file listing with ClientError."""
        mock_s3_client.get_paginator.side_effect = ClientError(
            error_response={"Error": {"Code": "NoSuchBucket", "Message": "Bucket not found"}},
            operation_name="list_objects_v2"
        )
        
        result = s3_client_instance.list_files("nonexistent-bucket")
        
        assert result == []
        captured = capsys.readouterr()
        assert "Error listing files:" in captured.out
    
    def test_list_buckets_success(self, s3_client_instance, mock_s3_client, capsys):
        """Test successful bucket listing."""
        mock_s3_client.list_buckets.return_value = {
            "Buckets": [
                {"Name": "bucket1"},
                {"Name": "bucket2"},
                {"Name": "bucket3"}
            ]
        }
        
        result = s3_client_instance.list_buckets()
        
        assert result == ["bucket1", "bucket2", "bucket3"]
        captured = capsys.readouterr()
        assert "Listing all S3 buckets" in captured.out
        assert "Found 3 buckets" in captured.out
    
    def test_list_buckets_empty(self, s3_client_instance, mock_s3_client):
        """Test bucket listing when no buckets exist."""
        mock_s3_client.list_buckets.return_value = {"Buckets": []}
        
        result = s3_client_instance.list_buckets()
        
        assert result == []
    
    def test_list_buckets_client_error(self, s3_client_instance, mock_s3_client, capsys):
        """Test bucket listing with ClientError."""
        mock_s3_client.list_buckets.side_effect = ClientError(
            error_response={"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
            operation_name="list_buckets"
        )
        
        result = s3_client_instance.list_buckets()
        
        assert result == []
        captured = capsys.readouterr()
        assert "Error listing buckets:" in captured.out
    
    def test_list_buckets_botocore_error(self, s3_client_instance, mock_s3_client, capsys):
        """Test bucket listing with BotoCoreError."""
        mock_s3_client.list_buckets.side_effect = BotoCoreError()
        
        result = s3_client_instance.list_buckets()
        
        assert result == []
        captured = capsys.readouterr()
        assert "Error listing buckets:" in captured.out
    
    def test_list_buckets_unexpected_error(self, s3_client_instance, mock_s3_client, capsys):
        """Test bucket listing with unexpected error."""
        mock_s3_client.list_buckets.side_effect = Exception("Unexpected error")
        
        result = s3_client_instance.list_buckets()
        
        assert result == []
        captured = capsys.readouterr()
        assert "Unexpected error listing buckets:" in captured.out
    
    @patch('s3_client.wr.s3.read_parquet')
    def test_read_parquet_success(self, mock_read_parquet, s3_client_instance, capsys):
        """Test successful parquet file reading."""
        mock_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        mock_read_parquet.return_value = mock_df
        
        result = s3_client_instance.read_parquet_to_dataframe("test-bucket", "data.parquet")
        
        pd.testing.assert_frame_equal(result, mock_df)
        mock_read_parquet.assert_called_once_with("s3://test-bucket/data.parquet")
        captured = capsys.readouterr()
        assert "✅ Loaded s3://test-bucket/data.parquet into DataFrame" in captured.out
    
    @patch('s3_client.wr.s3.read_parquet')
    def test_read_parquet_error(self, mock_read_parquet, s3_client_instance, capsys):
        """Test parquet file reading with error."""
        mock_read_parquet.side_effect = ClientError(
            error_response={"Error": {"Code": "NoSuchKey", "Message": "Key not found"}},
            operation_name="get_object"
        )
        
        result = s3_client_instance.read_parquet_to_dataframe("test-bucket", "nonexistent.parquet")
        
        assert result.empty
        captured = capsys.readouterr()
        assert "❌ Error reading s3://test-bucket/nonexistent.parquet:" in captured.out
    
    @patch('s3_client.wr.s3.to_parquet')
    def test_write_df_to_parquet_success(self, mock_to_parquet, s3_client_instance, capsys):
        """Test successful DataFrame to parquet writing."""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        
        s3_client_instance.write_df_to_parquet(df, "test-bucket", "output.parquet")
        
        mock_to_parquet.assert_called_once_with(
            df=df,
            path="s3://test-bucket/output.parquet",
            index=False,
            compression='snappy',
            boto3_session=None
        )
        captured = capsys.readouterr()
        assert "✅ Written DataFrame to parquet: s3://test-bucket/output.parquet" in captured.out
        assert "Rows: 3, Columns: 2" in captured.out
    
    @patch('s3_client.wr.s3.to_parquet')
    def test_write_df_to_parquet_custom_params(self, mock_to_parquet, s3_client_instance):
        """Test DataFrame to parquet writing with custom parameters."""
        df = pd.DataFrame({"col1": [1, 2]})
        
        s3_client_instance.write_df_to_parquet(
            df, "test-bucket", "output.parquet", compression="gzip", index=True
        )
        
        mock_to_parquet.assert_called_once_with(
            df=df,
            path="s3://test-bucket/output.parquet",
            index=True,
            compression='gzip',
            boto3_session=None
        )
    
    @patch('s3_client.wr.s3.to_parquet')
    def test_write_df_to_parquet_error(self, mock_to_parquet, s3_client_instance, capsys):
        """Test DataFrame to parquet writing with error."""
        df = pd.DataFrame({"col1": [1, 2]})
        mock_to_parquet.side_effect = ClientError(
            error_response={"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
            operation_name="put_object"
        )
        
        s3_client_instance.write_df_to_parquet(df, "test-bucket", "output.parquet")
        
        captured = capsys.readouterr()
        assert "❌ Error writing DataFrame to parquet s3://test-bucket/output.parquet:" in captured.out
    
    def test_upload_file_success(self, s3_client_instance, mock_s3_client, capsys):
        """Test successful file upload."""
        s3_client_instance.upload_file("/local/path/file.txt", "test-bucket", "remote/file.txt")
        
        mock_s3_client.upload_file.assert_called_once_with("/local/path/file.txt", "test-bucket", "remote/file.txt")
        captured = capsys.readouterr()
        assert "Uploaded /local/path/file.txt to s3://test-bucket/remote/file.txt" in captured.out
    
    def test_upload_file_error(self, s3_client_instance, mock_s3_client, capsys):
        """Test file upload with error."""
        mock_s3_client.upload_file.side_effect = ClientError(
            error_response={"Error": {"Code": "NoSuchBucket", "Message": "Bucket not found"}},
            operation_name="upload_file"
        )
        
        s3_client_instance.upload_file("/local/path/file.txt", "nonexistent-bucket", "file.txt")
        
        captured = capsys.readouterr()
        assert "Error uploading file:" in captured.out
    
    def test_get_latest_parquet_file_key_success(self, s3_client_instance, mock_s3_client):
        """Test getting latest parquet file key."""
        mock_paginator = Mock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        
        time1 = datetime(2023, 1, 1, 10, 0, 0)
        time2 = datetime(2023, 1, 1, 12, 0, 0)
        time3 = datetime(2023, 1, 1, 11, 0, 0)
        
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "ariccione/silver/estratto_fatture/PARTITION_DATE=2023-01-01/file1.parquet", "LastModified": time1},
                    {"Key": "ariccione/silver/estratto_fatture/PARTITION_DATE=2023-01-01/file2.parquet", "LastModified": time2},
                    {"Key": "ariccione/silver/estratto_fatture/PARTITION_DATE=2023-01-01/file3.parquet", "LastModified": time3}
                ]
            }
        ]
        
        result = s3_client_instance.get_latest_parquet_file_key("test-bucket", "ariccione", "2023-01-01")
        
        assert result == "ariccione/silver/estratto_fatture/PARTITION_DATE=2023-01-01/file2.parquet"
    
    def test_get_latest_parquet_file_key_no_files(self, s3_client_instance, mock_s3_client):
        """Test getting latest parquet file key when no files exist."""
        mock_paginator = Mock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{}]
        
        result = s3_client_instance.get_latest_parquet_file_key("test-bucket", "company", "2023-01-01")
        
        assert result is None
    
    def test_check_file_exists_true(self, s3_client_instance, mock_s3_client, capsys):
        """Test file existence check when file exists."""
        mock_s3_client.head_object.return_value = {}
        
        result = s3_client_instance.check_file_exists("test-bucket", "path/to/file.txt")
        
        assert result is True
        mock_s3_client.head_object.assert_called_once_with(Bucket="test-bucket", Key="path/to/file.txt")
        captured = capsys.readouterr()
        assert "✅ File exists: s3://test-bucket/path/to/file.txt" in captured.out
    
    def test_check_file_exists_false(self, s3_client_instance, mock_s3_client, capsys):
        """Test file existence check when file doesn't exist."""
        mock_s3_client.head_object.side_effect = ClientError(
            error_response={"Error": {"Code": "404", "Message": "Not Found"}},
            operation_name="head_object"
        )
        
        result = s3_client_instance.check_file_exists("test-bucket", "path/to/nonexistent.txt")
        
        assert result is False
        captured = capsys.readouterr()
        assert "❌ File not found: s3://test-bucket/path/to/nonexistent.txt" in captured.out
    
    def test_check_file_exists_error(self, s3_client_instance, mock_s3_client, capsys):
        """Test file existence check with unexpected error."""
        mock_s3_client.head_object.side_effect = ClientError(
            error_response={"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
            operation_name="head_object"
        )
        
        result = s3_client_instance.check_file_exists("test-bucket", "path/to/file.txt")
        
        assert result is False
        captured = capsys.readouterr()
        assert "❌ Error checking file existence:" in captured.out
    
    def test_create_file_success(self, s3_client_instance, mock_s3_client, capsys):
        """Test successful file creation."""
        result = s3_client_instance.create_file("test-bucket", "path/to", "file.txt", "Hello World", "text/plain")
        
        assert result == "path/to/file.txt"
        mock_s3_client.put_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="path/to/file.txt",
            Body="Hello World",
            ContentType="text/plain"
        )
        captured = capsys.readouterr()
        assert "✅ Created file: s3://test-bucket/path/to/file.txt" in captured.out
    
    def test_create_file_no_path(self, s3_client_instance, mock_s3_client):
        """Test file creation with empty path."""
        result = s3_client_instance.create_file("test-bucket", "", "file.txt")
        
        assert result == "file.txt"
        mock_s3_client.put_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="file.txt",
            Body="",
            ContentType="text/plain"
        )
    
    def test_create_file_path_without_slash(self, s3_client_instance, mock_s3_client):
        """Test file creation with path that doesn't end with slash."""
        result = s3_client_instance.create_file("test-bucket", "folder", "file.txt")
        
        assert result == "folder/file.txt"
    
    def test_create_file_error(self, s3_client_instance, mock_s3_client, capsys):
        """Test file creation with error."""
        mock_s3_client.put_object.side_effect = ClientError(
            error_response={"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
            operation_name="put_object"
        )
        
        result = s3_client_instance.create_file("test-bucket", "path", "file.txt")
        
        assert result is None
        captured = capsys.readouterr()
        assert "❌ Error creating file:" in captured.out