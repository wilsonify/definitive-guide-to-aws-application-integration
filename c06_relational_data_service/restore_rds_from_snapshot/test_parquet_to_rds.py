import boto3
from moto import mock_aws

from parquet_to_rds import s3_parquet_to_dataframe

MAKE_Q = "CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);"
SELECT_Q = "SELECT * FROM test_load;"


def test_s3_bucket_with_parquet():
    """Spin up a moto3-backed S3 bucket with one Parquet file."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "test-bucket"
        prefix = "snapshot"
        key = f"small.parquet"
        s3.create_bucket(Bucket=bucket)
        s3.upload_file(f"../input/small.parquet", Bucket=bucket, Key=f"{prefix}/{key}")


def test_s3_parquet_to_dataframe():
    """read Parquet from S3 bucket"""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "test-bucket"
        prefix = "snapshot"
        key = f"small.parquet"
        s3.create_bucket(Bucket=bucket)
        s3.upload_file(
            "/mnt/SSD1/mrepos/github.com/wilsonify/definitive-guide-to-aws-application-integration/c06_relational_data_service/input/small.parquet",
            Bucket=bucket, Key=f"{prefix}/{key}")
        s3_parquet_to_dataframe(bucket=bucket, key=f"{prefix}/{key}")
