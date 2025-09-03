"""
Load a directory of Parquet files from S3 into a PostgreSQL RDS instance.
"""
import os
from io import BytesIO
from pprint import pprint
from urllib.parse import quote_plus

import pandas as pd
from boto3 import Session
from sqlalchemy import create_engine

# === CONFIGURATION (defaults from env) ===
DEFAULT_SOURCE_S3_BUCKET = os.getenv("SOURCE_S3_BUCKET", "your-bucket-name")
DEFAULT_SOURCE_S3_PREFIX = os.getenv("SOURCE_S3_PREFIX", "parquet-snapshot/")
PGHOST = os.getenv("PGHOST", "your-rds-endpoint.rds.amazonaws.com")
PGPORT = int(os.getenv("PGPORT", 5432))
PGUSER = os.getenv("PGUSER", "your-user")
PGPASSWORD = os.getenv("PGPASSWORD", "your-password")
PGDATABASE = os.getenv("PGDATABASE", "your-database")

s3_session = Session()
s3 = s3_session.client('s3')


def list_parquet_files(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                yield obj["Key"]


def s3_parquet_to_dataframe(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response['Body']
    body_bytes = body.read()
    buffer = BytesIO(body_bytes)
    return pd.read_parquet(buffer)


def process_parquet_files(bucket, prefix):
    connection_url = (
        f"postgresql+psycopg2://{PGUSER}:{quote_plus(PGPASSWORD)}@{PGHOST}:{PGPORT}/{PGDATABASE}"
    )
    engine = create_engine(connection_url)

    results = []
    for key in list_parquet_files(bucket, prefix):
        table_name = os.path.splitext(os.path.basename(key))[0]
        print(f"Processing {key} → {table_name}")

        df = s3_parquet_to_dataframe(bucket, key)
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",
            index=False,
            chunksize=1000,
            method="multi",
        )

        msg = f"Loaded {len(df)} rows into {table_name}"
        print(msg)
        results.append({"table": table_name, "rows": len(df)})

    engine.dispose()
    return results


def lambda_handler(event, context):
    """
    AWS Lambda entrypoint.
    Event can specify:
      {
        "bucket": "my-bucket",
        "prefix": "my-prefix/"
      }
    Falls back to environment defaults if not provided.
    """
    print("event=")
    pprint(event)
    print("context=")
    pprint(context)
    bucket = event.get("bucket", DEFAULT_SOURCE_S3_BUCKET)
    prefix = event.get("prefix", DEFAULT_SOURCE_S3_PREFIX)

    print(f"Starting parquet → Postgres load from s3://{bucket}/{prefix}")
    results = process_parquet_files(bucket, prefix)

    return {
        "statusCode": 200,
        "body": {
            "processed_files": len(results),
            "details": results,
        },
    }
