import pytest

from firebolt_ingest.aws_settings import (
    AWSCredentials,
    AWSCredentialsRole,
    AWSSettings,
)
from firebolt_ingest.model.table import Column, Table


@pytest.fixture
def mock_aws_settings():
    return AWSSettings(
        s3_url="s3://bucket-name/",
        aws_credentials=AWSCredentials(
            role_creds=AWSCredentialsRole(role_arn="role_arn")
        ),
    )


@pytest.fixture
def mock_table():
    return Table(
        database_name="db_name",
        table_name="table_name",
        file_type="PARQUET",
        object_pattern=["*0.parquet", "*1.parquet"],
        columns=[Column(name="id", type="INT"), Column(name="name", type="TEXT")],
    )
