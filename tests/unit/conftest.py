import pytest

from firebolt_ingest.aws_settings import (
    AWSCredentials,
    AWSCredentialsRole,
    AWSSettings,
)
from firebolt_ingest.model.table import Column, Partition, Table


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


@pytest.fixture
def mock_table_partitioned_by_file():
    return Table(
        database_name="db_name",
        table_name="table_name",
        file_type="PARQUET",
        object_pattern=["*0.parquet", "*1.parquet"],
        columns=[
            Column(name="id", type="INT"),
            Column(name="name", type="TEXT"),
            Column(name="source_table_name", type="TEXT"),
            Column(name="source_table_timestamp", type="TIMESTAMP"),
        ],
        partitions=[
            Partition(column_name="source_table_name"),
            Partition(column_name="source_table_timestamp"),
        ],
    )


@pytest.fixture
def mock_table_partitioned():
    return Table(
        database_name="db_name",
        table_name="table_name",
        file_type="PARQUET",
        object_pattern=["*2.parquet", "*1.parquet"],
        columns=[
            Column(name="id", type="INT"),
            Column(name="user", type="STRING"),
            Column(name="birthdate", type="DATE"),
        ],
        partitions=[
            Partition(column_name="user"),
            Partition(column_name="birthdate", datetime_part="DAY"),
        ],
    )
