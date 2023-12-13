import copy

import pytest
import yaml

from firebolt_ingest.aws_settings import (
    AWSCredentials,
    AWSCredentialsRole,
    AWSSettings,
)
from firebolt_ingest.table_model import Column, Partition, Table


@pytest.fixture
def mock_s3_url():
    return "s3://bucket-name/"


@pytest.fixture
def mock_s3_url_1():
    return "s3://bucket-name-1/"


@pytest.fixture
def mock_aws_settings():
    return AWSSettings(
        s3_url="s3://bucket-name/",
        aws_credentials=AWSCredentials(
            role_creds=AWSCredentialsRole(role_arn="role_arn")
        ),
    )


@pytest.fixture
def mock_aws_settings_without_s3_url():
    return AWSSettings(
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
        columns=[
            Column(name="id", type="INTEGER"),
            Column(name="name", type="TEXT"),
            Column(name="name.member0", alias="aliased", type="TEXT"),
        ],
        primary_index=["id"],
    )


@pytest.fixture
def mock_table_with_s3_url(mock_table):
    mock_table_with_s3_url = copy.deepcopy(mock_table)
    mock_table_with_s3_url.s3_url = "s3://bucket-name-1/"
    return mock_table_with_s3_url


@pytest.fixture
def mock_table_partitioned_by_file():
    return Table(
        database_name="db_name",
        table_name="table_name",
        file_type="PARQUET",
        object_pattern=["*0.parquet", "*1.parquet"],
        columns=[
            Column(name="id", type="INTEGER"),
            Column(name="last_time.member0", alias="last_time", type="TIMESTAMP"),
            Column(name="source_file_name", type="TEXT"),
            Column(name="source_file_timestamp", type="TIMESTAMPNTZ"),
        ],
        partitions=[
            Partition(column_name="source_file_name"),
            Partition(column_name="source_file_timestamp"),
        ],
        primary_index=["last_time"],
    )


@pytest.fixture
def mock_table_partitioned():
    return Table(
        database_name="db_name",
        table_name="table_name",
        file_type="PARQUET",
        object_pattern=["*2.parquet", "*1.parquet"],
        columns=[
            Column(name="id", type="INTEGER"),
            Column(name="user", type="STRING"),
            Column(name="l_birthdate.member0", alias="birthdate", type="DATE"),
        ],
        partitions=[
            Partition(column_name="user"),
            Partition(column_name="birthdate", datetime_part="DAY"),
        ],
        primary_index=["id"],
    )


@pytest.fixture
def table_dict() -> dict:
    return {
        "table_name": "test_table",
        "columns": [
            {
                "name": "test_col_1",
                "type": "INTEGER",
                "extract_partition": "[^\\/]+\\/c_type=([^\\/]+)\\/[^\\/]+\\/[^\\/]+",
            },
            {"name": "test_col_2.member0", "alias": "test_col_2", "type": "TEXT"},
            {"name": "test_col_3", "type": "DATE"},
            {"name": "test_col_4-member0", "alias": "test_col_4", "type": "TEXT"},
        ],
        "primary_index": ["test_col_2"],
        "partitions": [
            {"column_name": "test_col_2"},
            {"column_name": "test_col_3", "datetime_part": "DAY"},
        ],
        "file_type": "PARQUET",
        "object_pattern": ["*0.parquet", "*1.parquet"],
        "compression": "GZIP",
        "sync_mode": "overwrite",
    }


@pytest.fixture
def table_yaml_string(table_dict) -> str:
    return yaml.dump(table_dict)
