from typing import List

import pytest
from firebolt.common.exception import FireboltError
from firebolt.db import Cursor

from firebolt_ingest.aws_settings import AWSSettings
from firebolt_ingest.model.table import Column, Table
from firebolt_ingest.service.table import TableService


def check_columns(cursor: Cursor, table_name: str, columns: List[Column]):
    cursor.execute(
        "SELECT column_name, data_type "
        "FROM INFORMATION_SCHEMA.columns "
        "WHERE table_name = ?",
        [table_name],
    )
    data = cursor.fetchall()
    columns_plain = [[column.name, column.type] for column in columns]

    assert sorted(data, key=lambda x: x[0]) == sorted(
        columns_plain, key=lambda x: x[0]
    ), "Expected columns and columns from table don't match"


def test_create_internal_table(connection, mock_table: Table):
    """create fact table and verify the correctness of the columns"""

    TableService(connection).create_internal_table(
        table=mock_table, add_file_metadata=False
    )

    cursor = connection.cursor()
    check_columns(cursor, mock_table.table_name, mock_table.columns)

    cursor.execute(f"DROP TABLE {mock_table.table_name}")


def test_create_internal_table_with_meta(connection, mock_table: Table):
    """create fact table with meta columns and verify it"""

    TableService(connection).create_internal_table(
        table=mock_table, add_file_metadata=True
    )

    cursor = connection.cursor()
    check_columns(
        cursor,
        mock_table.table_name,
        mock_table.columns
        + [
            Column(name="source_file_name", type="TEXT"),
            Column(name="source_file_timestamp", type="TIMESTAMP"),
        ],
    )

    cursor.execute(f"DROP TABLE {mock_table.table_name}")


def test_create_internal_table_twice(connection, mock_table: Table):
    """create fact table twice and ensure,
    that the second time it fails with an exception"""

    ts = TableService(connection)

    ts.create_internal_table(table=mock_table)

    with pytest.raises(FireboltError):
        ts.create_internal_table(table=mock_table)

    connection.cursor().execute(f"DROP TABLE {mock_table.table_name}")


def test_create_external_table(connection):
    """create external table from the getting started example"""

    s3_url = "s3://firebolt-publishing-public/samples/tpc-h/parquet/lineitem/"

    ts = TableService(connection)
    table_name = "ex_lineitem_ingest_integration"
    columns = [
        Column(name="l_orderkey", type="LONG"),
        Column(name="l_partkey", type="LONG"),
    ]

    ts.create_external_table(
        table=Table(
            table_name=table_name,
            columns=columns,
            file_type="PARQUET",
            object_pattern=["*.parquet"],
        ),
        aws_settings=AWSSettings(s3_url=s3_url),
    )

    cursor = connection.cursor()
    check_columns(cursor, table_name, columns)

    cursor.execute(f"DROP TABLE {table_name}")


def test_create_external_table_twice(connection):
    """create external table twice, and verify,
    that an exception will be raised on the second call"""

    ts = TableService(connection)

    s3_url = "s3://firebolt-publishing-public/samples/tpc-h/parquet/lineitem/"
    aws_settings = AWSSettings(s3_url=s3_url)

    table_name = "ex_lineitem_ingest_integration"
    table = Table(
        table_name=table_name,
        columns=[
            Column(name="l_orderkey", type="LONG"),
            Column(name="l_partkey", type="LONG"),
        ],
        file_type="PARQUET",
        object_pattern=["*.parquet"],
    )

    ts.create_external_table(table, aws_settings)

    with pytest.raises(FireboltError):
        ts.create_external_table(table, aws_settings)

    connection.cursor().execute(f"DROP TABLE {table_name}")
