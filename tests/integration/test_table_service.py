from typing import List

import pytest
from firebolt.common.exception import FireboltError
from firebolt.db import Cursor

from firebolt_ingest.aws_settings import AWSSettings
from firebolt_ingest.table_model import Column, Table
from firebolt_ingest.table_service import TableService
from firebolt_ingest.table_utils import drop_table


def check_columns(cursor: Cursor, table_name: str, columns_expected: List[Column]):
    def normalize_type(t: str) -> str:
        t = t.upper()
        if t == "TEXT":
            return "STRING"

        return t

    cursor.execute(
        "SELECT column_name, data_type "
        "FROM INFORMATION_SCHEMA.columns "
        "WHERE table_name = ?",
        [table_name],
    )
    data = cursor.fetchall()
    columns_expected = [
        [column.name, normalize_type(column.type)] for column in columns_expected
    ]
    columns_actual = [[column[0], normalize_type(column[1])] for column in data]

    assert sorted(columns_actual, key=lambda x: x[0]) == sorted(
        columns_expected, key=lambda x: x[0]
    ), "Expected columns and columns from table don't match"


def test_create_internal_table(connection, mock_table: Table):
    """create fact table and verify the correctness of the columns"""

    TableService(connection).create_internal_table(
        table=mock_table, add_file_metadata=False
    )

    cursor = connection.cursor()
    check_columns(cursor, mock_table.table_name, mock_table.columns)

    drop_table(connection.cursor(), mock_table.table_name)


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

    drop_table(connection.cursor(), mock_table.table_name)


def test_create_internal_table_twice(connection, mock_table: Table):
    """create fact table twice and ensure,
    that the second time it fails with an exception"""

    ts = TableService(connection)

    ts.create_internal_table(table=mock_table)

    with pytest.raises(FireboltError):
        ts.create_internal_table(table=mock_table)

    drop_table(connection.cursor(), mock_table.table_name)


def test_create_external_table(connection):
    """create external table from the getting started example"""

    s3_url = "s3://firebolt-publishing-public/samples/tpc-h/parquet/lineitem/"

    ts = TableService(connection)
    table_name = "lineitem_ingest_integration"
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
            primary_index=["l_orderkey"],
        ),
        aws_settings=AWSSettings(s3_url=s3_url),
    )

    check_columns(connection.cursor(), f"ex_{table_name}", columns)
    drop_table(connection.cursor(), f"ex_{table_name}")


def test_create_external_table_twice(connection):
    """create external table twice, and verify,
    that an exception will be raised on the second call"""

    ts = TableService(connection)

    s3_url = "s3://firebolt-publishing-public/samples/tpc-h/parquet/lineitem/"
    aws_settings = AWSSettings(s3_url=s3_url)

    table_name = "lineitem_ingest_integration"
    table = Table(
        table_name=table_name,
        columns=[
            Column(name="l_orderkey", type="LONG"),
            Column(name="l_partkey", type="LONG"),
        ],
        file_type="PARQUET",
        object_pattern=["*.parquet"],
        primary_index=["l_orderkey"],
    )

    ts.create_external_table(table, aws_settings)

    with pytest.raises(FireboltError):
        ts.create_external_table(table, aws_settings)

    drop_table(connection.cursor(), f"ex_{table.table_name}")


def test_ingestion_full_overwrite(mock_table: Table, s3_url: str, connection):
    """
    Happy path
    """
    ts = TableService(connection)

    ts.create_external_table(mock_table, AWSSettings(s3_url=s3_url))
    ts.create_internal_table(mock_table)

    ts.insert_full_overwrite(
        internal_table_name=mock_table.table_name,
        external_table_name=f"ex_{mock_table.table_name}",
        firebolt_dont_wait_for_upload_to_s3=True,
    )

    assert ts.verify_ingestion(mock_table.table_name, f"ex_{mock_table.table_name}")

    drop_table(connection.cursor(), mock_table.table_name)
    drop_table(connection.cursor(), f"ex_{mock_table.table_name}")


def test_ingestion_full_overwrite_twice(mock_table: Table, s3_url: str, connection):
    """
    Do full overwrite of the internal table twice,
    validating the ingestion after each overwrite
    """
    ts = TableService(connection)
    connection.cursor()

    ts.create_external_table(mock_table, AWSSettings(s3_url=s3_url))
    ts.create_internal_table(mock_table)

    ts.insert_full_overwrite(
        internal_table_name=mock_table.table_name,
        external_table_name=f"ex_{mock_table.table_name}",
        firebolt_dont_wait_for_upload_to_s3=True,
    )
    assert ts.verify_ingestion(mock_table.table_name, f"ex_{mock_table.table_name}")

    ts.insert_full_overwrite(
        internal_table_name=mock_table.table_name,
        external_table_name=f"ex_{mock_table.table_name}",
        firebolt_dont_wait_for_upload_to_s3=True,
    )
    assert ts.verify_ingestion(mock_table.table_name, f"ex_{mock_table.table_name}")

    drop_table(connection.cursor(), mock_table.table_name)
    drop_table(connection.cursor(), f"ex_{mock_table.table_name}")


def test_ingestion_incompatible_schema(mock_table: Table, s3_url: str, connection):
    """
    Validate, that if the schemes of external and internal tables do not match
    the insert_full_overwrite raises an exception
    """
    ts = TableService(connection)

    ts.create_external_table(mock_table, AWSSettings(s3_url=s3_url))
    mock_table.columns.append(Column(name="non_existing_column", type="TEXT"))

    with pytest.raises(FireboltError):
        ts.insert_full_overwrite(
            internal_table_name=mock_table.table_name,
            external_table_name=f"ex_{mock_table.table_name}",
            firebolt_dont_wait_for_upload_to_s3=True,
        )

    drop_table(connection.cursor(), f"ex_{mock_table.table_name}")


def test_ingestion_append(mock_table: Table, s3_url: str, connection):
    """
    Do incremental append, when the internal table is empty,
    the result should be the same as in full overwrite
    """
    ts = TableService(connection)

    connection.cursor()

    int_table_name = mock_table.table_name
    ext_table_name = f"ex_{mock_table.table_name}"
    ts.create_internal_table(mock_table)
    ts.create_external_table(mock_table, AWSSettings(s3_url=s3_url))

    # Create a partial sub table
    mock_table.table_name = "sub_lineitem"
    mock_table.object_pattern = ["*1.parquet"]
    ts.create_external_table(mock_table, AWSSettings(s3_url=s3_url))

    # First append from small table
    ts.insert_incremental_append(
        internal_table_name=int_table_name,
        external_table_name=f"ex_sub_lineitem",
        firebolt_dont_wait_for_upload_to_s3=True,
    )
    assert ts.verify_ingestion(int_table_name, f"ex_sub_lineitem")

    # Second append from full table
    ts.insert_incremental_append(
        internal_table_name=int_table_name,
        external_table_name=ext_table_name,
        firebolt_dont_wait_for_upload_to_s3=True,
    )
    assert ts.verify_ingestion(int_table_name, ext_table_name)

    # Try to append from the small table again
    ts.insert_incremental_append(
        internal_table_name=int_table_name,
        external_table_name=f"ex_sub_lineitem",
        firebolt_dont_wait_for_upload_to_s3=True,
    )
    assert ts.verify_ingestion(int_table_name, ext_table_name)

    # drop all test tables
    for table_name in [int_table_name, ext_table_name, "ex_sub_lineitem"]:
        drop_table(connection.cursor(), table_name)
