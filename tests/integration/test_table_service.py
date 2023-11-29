from typing import List

import pytest
from firebolt.common.exception import FireboltError
from firebolt.db import Cursor

from firebolt_ingest.aws_settings import AWSSettings
from firebolt_ingest.table_model import Column, Table
from firebolt_ingest.table_service import TableService
from firebolt_ingest.table_utils import drop_table
from firebolt_ingest.utils import format_query


@pytest.fixture
def remove_all_tables_teardown(connection):
    """

    Args:
        connection:

    Returns:

    """
    yield
    cursor = connection.cursor()

    cursor.execute(
        "SELECT table_name from information_schema.tables WHERE table_schema = 'public'"
    )

    for table_name in cursor.fetchall():
        drop_table(cursor, table_name[0])


def check_columns(cursor: Cursor, table_name: str, columns_expected: List[Column]):
    def normalize_type(t: str) -> str:
        type_mappings = {
            "TEXT": "STRING",
            "TIMESTAMPNTZ": "TIMESTAMP",
            "PGDATE": "DATE",
            "DATETIME": "TIMESTAMP",
        }
        return type_mappings.get(t.upper(), t.upper())

    cursor.execute(
        "SELECT column_name, data_type "
        "FROM information_schema.columns "
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


def test_create_internal_table(
    connection, mock_table: Table, remove_all_tables_teardown
):
    """create fact table and verify the correctness of the columns"""

    TableService(mock_table, connection).create_internal_table(add_file_metadata=False)

    cursor = connection.cursor()
    check_columns(cursor, mock_table.table_name, mock_table.columns)


def test_create_internal_table_with_meta(
    connection, mock_table: Table, remove_all_tables_teardown
):
    """create fact table with meta columns and verify it"""

    TableService(mock_table, connection).create_internal_table(add_file_metadata=True)

    cursor = connection.cursor()
    check_columns(
        cursor,
        mock_table.table_name,
        mock_table.columns
        + [
            Column(name="source_file_name", type="TEXT"),
            Column(name="source_file_timestamp", type="TIMESTAMPNTZ"),
        ],
    )


def test_timestamps_table_with_meta(
    connection, mock_table_timestamps: Table, remove_all_tables_teardown
):
    """create fact table with meta columns and verify it"""

    TableService(mock_table_timestamps, connection).create_internal_table(
        add_file_metadata=True
    )

    cursor = connection.cursor()
    check_columns(
        cursor,
        mock_table_timestamps.table_name,
        mock_table_timestamps.columns
        + [
            Column(name="source_file_name", type="TEXT"),
            Column(name="source_file_timestamp", type="TIMESTAMPNTZ"),
        ],
    )

    insert_query = f"""
                       INSERT INTO {mock_table_timestamps.table_name}
                       VALUES(1, '2020-07-29 09:01:00.812',
                       '2020-07-29 09:01:00.812',
                       '2020-07-29 09:01:00.812+00',
                       '2020-07-29 09:01:00.812',
                       '2020-07-29',
                       '2020-07-29',
                       'filename',
                       '2020-07-29 09:01:00.812'),
                       (2,
                       '2020-07-29 09:01:00.812',
                       '2020-07-29 09:01:00.812',
                       '2020-07-29 09:01:00.812',
                       '2020-07-29 09:01:00.812','2020-07-29',
                       '2020-07-29',
                       'filename',
                       '2020-07-29 09:01:00.812');
                       """

    cursor.execute(query=format_query(insert_query))

    # insert zone timestamp '2020-07-29 09:01:00.812+00' to TIMESTAMPNTZ
    with pytest.raises(FireboltError):

        insert_query = f"""
                       INSERT INTO {mock_table_timestamps.table_name}
                       VALUES(3,
                       '2020-07-29 09:01:00.812',
                       '2020-07-29 09:01:00.812+00',
                       '2020-07-29 09:01:00.812+00',
                       '2020-07-29 09:01:00.812',
                       '2020-07-29',
                       '2020-07-29',
                       'filename',
                       '2020-07-29 09:01:00.812');
                       """
        cursor.execute(query=format_query(insert_query))


def test_create_internal_table_twice(
    connection, mock_table: Table, remove_all_tables_teardown
):
    """create fact table twice and ensure,
    that the second time it fails with an exception"""

    ts = TableService(mock_table, connection)

    ts.create_internal_table()

    with pytest.raises(FireboltError):
        ts.create_internal_table()


def test_create_external_table(connection, remove_all_tables_teardown):
    """create external table from the getting started example"""

    s3_url = "s3://firebolt-publishing-public/samples/tpc-h/parquet/lineitem/"

    table_name = "lineitem_ingest_integration"
    columns = [
        Column(name="l_orderkey", type="BIGINT"),
        Column(name="l_partkey", type="BIGINT"),
    ]
    ts = TableService(
        Table(
            table_name=table_name,
            columns=columns,
            file_type="PARQUET",
            object_pattern=["*.parquet"],
            primary_index=["l_orderkey"],
        ),
        connection,
    )

    ts.create_external_table(
        aws_settings=AWSSettings(s3_url=s3_url),
    )

    check_columns(connection.cursor(), f"ex_{table_name}", columns)


def test_create_external_table_twice(connection, remove_all_tables_teardown):
    """create external table twice, and verify,
    that an exception will be raised on the second call"""

    s3_url = "s3://firebolt-publishing-public/samples/tpc-h/parquet/lineitem/"
    aws_settings = AWSSettings(s3_url=s3_url)

    table_name = "lineitem_ingest_integration"
    table = Table(
        table_name=table_name,
        columns=[
            Column(name="l_orderkey", type="BIGINT"),
            Column(name="l_partkey", type="BIGINT"),
        ],
        file_type="PARQUET",
        object_pattern=["*.parquet"],
        primary_index=["l_orderkey"],
    )
    ts = TableService(table, connection)

    ts.create_external_table(aws_settings)

    with pytest.raises(FireboltError):
        ts.create_external_table(aws_settings)


def test_ingestion_full_overwrite(
    mock_table: Table, s3_url: str, connection, remove_all_tables_teardown
):
    """
    Happy path
    """
    ts = TableService(mock_table, connection)

    ts.create_external_table(AWSSettings(s3_url=s3_url))
    ts.create_internal_table()

    ts.insert_full_overwrite()

    assert ts.verify_ingestion()


def test_ingestion_full_overwrite_twice(
    mock_table: Table, s3_url: str, connection, remove_all_tables_teardown
):
    """
    Do full overwrite of the internal table twice,
    validating the ingestion after each overwrite
    """
    ts = TableService(mock_table, connection)
    connection.cursor()

    ts.create_external_table(AWSSettings(s3_url=s3_url))
    ts.create_internal_table()

    ts.insert_full_overwrite()
    assert ts.verify_ingestion()

    ts.insert_full_overwrite()
    assert ts.verify_ingestion()


def test_ingestion_append(
    mock_table: Table, s3_url: str, connection, remove_all_tables_teardown
):
    """
    Do incremental append, when the internal table is empty,
    the result should be the same as in full overwrite
    """
    # Create a partial sub table
    mock_table.object_pattern = ["*1.parquet"]
    ts_sub = TableService(mock_table, connection)
    ts_sub.create_internal_table()
    ts_sub.create_external_table(AWSSettings(s3_url=s3_url))

    # First append from small table
    ts_sub.insert_incremental_append()
    assert ts_sub.verify_ingestion()
    drop_table(connection.cursor(), f"ex_{mock_table.table_name}")

    # Second append from full table
    mock_table.object_pattern = ["*.parquet"]
    ts = TableService(mock_table, connection)
    ts.create_external_table(AWSSettings(s3_url=s3_url))
    ts.insert_incremental_append()
    assert ts.verify_ingestion()


# This test doesn't raise an exception because raise_on_tables_non_compatibility
# was commented out in DATA-2138
# !TODO: uncomment it after FIR-26886 will be fixed
#
# def test_ingestion_incompatible_schema(
#    mock_table: Table, s3_url: str, connection, remove_all_tables_teardown
# ):
#    """
#    try ingestion with full overwrite, expect an exception
#    and verify the original table is not destroyed
#    """
#    ts1 = TableService(mock_table, connection)
#
#    ts1.create_internal_table()
#
#    mock_table.columns[0].name += "_non_compatible"
#    ts2 = TableService(mock_table, connection)
#    ts2.create_external_table(AWSSettings(s3_url=s3_url))
#
#    cursor = connection.cursor()
#    cursor.execute(
#        f"INSERT INTO {mock_table.table_name} "
#        f"VALUES (0, 0, 0, 0, 0, 0, 0, 0 , "
#        f"'', '', '', '', '', '', '', '', '', '2020-10-26 10:14:15')"
#    )
#
#    with pytest.raises(FireboltError):
#        ts1.insert_full_overwrite()
#
#    cursor.execute(query=f"SELECT count(*) FROM {mock_table.table_name}")
#
#    data = cursor.fetchall()
#    assert data[0][0] == 1

# This test doesn't raise an expected exception about columns
# because raise_on_tables_non_compatibility was commented out in DATA-2138
# Currently it raises: The INSERT statement failed because the number
# of target columns (16) does not match the number of inputs specified (18)
# !TODO: uncomment it after FIR-26886 will be fixed
# def test_ingestion_append_nometadata(
#    mock_table: Table, s3_url: str, connection, remove_all_tables_teardown
# ):
#    """
#    try ingestion with full overwrite, expect an exception
#    and verify the original table is not destroyed
#    """
#    ts = TableService(mock_table, connection)
#
#    ts.create_internal_table(add_file_metadata=False)
#
#    ts.create_external_table(AWSSettings(s3_url=s3_url))
#
#    with pytest.raises(FireboltError) as err:
#        ts.insert_incremental_append()

#    assert "source_file_name" in str(err)
#    assert "source_file_timestamp" in str(err)
