from unittest.mock import MagicMock

import sqlparse

from firebolt_ingest.aws_settings import AWSSettings
from firebolt_ingest.model.table import Table
from firebolt_ingest.service.table import TableService


def test_create_external_table_happy_path(
    mock_aws_settings: AWSSettings, mock_table: Table
):
    """
    call create external table and check,
    that the correct query is being passed to cursor
    """
    connection = MagicMock()
    cursor_mock = MagicMock()
    connection.cursor.return_value = cursor_mock

    ts = TableService(connection)
    mock_table.compression = "GZIP"
    ts.create_external_table(mock_table, mock_aws_settings)

    cursor_mock.execute.assert_called_once_with(
        "CREATE EXTERNAL TABLE table_name\n"
        "(id INT, name TEXT)\n"
        "CREDENTIALS = (AWS_ROLE_ARN = ?)\n"
        "URL = ?\n"
        "OBJECT_PATTERN = ?, ?\n"
        "TYPE = (PARQUET)\n"
        "COMPRESSION = GZIP\n",
        ["role_arn", "s3://bucket-name/", "*0.parquet", "*1.parquet"],
    )


def test_create_internal_table_happy_path(
    mock_aws_settings: AWSSettings, mock_table: Table, mock_table_partitioned: Table
):
    """
    call create internal table and check,
    that the correct query is being passed to cursor
    """
    connection = MagicMock()
    cursor_mock = MagicMock()
    connection.cursor.return_value = cursor_mock

    ts = TableService(connection)
    ts.create_internal_table(mock_table_partitioned, mock_aws_settings)

    cursor_mock.execute.assert_called_once_with(
        "CREATE FACT TABLE table_name\n"
        "(id INT, user STRING, birthdate DATE, "
        "source_file_name STRING, source_file_timestamp DATETIME)\n"
        "PARTITION BY user,EXTRACT(DAY FROM birthdate),"
        "source_file_name,source_file_timestamp\n",
        [],
    )


def test_insert_full_overwrite(mock_aws_settings: AWSSettings, mock_table: Table):
    """
    Call insert full overwrite and check
    that the correct drop & insert into queries are passed to the cursor
    """
    connection = MagicMock()
    cursor_mock = MagicMock()
    cursor_mock.execute.return_value = 0
    connection.cursor.return_value = cursor_mock

    ts = TableService(connection)
    ts.create_internal_table = MagicMock()
    ts.insert_full_overwrite(
        internal_table=mock_table,
        external_table_name="external_table_name",
        where_sql="1=1",
    )

    cursor_mock.execute.assert_any_call(query="DROP TABLE IF EXISTS table_name CASCADE")

    ts.create_internal_table.assert_called_once_with(table=mock_table)

    expected_query = sqlparse.format(
        f"INSERT INTO table_name "
        f"SELECT id, name "
        f"FROM external_table_name "
        f"WHERE 1=1",
        reindent=True,
        indent_width=4,
    )

    cursor_mock.execute.assert_called_with(query=expected_query)
