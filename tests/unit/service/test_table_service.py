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
    ts.create_external_table(mock_table, mock_aws_settings)

    cursor_mock.execute.assert_called_once_with(
        "CREATE EXTERNAL TABLE IF NOT EXISTS table_name "
        "(id INT, name TEXT) "
        "CREDENTIALS = (AWS_ROLE_ARN = ?) "
        "URL = ? "
        "OBJECT_PATTERN = ? "
        "TYPE = (PARQUET)",
        ["role_arn", "s3://bucket-name/", "*.parquet"],
    )


def test_insert_full_overwrite(mock_table: Table):
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
        f"SELECT id INT, name TEXT "
        f"FROM external_table_name "
        f"WHERE 1=1",
        reindent=True,
        indent_width=4,
    )

    cursor_mock.execute.assert_called_with(query=expected_query)
