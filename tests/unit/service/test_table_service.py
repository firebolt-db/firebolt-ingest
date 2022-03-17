from unittest.mock import MagicMock

from firebolt_ingest.aws_settings import AWSSettings
from firebolt_ingest.model.table import Column, Table
from firebolt_ingest.service.table import TableService, generate_columns_string


def test_generate_columns_string():
    """
    test generate columns string with 0, 1 and multiple columns
    """
    assert generate_columns_string([]) == ""

    assert generate_columns_string([Column(name="id", type="TEXT")]) == "id TEXT"

    assert (
        generate_columns_string(
            [Column(name="id", type="TEXT"), Column(name="part", type="INT")]
        )
        == "id TEXT, part INT"
    )


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

    ts = TableService(connection, mock_aws_settings)
    ts.create_external_table(mock_table)

    cursor_mock.execute.assert_called_once_with(
        "CREATE EXTERNAL TABLE IF NOT EXISTS table_name "
        "(id INT, name TEXT) "
        "CREDENTIALS = (AWS_ROLE_ARN = ?) "
        "URL = ? "
        "OBJECT_PATTERN = ? "
        "TYPE = (PARQUET)",
        ["role_arn", "s3://bucket-name/", "*.parquet"],
    )
