from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from firebolt_ingest.aws_settings import AWSSettings
from firebolt_ingest.table_model import Table
from firebolt_ingest.table_service import TableService
from firebolt_ingest.utils import format_query


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
        format_query(
            """CREATE EXTERNAL TABLE ex_table_name
                        (id INT, name TEXT)
                        CREDENTIALS = (AWS_ROLE_ARN = ?)
                        URL = ?
                        OBJECT_PATTERN = ?, ?
                        TYPE = (PARQUET)
                        COMPRESSION = GZIP"""
        ),
        ["role_arn", "s3://bucket-name/", "*0.parquet", "*1.parquet"],
    )


def test_create_external_table_json(mock_aws_settings: AWSSettings, mock_table: Table):
    """
    call create external table and check,
    that the correct query is being passed to cursor
    """
    connection = MagicMock()
    cursor_mock = MagicMock()
    connection.cursor.return_value = cursor_mock

    mock_table.file_type = "JSON"
    mock_table.json_parse_as_text = True

    ts = TableService(connection)
    ts.create_external_table(mock_table, mock_aws_settings)

    cursor_mock.execute.assert_called_once_with(
        format_query(
            """CREATE EXTERNAL TABLE ex_table_name
                        (id INT, name TEXT)
                        CREDENTIALS = (AWS_ROLE_ARN = ?)
                        URL = ?
                        OBJECT_PATTERN = ?, ?
                        TYPE = (JSON PARSE_AS_TEXT = 'TRUE')"""
        ),
        ["role_arn", "s3://bucket-name/", "*0.parquet", "*1.parquet"],
    )


def test_create_external_table_csv(mock_aws_settings: AWSSettings, mock_table: Table):
    """
    call create external table and check,
    that the correct query is being passed to cursor
    """
    connection = MagicMock()
    cursor_mock = MagicMock()
    connection.cursor.return_value = cursor_mock

    mock_table.file_type = "CSV"
    mock_table.csv_skip_header_row = False

    ts = TableService(connection)
    ts.create_external_table(mock_table, mock_aws_settings)

    cursor_mock.execute.assert_called_once_with(
        format_query(
            """CREATE EXTERNAL TABLE ex_table_name
                        (id INT, name TEXT)
                        CREDENTIALS = (AWS_ROLE_ARN = ?)
                        URL = ?
                        OBJECT_PATTERN = ?, ?
                        TYPE = (CSV SKIP_HEADER_ROWS = 0)"""
        ),
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
        format_query(
            """CREATE FACT TABLE table_name
                        (id INT, user STRING, birthdate DATE,
                        source_file_name STRING, source_file_timestamp DATETIME)
                        PRIMARY INDEX id
                        PARTITION BY user,EXTRACT(DAY FROM birthdate)"""
        ),
        [],
    )


def test_insert_full_overwrite(
    mock_aws_settings: AWSSettings, mocker: MockerFixture, mock_table: Table
):
    """
    Call insert full overwrite and check
    that the correct drop & insert into queries are passed to the cursor
    """
    connection = MagicMock()
    cursor_mock = MagicMock()
    cursor_mock.execute.return_value = 0
    connection.cursor.return_value = cursor_mock

    get_table_schema_mock = mocker.patch(
        "firebolt_ingest.table_service.get_table_schema"
    )
    get_table_schema_mock.return_value = "create_fact_table_request"

    get_table_columns_mock = mocker.patch(
        "firebolt_ingest.table_service.get_table_columns"
    )
    get_table_columns_mock.return_value = ["id", "name"]

    ts = TableService(connection)
    ts.create_internal_table = MagicMock()
    ts.insert_full_overwrite(
        internal_table_name="internal_table_name",
        external_table_name="external_table_name",
    )

    cursor_mock.execute.assert_any_call(query="create_fact_table_request")
    cursor_mock.execute.assert_any_call(
        query="DROP TABLE IF EXISTS internal_table_name CASCADE"
    )
    cursor_mock.execute.assert_any_call(
        query=format_query(
            """INSERT INTO internal_table_name
               SELECT id, name
               FROM external_table_name"""
        )
    )

    get_table_schema_mock.assert_called_once()
    get_table_columns_mock.assert_called_once()


def test_insert_incremental_append(
    mock_aws_settings: AWSSettings, mocker: MockerFixture, mock_table: Table
):
    """
    Happy path of insert incremental append
    """

    connection = MagicMock()
    cursor_mock = MagicMock()
    cursor_mock.execute.return_value = 0
    connection.cursor.return_value = cursor_mock

    does_table_exists_mock = mocker.patch(
        "firebolt_ingest.table_service.does_table_exist", return_value=[True, True]
    )

    ts = TableService(connection)
    ts.insert_incremental_append(
        internal_table_name="internal_table_name",
        external_table_name="external_table_name",
    )

    cursor_mock.execute.assert_any_call(
        query=format_query(
            """
            INSERT INTO internal_table_name
            SELECT *, source_file_name, source_file_timestamp
            FROM external_table_name
            WHERE (source_file_name, source_file_timestamp) NOT IN (
                SELECT DISTINCT source_file_name, source_file_timestamp
                FROM internal_table_name)"""
        )
    )

    does_table_exists_mock.assert_any_call(cursor_mock, "external_table_name")
    does_table_exists_mock.assert_any_call(cursor_mock, "internal_table_name")
