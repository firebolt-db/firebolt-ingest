from unittest.mock import MagicMock

import pytest
from firebolt.common.exception import FireboltError
from pytest_mock import MockerFixture

from firebolt_ingest.aws_settings import AWSSettings
from firebolt_ingest.table_model import Table
from firebolt_ingest.table_service import TableService
from firebolt_ingest.utils import format_query


@pytest.mark.parametrize(
    "aws_settings, table, s3_url",
    [
        ("mock_aws_settings", "mock_table", "mock_s3_url"),
        ("mock_aws_settings_without_s3_url", "mock_table_with_s3_url", "mock_s3_url_1"),
        ("mock_aws_settings", "mock_table_with_s3_url", "mock_s3_url_1"),
    ],
)
def test_create_external_table_happy_path(
    aws_settings: str, table: str, s3_url: str, request
):
    """
    call create external table and check,
    that the correct query is being passed to cursor
    This function runs a few times with parameters from pytest.mark.parametrize
    aws_settings, table and s3_url in this case contains names(strings) of the mocks,
    so you should use request.getfixturevalue to get mock's value itself
    """
    connection = MagicMock()
    cursor_mock = MagicMock()
    connection.cursor.return_value = cursor_mock

    tb = request.getfixturevalue(table)
    ts = TableService(tb, connection)
    tb.compression = "GZIP"
    ts.create_external_table(request.getfixturevalue(aws_settings))

    cursor_mock.execute.assert_called_once_with(
        format_query(
            """CREATE EXTERNAL TABLE ex_table_name
                        ("id" INTEGER, "name" TEXT, "name.member0" TEXT)
                        CREDENTIALS = (AWS_ROLE_ARN = ?)
                        URL = ?
                        OBJECT_PATTERN = ?
                        TYPE = (PARQUET)
                        COMPRESSION = GZIP"""
        ),
        ["role_arn", request.getfixturevalue(s3_url), "*0.parquet"],
    )


def test_create_external_table_without_s3_url(
    mock_aws_settings_without_s3_url: AWSSettings, mock_table: Table
):
    """
    call create external table and check if it fails with FireboltError
    because s3_url isn't provided
    """
    connection = MagicMock()
    cursor_mock = MagicMock()
    connection.cursor.return_value = cursor_mock

    mock_table.file_type = "JSON"
    mock_table.json_parse_as_text = True

    ts = TableService(mock_table, connection)
    with pytest.raises(FireboltError):
        ts.create_external_table(mock_aws_settings_without_s3_url)


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

    ts = TableService(mock_table, connection)
    ts.create_external_table(mock_aws_settings)

    cursor_mock.execute.assert_called_once_with(
        format_query(
            """CREATE EXTERNAL TABLE ex_table_name
                        ("id" INTEGER, "name" TEXT, "name.member0" TEXT)
                        CREDENTIALS = (AWS_ROLE_ARN = ?)
                        URL = ?
                        OBJECT_PATTERN = ?
                        TYPE = (JSON PARSE_AS_TEXT = 'TRUE')"""
        ),
        ["role_arn", "s3://bucket-name/", "*0.parquet"],
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

    ts = TableService(mock_table, connection)
    ts.create_external_table(mock_aws_settings)

    cursor_mock.execute.assert_called_once_with(
        format_query(
            """CREATE EXTERNAL TABLE ex_table_name
                        ("id" INTEGER, "name" TEXT, "name.member0" TEXT)
                        CREDENTIALS = (AWS_ROLE_ARN = ?)
                        URL = ?
                        OBJECT_PATTERN = ?
                        TYPE = (CSV SKIP_HEADER_ROWS = 0)"""
        ),
        ["role_arn", "s3://bucket-name/", "*0.parquet"],
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

    ts = TableService(mock_table_partitioned, connection)
    ts.create_internal_table(mock_aws_settings)

    cursor_mock.execute.assert_called_once_with(
        format_query(
            """CREATE FACT TABLE table_name
                        (id INTEGER, user STRING, birthdate DATE,
                        source_file_name TEXT, source_file_timestamp TIMESTAMPNTZ)
                        PRIMARY INDEX id
                        PARTITION BY user,EXTRACT(DAY FROM birthdate)"""
        ),
        [],
    )


def test_create_internal_table_happy_path_with_prefix(
    mock_aws_settings: AWSSettings, mock_table: Table, mock_table_partitioned: Table
):
    """
    call create internal table with internal_prefix and check,
    that the correct query is being passed to cursor
    """
    connection = MagicMock()
    cursor_mock = MagicMock()
    connection.cursor.return_value = cursor_mock

    ts = TableService(
        mock_table_partitioned, connection, internal_prefix="my_internal_prefix_"
    )
    ts.create_internal_table(mock_aws_settings)

    cursor_mock.execute.assert_called_once_with(
        format_query(
            """CREATE FACT TABLE my_internal_prefix_table_name
                        (id INTEGER, user STRING, birthdate DATE,
                        source_file_name TEXT, source_file_timestamp TIMESTAMPNTZ)
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
    get_table_columns_mock.return_value = [
        ("id", "INTEGER"),
        ("name", "TEXT"),
        ("aliased", "TEXT"),
    ]

    # mocker.patch("firebolt_ingest.table_service.raise_on_tables_non_compatibility")

    mock_table.sync_mode = "overwrite"

    ts = TableService(mock_table, connection)
    ts.create_internal_table = MagicMock()
    ts.insert(advanced_mode=False, use_short_column_path_parquet=False)
    cursor_mock.execute.assert_any_call(query="create_fact_table_request")
    cursor_mock.execute.assert_any_call(query="DROP TABLE IF EXISTS table_name CASCADE")
    cursor_mock.execute.assert_any_call(
        query=format_query(
            """INSERT INTO table_name
               SELECT "id", "name", "name.member0" AS aliased
               FROM ex_table_name"""
        )
    )
    get_table_schema_mock.assert_called_once()


def test_insert_incremental_append(mocker: MockerFixture, mock_table: Table):
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
    # mocker.patch("firebolt_ingest.table_service.raise_on_tables_non_compatibility")

    mock_table.sync_mode = "append"

    ts = TableService(mock_table, connection)
    ts.insert()

    cursor_mock.execute.assert_any_call(
        query=format_query(
            """
            INSERT INTO table_name
            SELECT "id", "name", "name.member0" AS aliased,
                   source_file_name, source_file_timestamp
            FROM ex_table_name
            WHERE (source_file_name, source_file_timestamp::timestampntz) NOT IN (
                SELECT DISTINCT source_file_name, source_file_timestamp
                FROM table_name)"""
        )
    )

    does_table_exists_mock.assert_any_call(cursor_mock, "table_name")
    does_table_exists_mock.assert_any_call(cursor_mock, "ex_table_name")


def test_verify_ingestion(mocker: MockerFixture, mock_table: Table):
    """
    Test, that verify ingestion calls all required checks
    """

    connection = MagicMock()
    cursor_mock = MagicMock()
    connection.cursor.return_value = cursor_mock

    verify_ingestion_rowcount_mock = mocker.patch(
        "firebolt_ingest.table_service.verify_ingestion_rowcount", return_value=True
    )

    verify_ingestion_file_names_mock = mocker.patch(
        "firebolt_ingest.table_service.verify_ingestion_file_names", return_value=True
    )

    ts = TableService(mock_table, connection)
    assert ts.verify_ingestion()

    verify_ingestion_rowcount_mock.assert_any_call(
        cursor_mock, "table_name", "ex_table_name"
    )
    verify_ingestion_file_names_mock.assert_any_call(cursor_mock, "table_name")


def test_does_external_table_exist(mocker: MockerFixture, mock_table: Table):
    """
    Test to check if the function 'does_external_table_exist' correctly
    checks for the existence of an external table in the database.
    """
    connection = MagicMock()
    cursor_mock = MagicMock()
    cursor_mock.execute.return_value = 0
    connection.cursor.return_value = cursor_mock

    ts = TableService(mock_table, connection)

    ts.does_external_table_exist()

    # Use the same format for the query as used in the actual function
    expected_query = "SELECT * FROM information_schema.tables WHERE table_name = ?"

    cursor_mock.execute.assert_called_once_with(
        expected_query, [ts.external_table_name]
    )


def test_does_internal_table_exist(mocker: MockerFixture, mock_table: Table):
    """
    Test to check if the function 'does_internal_table_exist' correctly
    checks for the existence of an internal table in the database.
    """
    connection = MagicMock()
    cursor_mock = MagicMock()
    cursor_mock.execute.return_value = 0
    connection.cursor.return_value = cursor_mock

    ts = TableService(mock_table, connection)

    ts.does_internal_table_exist()

    # Use the same format for the query as used in the actual function
    expected_query = "SELECT * FROM information_schema.tables WHERE table_name = ?"

    cursor_mock.execute.assert_called_once_with(
        expected_query, [ts.internal_table_name]
    )


def test_drop_tables(mocker: MockerFixture, mock_table: Table):
    """
    Test to check if the function 'drop_tables' correctly
    drops the internal and external tables in the database.
    """
    connection = MagicMock()
    cursor_mock = MagicMock()
    cursor_mock.execute.return_value = 0
    connection.cursor.return_value = cursor_mock

    ts = TableService(mock_table, connection)

    ts.drop_tables()

    cursor_mock.execute.assert_any_call(query="DROP TABLE IF EXISTS table_name CASCADE")
    cursor_mock.execute.assert_any_call(
        query="DROP TABLE IF EXISTS ex_table_name CASCADE"
    )
    expected_query = "SELECT * FROM information_schema.tables WHERE table_name = ?"
    cursor_mock.execute.assert_any_call(expected_query, [ts.external_table_name])
    cursor_mock.execute.assert_any_call(expected_query, [ts.internal_table_name])


def test_drop_outdated_partitions_table_without_partitions(
    mocker: MockerFixture, mock_table: Table
):
    """
    Test to check if the function 'drop_outdated_partitions' correctly
    drops the partitions in the fact table that are outdated.
    """

    connection = MagicMock()
    cursor_mock = MagicMock()
    cursor_mock.execute.return_value = 0
    connection.cursor.return_value = cursor_mock

    ts = TableService(mock_table, connection)

    with pytest.raises(FireboltError):
        ts.drop_outdated_partitions()


def test_drop_outdated_partitions(mocker: MockerFixture, mock_table_partitioned: Table):
    """
    Test to check if the function 'drop_outdated_partitions' correctly
    drops the partitions in the fact table that are outdated.
    """

    connection = MagicMock()
    cursor_mock = MagicMock()
    connection.cursor.return_value = cursor_mock

    does_table_exists_mock = mocker.patch(
        "firebolt_ingest.table_service.does_table_exist", return_value=[True, True]
    )

    ts = TableService(mock_table_partitioned, connection)

    cursor_mock.fetchall.return_value = [("user1", "12"), ("user2", "13")]

    ts.drop_outdated_partitions()

    cursor_mock.execute.assert_any_call(
        query=format_query(
            """SELECT DISTINCT user,
                EXTRACT(DAY FROM birthdate)
            FROM ex_table_name
            WHERE source_file_timestamp > ( SELECT MAX(source_file_timestamp)
                FROM table_name)
            """
        )
    )

    cursor_mock.execute.assert_any_call(
        query="ALTER TABLE table_name DROP PARTITION user1,12"
    )
    cursor_mock.execute.assert_any_call(
        query="ALTER TABLE table_name DROP PARTITION user2,13"
    )

    does_table_exists_mock.assert_any_call(cursor_mock, "table_name")
    does_table_exists_mock.assert_any_call(cursor_mock, "ex_table_name")
