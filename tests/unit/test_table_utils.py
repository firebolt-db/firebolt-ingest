from unittest.mock import MagicMock

from pytest import fixture
from pytest_mock import MockerFixture

from firebolt_ingest.table_utils import (
    drop_table,
    get_table_columns,
    get_table_schema,
)


@fixture
def table_name() -> str:
    return "my_table"


@fixture
def cursor(mocker: MockerFixture) -> MagicMock:
    cursor = mocker.MagicMock()

    table_name_column = mocker.MagicMock()
    table_name_column.name = "table_name"
    schema_column = mocker.MagicMock()
    schema_column.name = "schema"
    cursor.description = [table_name_column, schema_column]
    return cursor


def test_get_table_schema(table_name: str, cursor: MagicMock):
    create_statement = "CREATE my_table ..."

    cursor.execute.return_value = ["CREATE TABLE mock"]
    cursor.fetchall.return_value = [(table_name, create_statement)]
    assert get_table_schema(cursor=cursor, table_name=table_name) == create_statement

    cursor.execute.assert_called_once_with("SHOW TABLES")
    cursor.fetchall.assert_called_once()


def test_drop_table(mocker: MockerFixture, table_name: str):
    cursor = mocker.MagicMock()
    mocker.patch("firebolt_ingest.table_utils.does_table_exist", return_value=False)
    drop_table(cursor=cursor, table_name=table_name)

    cursor.execute.assert_called_once_with(
        query=f"DROP TABLE IF EXISTS {table_name} CASCADE"
    )


def test_get_table_columns(mocker: MockerFixture, table_name: str, cursor: MagicMock):
    mocker.patch("firebolt_ingest.table_utils.does_table_exist", return_value=True)
    columns = get_table_columns(cursor=cursor, table_name=table_name)

    assert columns == [column.name for column in cursor.description]
    cursor.execute.assert_called_once_with(f"SELECT * FROM {table_name} LIMIT 0")
