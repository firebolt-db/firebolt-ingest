from pytest_mock import MockerFixture

from firebolt_ingest.table_utils import get_table_schema


def test_get_table_schema(mocker: MockerFixture):
    create_statement = "CREATE my_table ..."

    cursor = mocker.MagicMock()

    table_name_column = mocker.MagicMock()
    table_name_column.name = "table_name"
    schema_column = mocker.MagicMock()
    schema_column.name = "schema"
    cursor.description = [table_name_column, schema_column]

    cursor.execute.return_value = ["CREATE TABLE mock"]
    cursor.fetchall.return_value = [("my_table", create_statement)]
    assert get_table_schema(cursor=cursor, table_name="my_table") == create_statement

    cursor.execute.assert_called_once_with("SHOW TABLES")
    cursor.fetchall.assert_called_once()
