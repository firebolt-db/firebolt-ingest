from typing import Sequence
from unittest.mock import MagicMock

import pytest
from pytest import fixture
from pytest_mock import MockerFixture

from firebolt_ingest.table_utils import (
    check_table_compatibility,
    does_table_exist,
    drop_table,
    get_table_columns,
    get_table_schema,
    verify_ingestion_file_names,
    verify_ingestion_rowcount,
)
from firebolt_ingest.utils import format_query


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
    cursor.fetchall.return_value = [["id", "INT"], ["name", "TEXT"]]
    columns = get_table_columns(cursor=cursor, table_name=table_name)

    cursor.fetchall.assert_called_once_with()

    assert columns == [("id", "INT"), ("name", "TEXT")]


def test_does_table_exist(table_name: str, cursor: MagicMock):
    cursor.execute.return_value = 1

    assert does_table_exist(cursor=cursor, table_name=table_name)

    cursor.execute.assert_called_once_with(
        f"SELECT * FROM information_schema.tables WHERE table_name = ?", [table_name]
    )


def test_verify_ingestion_rowcount(cursor: MagicMock):
    """
    Test happy path of verify ingestion rowcount function
    """
    cursor.fetchall.return_value = [[100, 100]]

    assert verify_ingestion_rowcount(
        cursor, "internal_table_name", "external_table_name"
    )

    cursor.execute.assert_called_once_with(
        query=format_query(
            """
            SELECT (SELECT count(*) FROM internal_table_name) AS rc_fact,
                   (SELECT count(*) FROM external_table_name) AS rc_external"""
        )
    )

    cursor.fetchall.assert_called_once()


def test_verify_ingestion_rowcount_different(cursor: MagicMock):
    """
    Test verify ingestion rowcount, when rowcount is not equal
    """
    cursor.fetchall.return_value = [[100, 101]]

    assert not verify_ingestion_rowcount(
        cursor, "internal_table_name", "external_table_name"
    )

    cursor.execute.assert_called_once_with(
        query=format_query(
            """
            SELECT (SELECT count(*) FROM internal_table_name) AS rc_fact,
                   (SELECT count(*) FROM external_table_name) AS rc_external"""
        )
    )

    cursor.fetchall.assert_called_once()


@pytest.mark.parametrize(
    "fetch_return,expected", [([], True), ([["some_file_name"]], False)]
)
def test_verify_ingestion_file_names(
    cursor: MagicMock, mocker: MockerFixture, fetch_return: Sequence, expected: bool
):
    """
    test verify ingestion correct and incorrect cases
    """

    mocker.patch(
        "firebolt_ingest.table_utils.get_table_columns",
        return_value=[
            ("source_file_name", "TEXT"),
            ("source_file_timestamp", "TIMESTAMP"),
            ("other_column", "LONG"),
        ],
    )
    cursor.fetchall.return_value = fetch_return

    assert verify_ingestion_file_names(cursor, "internal_table_name") == expected

    cursor.execute.assert_called_once_with(
        query=format_query(
            """
            SELECT source_file_name FROM internal_table_name
            GROUP BY source_file_name
            HAVING count(DISTINCT source_file_timestamp) <> 1"""
        )
    )
    cursor.fetchall.assert_called_once()


def test_verify_ingestion_file_names_no_meta(cursor: MagicMock, mocker: MockerFixture):
    """
    test verify ingestion no file metacolumns
    """

    mocker.patch(
        "firebolt_ingest.table_utils.get_table_columns",
        return_value=["source_file_name", "other_column"],
    )

    assert verify_ingestion_file_names(cursor, "internal_table_name")

    cursor.execute.assert_not_called()
    cursor.fetchall.assert_not_called()


def test_check_table_compatibility_happy(cursor, mocker: MockerFixture):
    """
    test check_table_compatibility happy path
    """
    mocker.patch(
        "firebolt_ingest.table_utils.get_table_columns",
        return_value=[("name", "TEXT"), ("id", "INT")],
    )

    expected_columns = set([("id", "INT"), ("name", "TEXT")])

    err_list = check_table_compatibility(cursor, "table_name", expected_columns, set())
    assert len(err_list) == 0


def test_check_table_compatibility_ignore(cursor, mocker: MockerFixture):
    """
    check, that incompatibility in ignore list doesn't result into an error
    """
    mocker.patch(
        "firebolt_ingest.table_utils.get_table_columns",
        return_value=[("name", "TEXT"), ("id", "INT"), ("id1", "INT")],
    )

    expected_columns = set([("id", "INT"), ("name", "TEXT")])

    err_list = check_table_compatibility(
        cursor, "table_name", expected_columns, set([("id1", "INT")])
    )
    assert len(err_list) == 0


def test_check_table_compatibility_error(cursor, mocker: MockerFixture):
    """
    check, whether incompatibility is caught
    """
    mocker.patch(
        "firebolt_ingest.table_utils.get_table_columns",
        return_value=[("name", "TEXT"), ("id", "INT"), ("id1", "INT")],
    )

    expected_columns = set([("id", "INT"), ("name", "TEXT")])

    err_list = check_table_compatibility(cursor, "table_name", expected_columns, set())
    assert len(err_list) == 1
    assert err_list[0][0] == "id1"
    assert err_list[0][1] == "INT"
    assert err_list[0][2] == "table_name"
