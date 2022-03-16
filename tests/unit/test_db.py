from unittest.mock import MagicMock

import pytest

from firebolt_ingest.db import (
    get_or_create_database,
    get_or_create_engine,
    set_up_connection,
)


@pytest.fixture
def engines(mocker):
    return mocker.patch("firebolt_ingest.db.engines")


@pytest.fixture
def databases(mocker):
    return mocker.patch("firebolt_ingest.db.databases")


@pytest.fixture
def database_name():
    return "test_db_name"


@pytest.fixture
def engine_name():
    return "test_engine_name"


def test_get_or_create_database(mocker, database_name):
    rm = MagicMock()

    get_or_create_database(rm, database_name)
    rm.databases.get_by_name.assert_called_with(name=database_name)


def test_get_or_create_engine(engine_name):
    rm = MagicMock()

    get_or_create_engine(rm, engine_name)
    rm.engines.get_by_name.assert_called_with(name=engine_name)


def test_set_up_connection(mocker, database_name, engine_name):
    # mock
    rm = MagicMock()

    get_or_create_database = mocker.patch("firebolt_ingest.db.get_or_create_database")
    database = get_or_create_database.return_value

    get_or_create_engine = mocker.patch("firebolt_ingest.db.get_or_create_engine")
    engine = get_or_create_engine.return_value

    # call
    set_up_connection(rm, database_name=database_name, engine_name=engine_name)

    # assert
    database.attach_to_engine.assert_called_once_with(
        engine=engine, is_default_engine=True
    )
    engine.start.assert_called_once_with()
