from logging import getLogger
from os import environ

from firebolt.client.auth import UsernamePassword
from firebolt.db.connection import connect
from pytest import fixture

from firebolt_ingest.table_model import Column, Table

LOGGER = getLogger(__name__)

ENGINE_NAME_ENV = "ENGINE_NAME"
DATABASE_NAME_ENV = "DATABASE_NAME"
USER_NAME_ENV = "USER_NAME"
PASSWORD_ENV = "PASSWORD"
ACCOUNT_NAME_ENV = "ACCOUNT_NAME"
API_ENDPOINT_ENV = "API_ENDPOINT"


def must_env(var_name: str) -> str:
    assert var_name in environ, f"Expected {var_name} to be provided in environment"
    LOGGER.info(f"{var_name}: {environ[var_name]}")
    return environ[var_name]


@fixture(scope="session")
def engine_name() -> str:
    return must_env(ENGINE_NAME_ENV)


@fixture(scope="session")
def database_name() -> str:
    return must_env(DATABASE_NAME_ENV)


@fixture(scope="session")
def username() -> str:
    return must_env(USER_NAME_ENV)


@fixture(scope="session")
def password() -> str:
    return must_env(PASSWORD_ENV)


@fixture(scope="session")
def account_name() -> str:
    return must_env(ACCOUNT_NAME_ENV)


@fixture(scope="session")
def api_endpoint() -> str:
    return must_env(API_ENDPOINT_ENV)


@fixture(scope="session")
def connection(
    engine_name, database_name, username, password, account_name, api_endpoint
):
    connection = connect(
        engine_name=engine_name,
        account_name=account_name,
        database=database_name,
        auth=UsernamePassword(username=username, password=password),
        api_endpoint=api_endpoint,
    )

    yield connection

    connection.close()


@fixture(scope="session")
def s3_url() -> str:
    return "s3://firebolt-publishing-public/samples/tpc-h/parquet/lineitem/"


@fixture
def mock_table() -> Table:
    return Table(
        table_name="lineitem",
        columns=[
            Column(name="l_orderkey", type="BIGINT"),
            Column(name="l_partkey", type="BIGINT"),
            Column(name="l_suppkey", type="BIGINT"),
            Column(name="l_linenumber", type="INTEGER"),
            Column(name="l_quantity", type="BIGINT"),
            Column(name="l_extendedprice", type="BIGINT"),
            Column(name="l_discount", type="BIGINT"),
            Column(name="l_tax", type="BIGINT"),
            Column(name="l_returnflag", type="TEXT"),
            Column(name="l_linestatus", type="TEXT"),
            Column(name="l_shipdate", type="TEXT"),
            Column(name="l_commitdate", type="TEXT"),
            Column(name="l_receiptdate", type="TEXT"),
            Column(name="l_shipinstruct", type="TEXT"),
            Column(name="l_shipmode", type="TEXT"),
            Column(name="l_comment", type="TEXT"),
        ],
        file_type="PARQUET",
        object_pattern="*.parquet",
        primary_index=["l_orderkey", "l_linenumber"],
    )


@fixture
def mock_table_timestamps() -> Table:
    return Table(
        table_name="timestamps_table",
        columns=[
            Column(name="id", type="INTEGER"),
            Column(name="timestamp1", type="TIMESTAMP"),
            Column(name="timestamp2", type="TIMESTAMPNTZ"),
            Column(name="timestamp3", type="TIMESTAMPTZ"),
            Column(name="timestamp4", type="DATETIME"),
            Column(name="date1", type="DATE"),
            Column(name="date2", type="PGDATE"),
        ],
        file_type="PARQUET",
        object_pattern="*.parquet",
        primary_index=["id"],
    )
