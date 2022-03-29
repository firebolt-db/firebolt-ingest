from logging import getLogger
from os import environ

from firebolt.db.connection import connect
from pytest import fixture

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
        username=username,
        password=password,
        api_endpoint=api_endpoint,
    )

    yield connection

    connection.close()
