import logging

from firebolt.common.exception import AlreadyBoundError
from firebolt.db import Connection
from firebolt.model.database import Database
from firebolt.model.engine import Engine
from firebolt.service.manager import ResourceManager

logger = logging.getLogger(__name__)


def set_up_connection(
    rm: ResourceManager, database_name: str, engine_name: str
) -> Connection:
    """
    Ensure database and engine exist, and that engine is running.
    Args:
        database_name: Name of the database to use (or create).
        engine_name: Name of the engine to use (or create).

    Returns:
        Engine connection object (for running queries).
    """
    database = get_or_create_database(rm, database_name=database_name)
    engine = get_or_create_engine(rm, engine_name=engine_name)
    try:
        database.attach_to_engine(engine=engine, is_default_engine=True)
    except AlreadyBoundError:
        pass
    engine.start()
    return engine.get_connection()


def get_or_create_engine(rm: ResourceManager, engine_name: str) -> Engine:
    """Get a Firebolt engine by name. If it does not exist, create it."""
    try:
        engine = rm.engines.get_by_name(name=engine_name)
    except RuntimeError:
        engine = rm.engines.create(name=engine_name)
        logger.info(f"Created engine: {engine}")
    return engine


def get_or_create_database(rm: ResourceManager, database_name: str) -> Database:
    """Get a Firebolt database by name. If it does not exist, create it."""
    try:
        database = rm.databases.get_by_name(name=database_name)
    except RuntimeError:
        database = rm.databases.create(name=database_name)
        logger.info(f"Created database: {database}")
    return database
