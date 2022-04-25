from functools import wraps
from typing import Any, List, Optional, Sequence

from firebolt.common.exception import FireboltError
from firebolt.db import Cursor

from firebolt_ingest.table_model import FILE_METADATA_COLUMNS
from firebolt_ingest.utils import format_query


def table_must_exist(func):
    @wraps(func)
    def with_table_existence_check(cursor: Cursor, table_name: str, **kwargs):
        if not does_table_exist(cursor=cursor, table_name=table_name):
            raise FireboltError(
                f"Table {table_name} does not exist " f"when calling {func.__name__}"
            )
        return func(cursor=cursor, table_name=table_name, **kwargs)

    return with_table_existence_check


def get_table_schema(cursor: Cursor, table_name: str) -> str:
    """
    Return the create command of the existing table.

    Args:
        cursor: Firebolt database cursor
        table_name: Name of the table

    Returns:
        CREATE TABLE ... command
    """
    cursor.execute("SHOW TABLES")
    data = cursor.fetchall()

    def find_column_index(columns: Sequence, column_name: str) -> int:
        result = [
            idx for idx, column in enumerate(columns) if column.name == column_name
        ]

        if len(result) == 0:
            raise FireboltError(f"Cannot find expected column: {column_name}")
        elif len(result) > 1:
            raise FireboltError(f"Too many columns: {column_name} found")

        return result[0]

    table_name_index = find_column_index(cursor.description, "table_name")
    schema_index = find_column_index(cursor.description, "schema")

    internal_table_schema = [
        row[schema_index]
        for row in data  # type: ignore
        if row[table_name_index] == table_name
    ]

    if len(internal_table_schema) == 0:
        raise FireboltError("internal table doesn't exist")

    return str(internal_table_schema[0])


def drop_table(cursor: Cursor, table_name: str) -> None:
    """
    Drop a table.

    Args:
        cursor: Firebolt database cursor
        table_name: Name of the table to drop.

    Raises an exception if the table did not drop.
    """
    drop_query = f"DROP TABLE IF EXISTS {table_name} CASCADE"

    # drop the table
    cursor.execute(query=format_query(drop_query))

    # verify that the drop succeeded
    if does_table_exist(cursor, table_name):
        raise FireboltError(f"Table {table_name} did not drop successfully.")


@table_must_exist
def get_table_columns(cursor: Cursor, table_name: str) -> List[str]:
    """
    Get the column names of an existing table on Firebolt.

    Args:
        cursor: Firebolt database cursor
        table_name: Name of the table
    """
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
    return [column.name for column in cursor.description]


@table_must_exist
def get_table_partition_columns(cursor: Cursor, table_name: str) -> List[str]:
    """
    Get the names of partition columns of an existing table on Firebolt.

    Args:
        cursor: Firebolt database cursor
        table_name: Name of the table.
    """
    query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE
        table_schema = ? AND
        table_name = ? AND
        is_in_partition_expr = 'YES'
    """

    cursor.execute(
        query=format_query(query), parameters=(cursor.connection.database, table_name)
    )
    return cursor.fetchall()  # type: ignore


@table_must_exist
def get_partition_keys(
    cursor: Cursor, table_name: str, where_sql: Optional[str] = None
) -> Sequence[Any]:
    """
    Get the partition keys of an existing table on Firebolt.

    Args:
        cursor: Firebolt database cursor
        table_name: Name of the table.
        where_sql: (Optional) A where clause, for filtering data.
            Do not include the "WHERE" keyword.
            If no clause is provided (default), all partition keys are returned.
    """
    # FUTURE: replace this query with `show partitions` when
    # https://packboard.atlassian.net/browse/FIR-2370 is completed
    part_expr = ",".join(
        get_table_partition_columns(cursor=cursor, table_name=table_name)
    )
    query = f"""
    SELECT DISTINCT {part_expr}
    FROM {table_name}
    """
    if where_sql is not None:
        query += f"WHERE {where_sql}"

    cursor.execute(format_query(query))
    return cursor.fetchall()  # type: ignore


def verify_ingestion_rowcount(
    cursor: Cursor, internal_table_name: str, external_table_name: str
) -> bool:
    """
    Verify, that the fact and external table have the same number of rows

    Note: doesn't check for existence of the fact and external tables,
    hence not safe for external usage. Could lead to sql-injection

    Args:
        cursor: Firebolt database cursor
        internal_table_name: name of the fact table
        external_table_name: name of the external table

    Returns: true if the number of rows the same
    """
    query = f"""
    SELECT
        (SELECT count(*) FROM {internal_table_name}) AS rc_fact,
        (SELECT count(*) FROM {external_table_name}) AS rc_external
    """
    cursor.execute(query=format_query(query))

    data = cursor.fetchall()
    if data is None:
        return False

    return data[0][0] == data[0][1]  # type: ignore


def verify_ingestion_file_names(cursor: Cursor, internal_table_name: str) -> bool:
    """
    Verify ingestion using the metadata. If we have entries with the same
    source_file_name and different source_file_timestamp we might have duplicates
    """

    table_columns = get_table_columns(cursor, internal_table_name)

    # if the metadata is missing return True,
    # since it is not possible to do the validation
    if not {column.name for column in FILE_METADATA_COLUMNS}.issubset(table_columns):
        return True

    query = f"""
    SELECT source_file_name FROM {internal_table_name}
    GROUP BY source_file_name
    HAVING count(DISTINCT source_file_timestamp) <> 1
    """
    cursor.execute(query=format_query(query))

    # if the table is correct, the number of fetched rows should be zero
    return len(cursor.fetchall()) == 0  # type: ignore


def does_table_exist(cursor: Cursor, table_name: str) -> bool:
    """
    Check whether table with table_name exists,
    and return True if it exists, False otherwise.
    """
    find_query = f"SELECT * FROM information_schema.tables WHERE table_name = ?"

    return cursor.execute(find_query, [table_name]) != 0
