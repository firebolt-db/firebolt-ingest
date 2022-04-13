from typing import List, Sequence

from firebolt.async_db import Cursor
from firebolt.common.exception import FireboltError


def get_table_schema(cursor: Cursor, table_name: str) -> str:
    """
    Return the create command of the existing table.
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

    return internal_table_schema[0]


def drop_table(cursor: Cursor, table_name: str) -> None:
    """
    Drop table table_name and raise an exception
    """
    drop_query = f"DROP TABLE IF EXISTS {table_name} CASCADE"

    # drop the table
    cursor.execute(query=drop_query)

    # verify that the drop succeeded
    if does_table_exists(cursor, table_name):
        raise FireboltError(f"Table {table_name} did not drop successfully.")


def get_table_columns(cursor: Cursor, table_name: str) -> List[str]:
    """
    Get the column names of an existing table on Firebolt.

    Args:
        table_name: Name of the table
    """
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
    return [column.name for column in cursor.description]


def does_table_exists(cursor: Cursor, table_name: str) -> bool:
    """
    Check whether table with table_name exists,
    and return True if it exists, False otherwise
    """
    find_query = f"SELECT * FROM information_schema.tables WHERE table_name = ?"

    return cursor.execute(find_query, [table_name]) != 0
