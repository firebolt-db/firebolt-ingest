from functools import wraps
from typing import List, Sequence, Set, Tuple

from firebolt.common.exception import FireboltError
from firebolt.db import Cursor

from firebolt_ingest.table_model import FILE_METADATA_COLUMNS, Table
from firebolt_ingest.utils import format_query


def table_must_exist(func):
    @wraps(func)
    def with_table_existence_check(cursor: Cursor, table_name: str, **kwargs):
        if not does_table_exist(cursor=cursor, table_name=table_name):
            raise FireboltError(
                f"Table {table_name} does not exist when calling {func.__name__}"
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
def get_table_columns(cursor: Cursor, table_name: str) -> List[Tuple]:
    """
    Get the column names of an existing table on Firebolt.

    Args:
        cursor: Firebolt database cursor
        table_name: Name of the table
    """

    cursor.execute(
        f"SELECT column_name, data_type "
        f"FROM information_schema.columns "
        f"WHERE table_name = ?",
        [table_name],
    )
    return [(column_name, data_type) for column_name, data_type in cursor.fetchall()]


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
    if not {(column.name, column.type) for column in FILE_METADATA_COLUMNS}.issubset(
        table_columns
    ):
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


def check_table_compatibility(
    cursor: Cursor,
    table_name: str,
    expected_table_columns: Set[Tuple[str, str]],
    skip_columns: Set[Tuple[str, str]],
) -> List[Tuple[str, str, str, str]]:
    """
    Check whether actual table columns are equivalent to expected

    Args:
        cursor: cursor for query execution
        table_name: name of the existing table, from which
         the actual columns will be extracted
        expected_table_columns: columns, that should be there
        skip_columns: all errors with the specified columns will be ignored

    Returns: a list of all incompatibilities as a list of tuple
             (column_name, column_type, table_name, message)

    """
    actual_table_columns = set(get_table_columns(cursor, table_name))

    error_list = []
    for column in actual_table_columns - expected_table_columns - skip_columns:
        error_list.append((column[0], column[1], table_name, "expected but not found"))
    for column in expected_table_columns - actual_table_columns - skip_columns:
        error_list.append((column[0], column[1], table_name, "found but not expected"))

    return error_list


def raise_on_tables_non_compatibility(
    cursor: Cursor,
    table: Table,
    ignore_meta_columns: bool,
):
    """
    Check whether internal and external tables are compatible,
    and if not raise an exception with an appropriate error message
    """
    expected_internal_columns = set(
        ((c.alias if c.alias else c.name), c.type) for c in table.columns
    )
    expected_external_columns = set((c.name, c.type) for c in table.columns)

    skip_columns = (
        [(c.name, c.type) for c in FILE_METADATA_COLUMNS] if ignore_meta_columns else []
    )
    if not ignore_meta_columns:
        expected_internal_columns.update(
            (c.name, c.type) for c in FILE_METADATA_COLUMNS
        )

    error_list = check_table_compatibility(
        cursor, table.table_name, expected_internal_columns, set(skip_columns)
    ) + check_table_compatibility(
        cursor, f"ex_{table.table_name}", expected_external_columns, set()
    )

    if error_list:
        raise FireboltError(
            "\n".join(
                f"Column ({err[0]}, {err[1]}) in table ({err[2]}) {err[3]}"
                for err in error_list
            )
        )


def execute_set_statements(
    cursor: Cursor,
    firebolt_dont_wait_for_upload_to_s3: bool = False,
    advanced_mode: bool = False,
    use_short_column_path_parquet: bool = False,
) -> None:
    """
    Pre-execute set statements on the cursor
    Args:
        firebolt_dont_wait_for_upload_to_s3: sets the variable to 1
        advanced_mode: sets the variable to 1
        use_short_column_path_parquet: sets the variable to 1

    """
    if firebolt_dont_wait_for_upload_to_s3:
        cursor.execute("set firebolt_dont_wait_for_upload_to_s3=1")

    if advanced_mode:
        cursor.execute("set advanced_mode=1")

    if use_short_column_path_parquet:
        cursor.execute("set use_short_column_path_parquet=1")
