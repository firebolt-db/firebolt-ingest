from typing import Optional, Sequence

from firebolt.db import Connection

from firebolt_ingest.model.table import Column, Table


def columns_as_sql_str(columns: Sequence[Column]):
    return ",\n".join(f"{c.name} {c.type}" for c in columns)


def insert_full_overwrite(
    connection: Connection,
    internal_table: Table,
    external_table_name: str,
    where_sql: Optional[str] = None,
) -> None:
    """
    Perform a full overwrite from an external table into an internal table. This means
    that all existing data in the internal table will be deleted, and data from the
    external table will be loaded.

    This function is appropriate for unpartitioned tables, or for
    partitioned tables you wish to fully overwrite.

    Args:
        connection: Connection from the Firebolt SDK.
        internal_table: (destination) The internal table which will be overwritten.
        external_table_name: (source) The external table from which to load.
        where_sql: (Optional) A where clause, for filtering data.
            Do not include the "WHERE" keyword.
            If no clause is provided (default), the entire external table is loaded.
    """
    cursor = connection.cursor()

    if internal_table.database_name != connection.database:
        raise RuntimeError(
            f"The connection is to the {connection.database} database, but"
            f"the internal table is for the {internal_table.database_name} database."
        )

    drop_query = f"DROP TABLE IF EXISTS {internal_table.table_name} CASCADE"

    cursor.execute(drop_query)

    # TODO: call TableService.create_internal_table

    insert_query = f"""
    INSERT INTO {internal_table.table_name}
    SELECT
    {columns_as_sql_str(internal_table.columns)}
    FROM
    {external_table_name}
    """

    if where_sql is not None:
        insert_query += """
        WHERE
        {where}
        """

    cursor.execute(query=insert_query)


def insert_incremental_overwrite(
    connection: Connection,
    internal_table: Table,
    external_table_name: str,
    partitions: Sequence[str],
    where_sql: Optional[str] = None,
) -> None:
    """
    Perform an incremental overwrite from an external table into an internal table, by
    dropping partitions in the internal table, and re-writing them by insert-selecting
    from the external table.

    This function is only appropriate for partitioned tables.

    Args:
        connection: Connection from the Firebolt SDK.
        internal_table: (destination) The internal table which will be overwritten.
        external_table_name: (source) The external table from which to load.
        partitions: (destination) The partitions to drop from the internal table.
        where_sql: (Optional, source) A where clause, for filtering data.
            Do not include the "WHERE" keyword.
            If no clause is provided (default), the entire external table is loaded.

    TODO: this needs a tighter coupling between external and interal tables so that
        it's truly idempotent and you cannot end up with duplicate data
    """

    if internal_table.database_name != connection.database:
        raise RuntimeError(
            f"The connection is to the {connection.database} database, but"
            f"the internal table is for the {internal_table.database_name} database."
        )
    cursor = connection.cursor()

    for partition in partitions:
        drop_partition_query = (
            f"ALTER TABLE {internal_table.table_name} DROP PARTITION {partition}"
        )
        cursor.execute(drop_partition_query)

    insert_query = f"""
    INSERT INTO {internal_table.table_name}
    SELECT
    {columns_as_sql_str(internal_table.columns)}
    FROM
    {external_table_name}
    """

    if where_sql is not None:
        insert_query += """
        WHERE
        {where}
        """

    cursor.execute(query=insert_query)
