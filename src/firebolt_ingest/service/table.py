from typing import Any, List, Optional, Sequence

import sqlparse  # type: ignore
from firebolt.async_db.connection import Connection

from firebolt_ingest.aws_settings import (
    AWSSettings,
    generate_aws_credentials_string,
)
from firebolt_ingest.model.table import FILE_METADATA_COLUMNS, Table


class TableService:
    """ """

    def __init__(self, connection: Connection):
        """

        Args:
            connection:
        """
        self.connection = connection

    def create_external_table(self, table: Table, aws_settings: AWSSettings) -> None:
        """
        Constructs a query for creating an external table and executes it.

        Args:
            table: table definition
            aws_settings: aws settings
        """
        # Prepare aws credentials
        if aws_settings.aws_credentials:
            cred_stmt, cred_params = generate_aws_credentials_string(
                aws_settings.aws_credentials
            )
            cred_stmt = f"CREDENTIALS = {cred_stmt}"
        else:
            cred_stmt, cred_params = "", []

        # Prepare columns
        columns_stmt, columns_params = table.generate_external_columns_string()

        # Generate query
        query = (
            f"CREATE EXTERNAL TABLE {table.table_name}\n"
            f"({columns_stmt})\n"
            f"{cred_stmt}\n"
            f"URL = ?\n"
            f"OBJECT_PATTERN = {', '.join(['?'] * len(table.object_pattern))}\n"
            f"TYPE = ({table.file_type.name})\n"
        )
        if table.compression:
            query += f"COMPRESSION = {table.compression}\n"

        params = (
            cred_params + columns_params + [aws_settings.s3_url] + table.object_pattern
        )

        # Execute parametrized query
        self.connection.cursor().execute(query, params)

    def create_internal_table(self, table: Table, add_file_metadata=True) -> None:
        """
        Constructs a query for creating an internal table and executes it

        Args:
            table: table definition
        """

        # TODO: partition support, primary index support
        columns_stmt, columns_params = table.generate_internal_columns_string(
            add_file_metadata
        )
        query = f"CREATE FACT TABLE {table.table_name}\n" f"({columns_stmt})\n"

        if table.primary_index:
            query += f"PRIMARY INDEX {table.generate_primary_index_string()}\n"

        if table.partitions:
            query += f"PARTITION BY {table.generate_partitions_string(add_file_metadata=add_file_metadata)}\n"  # noqa: E501

        self.connection.cursor().execute(query, columns_params)

    def get_table_columns(self, table_name: str) -> List[str]:
        """
        Get the column names of an existing table on Firebolt.

        Args:
            table_name: Name of the table
        """
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
        return [column.name for column in cursor.description]

    def get_table_partition_columns(self, table_name: str) -> List[str]:
        """
        Get the names of partition columns of an existing table on Firebolt.

        Args:
            table_name: Name of the table.
        """
        cursor = self.connection.cursor()
        sql = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE
            table_schema = ? AND
            table_name = ? AND
            is_in_partition_expr = 'YES'
        """
        cursor.execute(query=sql, parameters=(self.connection.database, table_name))
        return cursor.fetchall()  # type: ignore

    def get_partition_keys(
        self, table_name: str, where_sql: Optional[str] = None
    ) -> Sequence[Any]:
        """
        Get the partition keys of an existing table on Firebolt.
        Args:
            table_name: Name of the table.
             where_sql: (Optional) A where clause, for filtering data.
                Do not include the "WHERE" keyword.
                If no clause is provided (default), all partition keys are returned.
        """
        # FUTURE: replace this query with `show partitions` when
        # https://packboard.atlassian.net/browse/FIR-2370 is completed
        part_expr = ",".join(self.get_table_partition_columns(table_name=table_name))
        sql = f"""
        SELECT DISTINCT {part_expr}
        FROM {table_name}
        """
        if where_sql is not None:
            sql += f"WHERE {where_sql}"

        cursor = self.connection.cursor()
        cursor.execute(sql)
        return cursor.fetchall()  # type: ignore

    def _insert(
        self, internal_table: Table, external_table_name: str, where_sql: Optional[str]
    ):
        """
        Internal method to perform the insert step. Not idempotent.

        Args:
            internal_table: Internal table object, to write into.
            external_table_name: Name of the external table to read from.
            where_sql: (Optional) A where clause, for filtering data.
                Do not include the "WHERE" keyword.
                If no clause is provided (default), the entire external table is loaded.
        """

        # if the internal table on firebolt has the file metadata columns,
        # we need to be sure to include them as part of our insert.
        add_file_metadata = set(c.name for c in FILE_METADATA_COLUMNS).issubset(
            set(self.get_table_columns(internal_table.table_name))
        )

        column_names = [
            column.name
            for column in internal_table.columns
            + (FILE_METADATA_COLUMNS if add_file_metadata else [])
        ]
        insert_query = (
            f"INSERT INTO {internal_table.table_name}\n"
            f"SELECT {', '.join(column_names)}\n"
            f"FROM {external_table_name}\n"
        )
        if where_sql is not None:
            insert_query += f"WHERE {where_sql}"

        formatted_query = sqlparse.format(insert_query, reindent=True, indent_width=4)
        cursor = self.connection.cursor()
        cursor.execute(query=formatted_query)

    def insert_full_overwrite(
        self,
        internal_table: Table,
        external_table_name: str,
        where_sql: Optional[str] = None,
    ) -> None:
        """
        Perform a full overwrite from an external table into an internal table.
        All existing data in the internal table will be deleted, and data from the
        external table will be loaded.

        This function is appropriate for unpartitioned tables, or for
        partitioned tables you wish to fully overwrite.

        Args:
            internal_table: (destination) The internal table which will be overwritten.
            external_table_name: (source) The external table from which to load.
            where_sql: (Optional) A where clause, for filtering data.
                Do not include the "WHERE" keyword.
                If no clause is provided (default), the entire external table is loaded.
        """
        cursor = self.connection.cursor()

        drop_query = f"DROP TABLE IF EXISTS {internal_table.table_name} CASCADE"

        cursor.execute(query=drop_query)

        # verify that the drop succeeded
        find_query = (
            f"SELECT * FROM information_schema.tables "
            f"WHERE table_name = '{internal_table.table_name}'"
        )
        matching_table_count = cursor.execute(find_query)
        if matching_table_count != 0:
            raise RuntimeError(
                f"Table {internal_table.table_name} did not drop successfully."
            )

        self.create_internal_table(table=internal_table)

        self._insert(
            internal_table=internal_table,
            external_table_name=external_table_name,
            where_sql=where_sql,
        )

    def insert_incremental_overwrite(
        self,
        internal_table: Table,
        external_table_name: str,
        where_sql: Optional[str] = None,
    ) -> None:
        """
        Perform an incremental overwrite from an external table into an internal table.

        The internal table must be partitioned by:
         - source_file_name
         - source_file_timestamp

        Args:
            internal_table: (destination) The internal table which will be overwritten.
            external_table_name: (source) The external table from which to load.
            where_sql: (Optional) A where clause, for filtering data.
                Do not include the "WHERE" keyword.
                If no clause is provided (default), the entire external table is loaded.
        """

        cursor = self.connection.cursor()

        # verify the internal table is partitioned by source file name & timestamp
        if not set(c.name for c in FILE_METADATA_COLUMNS).issubset(
            set(self.get_table_partition_columns(internal_table.table_name))
        ):
            raise RuntimeError(
                f"Internal table {internal_table.table_name} must be partitioned "
                f"by source_file_name and source_file_timestamp."
            )

        for part_key in self.get_partition_keys(
            table_name=internal_table.table_name, where_sql=where_sql
        ):
            cursor.execute(
                f"ALTER TABLE {internal_table.table_name} DROP PARTITION {part_key}"
            )

        self._insert(
            internal_table=internal_table,
            external_table_name=external_table_name,
            where_sql=where_sql,
        )
