from typing import Optional

from firebolt.async_db.connection import Connection

from firebolt_ingest.aws_settings import (
    AWSSettings,
    generate_aws_credentials_string,
)
from firebolt_ingest.model.table import FILE_METADATA_COLUMNS, Table
from firebolt_ingest.table_utils import (
    drop_table,
    get_partition_keys,
    get_table_columns,
    get_table_partition_columns,
    get_table_schema,
)

EXTERNAL_TABLE_PREFIX = "ex_"


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
            f"CREATE EXTERNAL TABLE {EXTERNAL_TABLE_PREFIX}{table.table_name}\n"
            f"({columns_stmt})\n"
            f"{cred_stmt}\n"
            f"URL = ?\n"
            f"OBJECT_PATTERN = {', '.join(['?'] * len(table.object_pattern))}\n"
            f"TYPE = ({table.generate_file_type()})\n"
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

        columns_stmt, columns_params = table.generate_internal_columns_string(
            add_file_metadata
        )
        query = (
            f"CREATE FACT TABLE {table.table_name}\n"
            f"({columns_stmt})\n"
            f"PRIMARY INDEX {table.generate_primary_index_string()}\n"
        )

        if table.partitions:
            query += f"PARTITION BY {table.generate_partitions_string(add_file_metadata=add_file_metadata)}\n"  # noqa: E501

        self.connection.cursor().execute(query, columns_params)

    def insert_full_overwrite(
        self,
        internal_table_name: str,
        external_table_name: str,
        firebolt_dont_wait_for_upload_to_s3: bool = False,
    ) -> None:
        """
        Perform a full overwrite from an external table into an internal table.
        All existing data in the internal table will be deleted, and data from the
        external table will be loaded.

        This function is appropriate for unpartitioned tables, or for
        partitioned tables you wish to fully overwrite.

        Args:
            internal_table_name: (destination) The internal table which
                                 will be overwritten.
            external_table_name: (source) The external table from which to load.

            firebolt_dont_wait_for_upload_to_s3: (Optional) if set, the insert will not
                wait until the changes will be written to s3.

        """
        cursor = self.connection.cursor()

        # get table schema
        internal_table_schema = get_table_schema(cursor, internal_table_name)

        # drop the table
        drop_table(cursor, internal_table_name)

        # recreate the table
        cursor.execute(query=internal_table_schema)

        # insert the data from external to internal
        column_names = get_table_columns(cursor, internal_table_name)
        insert_query = (
            f"INSERT INTO {internal_table_name}\n"
            f"SELECT {', '.join(column_names)}\n"
            f"FROM {external_table_name}\n"
        )

        if firebolt_dont_wait_for_upload_to_s3:
            cursor.execute(query="set firebolt_dont_wait_for_upload_to_s3=1")

        cursor.execute(query=insert_query)

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
        cursor = self.connection.cursor()

        # if the internal table on firebolt has the file metadata columns,
        # we need to be sure to include them as part of our insert.
        add_file_metadata = set(c.name for c in FILE_METADATA_COLUMNS).issubset(
            set(get_table_columns(cursor=cursor, table_name=internal_table.table_name))
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

        cursor.execute(query=insert_query)

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
            set(
                get_table_partition_columns(
                    cursor=cursor, table_name=internal_table.table_name
                )
            )
        ):
            raise RuntimeError(
                f"Internal table {internal_table.table_name} must be partitioned "
                f"by source_file_name and source_file_timestamp."
            )

        for part_key in get_partition_keys(
            cursor=cursor, table_name=internal_table.table_name, where_sql=where_sql
        ):
            cursor.execute(
                f"ALTER TABLE {internal_table.table_name} DROP PARTITION {part_key}"
            )

        self._insert(
            internal_table=internal_table,
            external_table_name=external_table_name,
            where_sql=where_sql,
        )
