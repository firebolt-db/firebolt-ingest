from typing import Optional

import sqlparse  # type: ignore
from firebolt.async_db.connection import Connection

from firebolt_ingest.aws_settings import (
    AWSSettings,
    generate_aws_credentials_string,
)
from firebolt_ingest.model.table import Table


class TableService:
    """ """

    def __init__(self, connection: Connection, aws_settings: AWSSettings):
        """

        Args:
            connection:
            aws_settings:
        """
        self.connection = connection
        self.aws_settings = aws_settings

    def create_external_table(self, table: Table) -> None:
        """
        Constructs a query for creating an external table and executes it

        Args:
            table: table definition
        """
        # Prepare aws credentials
        if self.aws_settings.aws_credentials:
            cred_stmt, cred_params = generate_aws_credentials_string(
                self.aws_settings.aws_credentials
            )
            cred_stmt = f"CREDENTIALS = {cred_stmt}"
        else:
            cred_stmt, cred_params = "", []

        # Prepare columns
        columns = table.generate_columns_string()

        # Generate query
        query = (
            f"CREATE EXTERNAL TABLE IF NOT EXISTS {table.table_name} "
            f"({columns}) "
            f"{cred_stmt} "
            f"URL = ? "
            f"OBJECT_PATTERN = {', '.join(['?'] * len(table.object_pattern))} "
            + f"TYPE = ({table.file_type.name})"
        )
        params = cred_params + [self.aws_settings.s3_url] + table.object_pattern

        # Execute parametrized query
        self.connection.cursor().execute(query, params)

    def create_internal_table(self, table: Table) -> None:
        """

        Args:
            table:

        Returns:

        """

        # TODO: partition support, primary index support
        query = (
            f"CREATE FACT TABLE IF NOT EXISTS {table.table_name} "
            f"({table.generate_columns_string()}) "
        )

        self.connection.cursor().execute(query)

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

        insert_query = (
            f"INSERT INTO {internal_table.table_name} "
            f"SELECT {internal_table.generate_columns_string()} "
            f"FROM {external_table_name} "
        )
        if where_sql is not None:
            insert_query += f"WHERE {where_sql}"

        formatted_query = sqlparse.format(insert_query, reindent=True, indent_width=4)
        cursor.execute(query=formatted_query)
