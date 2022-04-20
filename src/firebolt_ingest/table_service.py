from firebolt.async_db.connection import Connection

from firebolt_ingest.aws_settings import (
    AWSSettings,
    generate_aws_credentials_string,
)
from firebolt_ingest.table_model import Table
from firebolt_ingest.table_utils import (
    drop_table,
    get_table_columns,
    get_table_schema,
)

EXTERNAL_TABLE_PREFIX = "ex_"


class TableService:
    def __init__(self, connection: Connection):
        """
        Table service class used for creation of external/internal tables and
        performing ingestion from external into internal table

        Args:
            connection: a connection to some database/engine
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
