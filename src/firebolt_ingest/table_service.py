from firebolt.common.exception import FireboltError
from firebolt.db.connection import Connection

from firebolt_ingest.aws_settings import (
    AWSSettings,
    generate_aws_credentials_string,
)
from firebolt_ingest.table_model import FILE_METADATA_COLUMNS, Table
from firebolt_ingest.table_utils import (
    does_table_exist,
    drop_table,
    execute_set_statements,
    get_table_columns,
    get_table_schema,
    raise_on_tables_non_compatibility,
    verify_ingestion_file_names,
    verify_ingestion_rowcount,
)
from firebolt_ingest.utils import format_query

EXTERNAL_TABLE_PREFIX = "ex_"


class TableService:
    def __init__(self, table: Table, connection: Connection):
        """
        Table service class used for creation of external/internal tables and
        performing ingestion from external into internal table

        Args:
            table: a definition of table, which will be used for
             creating tables and performing an ingestion
            connection: a connection to some database/engine
        """
        self.connection = connection
        self.table = table
        self.internal_table_name = self.table.table_name
        self.external_table_name = f"{EXTERNAL_TABLE_PREFIX}{self.table.table_name}"

    def create_external_table(self, aws_settings: AWSSettings) -> None:
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
        columns_stmt, columns_params = self.table.generate_external_columns_string()

        # Generate query
        query = (
            f"CREATE EXTERNAL TABLE {self.external_table_name}\n"
            f"({columns_stmt})\n"
            f"{cred_stmt}\n"
            f"URL = ?\n"
            f"OBJECT_PATTERN = {', '.join(['?'] * len(self.table.object_pattern))}\n"
            f"TYPE = ({self.table.generate_file_type()})\n"
        )
        if self.table.compression:
            query += f"COMPRESSION = {self.table.compression}\n"

        params = (
            cred_params
            + columns_params
            + [aws_settings.s3_url]
            + self.table.object_pattern
        )

        # Execute parametrized query
        self.connection.cursor().execute(format_query(query), params)

    def create_internal_table(self, add_file_metadata=True) -> None:
        """
        Constructs a query for creating an internal table and executes it

        Args:
            table: table definition
        """

        columns_stmt, columns_params = self.table.generate_internal_columns_string(
            add_file_metadata
        )
        query = (
            f"CREATE FACT TABLE {self.internal_table_name}\n"
            f"({columns_stmt})\n"
            f"PRIMARY INDEX {self.table.generate_primary_index_string()}\n"
        )

        if self.table.partitions:
            query += f"PARTITION BY {self.table.generate_partitions_string()}\n"  # noqa: E501

        self.connection.cursor().execute(format_query(query), columns_params)

    def insert_full_overwrite(
        self,
        firebolt_dont_wait_for_upload_to_s3: bool = False,
        advanced_mode: bool = False,
        use_short_column_path_parquet: bool = False,
    ) -> None:
        """
        Perform a full overwrite from an external table into an internal table.
        All existing data in the internal table will be deleted, and data from the
        external table will be loaded.

        This function is appropriate for unpartitioned tables, or for
        partitioned tables you wish to fully overwrite.

        Args:
            firebolt_dont_wait_for_upload_to_s3: (Optional) if set, the insert will not
                wait until the changes will be written to s3.
            advanced_mode: (Optional)
            use_short_column_path_parquet: (Optional) Use short parquet column path
             and skipping repeated nodes and their child node
        """
        cursor = self.connection.cursor()

        raise_on_tables_non_compatibility(cursor, self.table, ignore_meta_columns=True)

        # get table schema
        internal_table_schema = get_table_schema(cursor, self.internal_table_name)
        internal_table_columns = get_table_columns(cursor, self.internal_table_name)

        # drop the table
        drop_table(cursor, self.internal_table_name)

        # recreate the table
        cursor.execute(query=internal_table_schema)

        # insert the data from external to internal
        column_names = [
            (f'"{c.name}"' + (f" AS {c.alias}" if c.alias else ""))
            for c in self.table.columns
        ]
        if set((c.name, c.type) for c in FILE_METADATA_COLUMNS).issubset(
            internal_table_columns
        ):
            column_names.append("source_file_name")
            column_names.append("source_file_timestamp")

        insert_query = (
            f"INSERT INTO {self.internal_table_name}\n"
            f"SELECT {', '.join(column_names)}\n"
            f"FROM {self.external_table_name}\n"
        )

        execute_set_statements(
            cursor,
            firebolt_dont_wait_for_upload_to_s3,
            advanced_mode,
            use_short_column_path_parquet,
        )
        cursor.execute(query=format_query(insert_query))

    def insert_incremental_append(
        self,
        firebolt_dont_wait_for_upload_to_s3: bool = False,
        advanced_mode: bool = False,
        use_short_column_path_parquet: bool = False,
    ) -> None:
        """
        Insert from the external table only new files,
        that aren't in the internal table.

        Requires internal table to have file-metadata columns
        (source_file_name and source_file_timestamp)

        Args:
            firebolt_dont_wait_for_upload_to_s3: (Optional) if set, the insert will not
                wait until the changes will be written to s3.
            advanced_mode: (Optional)
            use_short_column_path_parquet: (Optional) Use short parquet column path
             and skipping repeated nodes and their child node
        Returns:

        """
        cursor = self.connection.cursor()
        raise_on_tables_non_compatibility(cursor, self.table, ignore_meta_columns=False)

        if not does_table_exist(cursor, self.internal_table_name):
            raise FireboltError(f"Fact table {self.internal_table_name} doesn't exist")
        if not does_table_exist(cursor, self.external_table_name):
            raise FireboltError(
                f"External table {self.external_table_name} doesn't exist"
            )

        column_names = [
            (f'"{c.name}"' + (f" AS {c.alias}" if c.alias else ""))
            for c in self.table.columns
        ]
        insert_query = f"""
                       INSERT INTO {self.internal_table_name}
                       SELECT {', '.join(column_names)},
                              source_file_name, source_file_timestamp
                       FROM {self.external_table_name}
                       WHERE (source_file_name, source_file_timestamp)
                       NOT IN (
                            SELECT DISTINCT source_file_name,
                                            source_file_timestamp
                            FROM {self.internal_table_name})
                       """

        execute_set_statements(
            cursor,
            firebolt_dont_wait_for_upload_to_s3,
            advanced_mode,
            use_short_column_path_parquet,
        )
        cursor.execute(query=format_query(insert_query))

    def verify_ingestion(self) -> bool:
        """
        verify ingestion by running a sequence of verification, currently implemented:
        - verification by rowcount
        """

        cursor = self.connection.cursor()
        return verify_ingestion_rowcount(
            cursor, self.internal_table_name, self.external_table_name
        ) and verify_ingestion_file_names(cursor, self.internal_table_name)
