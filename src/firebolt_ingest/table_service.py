import logging

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
    verify_ingestion_file_names,
    verify_ingestion_rowcount,
)
from firebolt_ingest.utils import format_query

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s :: %(levelname)s :: %(filename)s ::\
 %(funcName)s :: %(message)s",
)

logger = logging.getLogger(__name__)


class TableService:
    def __init__(
        self,
        table: Table,
        connection: Connection,
        external_prefix: str = "ex_",
        internal_prefix: str = "",
    ):
        """
        Table service class used for creation of external/internal tables and
        performing ingestion from external into internal table

        Args:
            table (Table): An object representing the table definition. This is used for
                creating external and internal tables and for data ingestion processes.
            connection (Connection): A database connection object used to interact with
                the database or engine where the tables are managed.
            external_prefix (str, optional): A prefix string added to the table name to
                create the name of the external table. Defaults to 'ex_'.
            internal_prefix (str, optional): A prefix string added to the table name to
                create the name of the internal table. Defaults to an empty string.
        """
        self.connection = connection
        self.table = table
        self.internal_table_name = f"{internal_prefix}{self.table.table_name}"
        self.external_table_name = f"{external_prefix}{self.table.table_name}"

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

        if not self.table.s3_url and not aws_settings.s3_url:
            raise FireboltError("S3 URL wasn't provided")

        # Prepare columns
        columns_stmt, columns_params = self.table.generate_external_columns_string()

        # Generate query
        query = (
            f"CREATE EXTERNAL TABLE {self.external_table_name}\n"
            f"({columns_stmt})\n"
            f"{cred_stmt}\n"
            f"URL = ?\n"
            f"OBJECT_PATTERN = ?\n"
            f"TYPE = ({self.table.generate_file_type()})\n"
        )
        if self.table.compression:
            query += f"COMPRESSION = {self.table.compression}\n"

        params = (
            cred_params
            + columns_params
            + [self.table.s3_url if self.table.s3_url else aws_settings.s3_url]
            + [self.table.object_pattern]
        )

        logger.info(f"Create external table with query:\n{query}")
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

        logger.info(f"Create internal table with query:\n{query}")
        self.connection.cursor().execute(format_query(query), columns_params)

    def insert_full_overwrite(
        self,
        **kwargs,
    ) -> None:
        """
        Perform a full overwrite from an external table into an internal table.
        All existing data in the internal table will be deleted, and data from the
        external table will be loaded.

        This function is appropriate for unpartitioned tables, or for
        partitioned tables you wish to fully overwrite.

        Kwargs:
            advanced_mode: (Optional)
            use_short_column_path_parquet: (Optional) Use short parquet column path
             and skipping repeated nodes and their child node
        """
        cursor = self.connection.cursor()

        # TODO: uncomment it back after the fix applied,
        # commented because of https://packboard.atlassian.net/browse/FIR-26886
        # raise_on_tables_non_compatibility(cursor,
        #                                   self.table,
        #                                   ignore_meta_columns=True)

        # get table schema
        internal_table_schema = get_table_schema(cursor, self.internal_table_name)
        internal_table_columns = get_table_columns(cursor, self.internal_table_name)

        # drop the table
        logger.info(f"Drop internal table: {self.internal_table_name}")
        drop_table(cursor, self.internal_table_name)

        # recreate the table
        logger.info(f"Create internal table:\n{internal_table_schema}")
        cursor.execute(query=internal_table_schema)

        # insert the data from external to internal
        column_names = [
            (f'"{c.name}"' + (f" AS {c.alias}" if c.alias else ""))
            for c in self.table.columns
        ]

        for c in FILE_METADATA_COLUMNS:
            name, type_ = c.name, c.type
            # Check if FILE_METADATA_COLUMNS is present in internal_table_columns
            # TIMESTAMPNTZ is sometimes represented as TIMESTAMP, need to check both
            if (name, type_) in internal_table_columns or (
                type_ == "TIMESTAMPNTZ"
                and (name, "TIMESTAMP") in internal_table_columns
            ):
                column_names.append(name)

        insert_query = (
            f"INSERT INTO {self.internal_table_name}\n"
            f"SELECT {', '.join(column_names)}\n"
            f"FROM {self.external_table_name}\n"
        )

        logger.info(f"Insert with query:\n{insert_query}")
        execute_set_statements(
            cursor,
            **kwargs,
        )
        cursor.execute(query=format_query(insert_query))

    def insert_incremental_append(self, use_materialized_query=False, **kwargs) -> None:
        """
        Insert from the external table only new files,
        that aren't in the internal table.

        Requires internal table to have file-metadata columns
        (source_file_name and source_file_timestamp)

        Args:
        use_materialized_query (bool):
            If set to True, the function uses an materialized query
                strategy for insertion.
            If set to False (default), the function uses the standard query approach.
        **kwargs: Additional keyword arguments which may be passed to other functions
            used in this method.
        Returns:

        """
        cursor = self.connection.cursor()
        # TODO: uncomment it back after the fix applied,
        # commented because of https://packboard.atlassian.net/browse/FIR-26886
        # raise_on_tables_non_compatibility(cursor,
        #                                   self.table,
        #                                   ignore_meta_columns=False)

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

        if use_materialized_query:
            # Optimized query
            insert_query = f"""
                INSERT INTO {self.internal_table_name}
                WITH a AS materialized (
                    SELECT DISTINCT source_file_name
                    FROM {self.internal_table_name}
                )
                SELECT {', '.join(column_names)},
                    source_file_name, source_file_timestamp
                FROM {self.external_table_name}
                WHERE source_file_name NOT IN (
                    SELECT source_file_name
                    FROM a
                )
            """
        else:
            insert_query = f"""
                INSERT INTO {self.internal_table_name}
                SELECT {', '.join(column_names)},
                        source_file_name, source_file_timestamp
                FROM {self.external_table_name}
                WHERE (source_file_name, source_file_timestamp::timestampntz)
                NOT IN (
                    SELECT DISTINCT source_file_name,
                                    source_file_timestamp
                    FROM {self.internal_table_name})
                """

        logger.info(f"Insert with query:\n{insert_query}")
        execute_set_statements(
            cursor,
            **kwargs,
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

    def insert(self, use_materialized_query=False, **kwargs) -> None:
        """
        Inserts data into a table based on the synchronization mode specified
        in the table's configuration.

        This method checks the `sync_mode` attribute of the associated table
        and performs an insert operation accordingly.
        If the `sync_mode` is set to "overwrite", it performs
        a full overwrite of the data in the table.
        If it's set to "append", it appends the new data incrementally.
        For any other `sync_mode` values, a ValueError is raised, indicating
        an uncertain sync mode configuration.

        Args:
        use_materialized_query (bool):
            If set to True, the function uses an materialized query
                strategy for insertion.
            If set to False (default), the function uses the standard query approach.
        **kwargs: Additional keyword arguments which may be passed to other functions
            used in this method.
        """
        if self.table.sync_mode == "overwrite":
            self.insert_full_overwrite(**kwargs)
        elif self.table.sync_mode == "append":
            self.insert_incremental_append(
                use_materialized_query=use_materialized_query, **kwargs
            )
        else:
            raise ValueError(
                "Uncertain sync mode in config \
                use insert_full_overwrite/insert_incremental_append instead"
            )

    def drop_internal_table(self) -> None:
        """
        Drops the internal table associated with the current object.
        """
        logger.info(f"Drop internal table: {self.internal_table_name}")
        cursor = self.connection.cursor()
        drop_table(cursor, self.internal_table_name)

    def drop_external_table(self) -> None:
        """
        Drops the external table associated with the current object.
        """
        logger.info(f"Drop external table: {self.external_table_name}")
        cursor = self.connection.cursor()
        drop_table(cursor, self.external_table_name)

    def drop_tables(self) -> None:
        """
        Drops both internal and external tables associated with the current object.
        """
        self.drop_internal_table()
        self.drop_external_table()

    def does_external_table_exist(self) -> bool:
        """
        Checks if the external table exists in the database.
        """
        return does_table_exist(self.connection.cursor(), self.external_table_name)

    def does_internal_table_exist(self) -> bool:
        """
        Checks if the internal table exists in the database.
        """
        return does_table_exist(self.connection.cursor(), self.internal_table_name)

    def drop_outdated_partitions(self):
        """
        Drops partitions in the fact table that are outdated, meaning the corresponding
            file in the external table has a more recent timestamp (was updated).
        """
        cursor = self.connection.cursor()
        if not does_table_exist(cursor, self.internal_table_name):
            raise FireboltError(f"Fact table {self.internal_table_name} doesn't exist")
        if not does_table_exist(cursor, self.external_table_name):
            raise FireboltError(
                f"External table {self.external_table_name} doesn't exist"
            )
        if not self.table.partitions:
            raise FireboltError(
                f"Fact table {self.internal_table_name} is not partitioned"
            )

        outdated_partitions_query = f"SELECT DISTINCT {','.join([p.as_sql_string() for p in self.table.partitions])} \
        FROM {self.external_table_name} \
        WHERE source_file_timestamp > ( SELECT MAX(source_file_timestamp) \
            FROM {self.internal_table_name} )"

        cursor.execute(query=format_query(outdated_partitions_query))
        outdated_partitions = cursor.fetchall()
        logger.info(f"List of outdated partitions: {outdated_partitions}")

        if outdated_partitions:
            q = "ALTER TABLE {table_name} DROP PARTITION {partition_expression}"
            for outdated_partition in outdated_partitions:
                logger.debug(
                    f"Going to drop the following partitions: {outdated_partition}"
                )
                drop_partition_query = q.format(
                    table_name=self.internal_table_name,
                    partition_expression=",".join(map(str, outdated_partition)),
                )
                cursor.execute(query=drop_partition_query)
