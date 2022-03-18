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

        Returns: Nothing in case of success

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
            f"OBJECT_PATTERN = ? "
            f"TYPE = ({table.type.name})"
        )
        params = cred_params + [self.aws_settings.s3_url, table.object_pattern]

        # Execute parametrized query
        self.connection.cursor().execute(query, params)

    def create_internal_table(self, table: Table) -> None:
        """

        Args:
            table:

        Returns:

        """
        raise NotImplementedError("Not yet implemented")
