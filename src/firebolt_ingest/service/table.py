from typing import List

from firebolt.async_db.connection import Connection

from firebolt_ingest.aws_settings import (
    AWSSettings,
    generate_aws_credentials_string,
)
from firebolt_ingest.model.table import Column, Table


def generate_columns_string(columns: List[Column]) -> str:
    """
    Function generates a prepared string from list of columns to
    be used in creation of external or internal table

    Args:
        columns: the list of columns

    Returns:
        a prepared string
    """

    return ", ".join([f"{column.name} {column.type}" for column in columns])


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
        columns = generate_columns_string(table.columns)

        # Generate query
        query = (
            f"CREATE EXTERNAL TABLE IF NOT EXISTS ? "
            f"({columns}) "
            f"{cred_stmt} "
            f"URL = ? "
            f"OBJECT_PATTERN = ? "
            f"TYPE = (?)"
        )
        params = [
            [table.table_name]
            + cred_params
            + [self.aws_settings.s3_url, table.object_pattern, table.type.name]
        ]

        # Execute parametrized query
        self.connection.cursor().execute(query, params)

    def create_internal_table(self, table: Table) -> None:
        """

        Args:
            table:

        Returns:

        """
        raise NotImplementedError("Not yet implemented")
