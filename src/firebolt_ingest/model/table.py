from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, root_validator

from firebolt_ingest.model import YamlModelMixin

atomic_type = {
    "INT",
    "INTEGER",
    "BIGINT",
    "LONG",
    "FLOAT",
    "DOUBLE",
    "TEXT",
    "VARCHAR",
    "STRING",
    "DATE",
}

date_time_types = {"DATE", "TIMESTAMP", "DATETIME"}


def match_array(s: str) -> bool:
    """
    Check whether a string is a valid array and return true
    """
    if s.startswith("ARRAY(") and s.endswith(")"):
        return match_array(s[6:-1])
    if s in atomic_type:
        return True
    return False


class FileType(str, Enum):
    ORC = "ORC"
    PARQUET = "PARQUET"
    TSV = "TSV"


class DatetimePart(str, Enum):
    DAY = "DAY"
    DOW = "DOW"
    MONTH = "MONTH"
    WEEK = "WEEK"
    WEEKISO = "WEEKISO"
    QUARTER = "QUARTER"
    YEAR = "YEAR"
    HOUR = "HOUR"
    MINUTE = "MINUTE"
    SECOND = "SECOND"
    EPOCH = "EPOCH"


class Column(BaseModel):
    name: str = Field(min_length=1, max_length=255, regex=r"^[0-9a-zA-Z_]+$")
    type: str = Field(min_length=1, max_length=255)

    @root_validator
    def type_validator(cls, values: dict) -> dict:
        if values["type"] in atomic_type or match_array(values["type"]):
            return values

        raise ValueError("unknown column type")


class Partition(BaseModel):
    """
    A partition column or expression.
    If a DatetimePart is provided, it will be an expression.
    Currently, the only supported expressions are date/time EXTRACT(...)

    see: https://docs.firebolt.io/working-with-partitions.html
    see: https://docs.firebolt.io/sql-reference/functions-reference/date-and-time-functions.html#extract  # noqa: E501
    """

    column_name: str
    datetime_part: Optional[DatetimePart]

    def as_sql_string(self) -> str:
        if self.datetime_part is not None:
            return f"EXTRACT({self.datetime_part} FROM {self.column_name})"
        return self.column_name


class Table(BaseModel, YamlModelMixin):
    table_name: str = Field(min_length=1, max_length=255, regex=r"^[0-9a-zA-Z_]+$")
    columns: List[Column]
    primary_index: List[str] = []
    partitions: List[Partition] = []
    file_type: FileType
    object_pattern: str = Field(min_length=1, max_length=255)

    @root_validator
    def non_empty_column_list(cls, values: dict) -> dict:
        if len(values["columns"]) == 0:
            raise ValueError("Table should have at least one column")

        return values

    @root_validator
    def primary_index_columns(cls, values: dict) -> dict:
        """
        Ensure the primary index column names exist in the list of columns.
        """
        for index_column in values.get("primary_index", []):
            if index_column not in {c.name for c in values["columns"]}:
                raise ValueError(
                    f"Could not find primary index {index_column} "
                    f"in the list of table columns."
                )
        return values

    @root_validator
    def partition_columns(cls, values: dict) -> dict:
        """
        Ensure the partition column_name exists in the list of columns.
        Ensure partition columns that use EXTRACT refer to date/time columns.
        """
        column_name_to_type = {c.name: c.type for c in values["columns"]}
        for partition in values.get("partitions", []):
            if partition.datetime_part is None:
                continue

            if partition.column_name not in column_name_to_type.keys():
                raise ValueError(
                    f"Could not find partition column name {partition.column_name} "
                    f"in the list of table columns"
                )

            partition_column_type = column_name_to_type.get(partition.column_name)

            if partition_column_type not in date_time_types:
                raise ValueError(
                    f"Partition column {partition.column_name} must be a "
                    f"compatible datetime type, not a {partition_column_type}"
                )
        return values

    def generate_columns_string(self) -> str:
        """
        Generate a prepared sql string from list of columns to
        be used in the creation of external or internal tables.
        """
        return ", ".join([f"{column.name} {column.type}" for column in self.columns])

    def generate_primary_index_string(self) -> str:
        """
        Generate a prepared sql string from list of primary index columns to
        be used in the creation of internal tables with a primary index.
        """
        return ", ".join([index for index in self.primary_index])

    def generate_partitions_string(self) -> str:
        """
        Generate a prepared sql string from list of partition columns to
        be used in the creation of internal partitioned tables.
        """
        return ",".join([p.as_sql_string() for p in self.partitions])
