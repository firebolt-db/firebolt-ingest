from enum import Enum
from typing import List, Literal, Optional, Tuple

from pydantic import BaseModel, Field, root_validator

from firebolt_ingest.model import YamlModelMixin

# see: https://docs.firebolt.io/general-reference/data-types.html
ATOMIC_TYPES = {
    "INT",
    "INTEGER",
    "BIGINT",
    "LONG",
    "FLOAT",
    "DOUBLE",
    "DOUBLE PRECISION",
    "TEXT",
    "VARCHAR",
    "STRING",
    "DATE",
    "DATETIME",
    "TIMESTAMP",
    "BOOLEAN",
}

DATE_TIME_TYPES = {"DATE", "TIMESTAMP", "DATETIME"}


def match_array(s: str) -> bool:
    """
    Check whether a string is a valid array and return true
    """
    if s.startswith("ARRAY(") and s.endswith(")"):
        return match_array(s[6:-1])
    if s in ATOMIC_TYPES:
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
    extract_partition: Optional[str] = Field(min_length=1, max_length=255)

    @root_validator
    def type_validator(cls, values: dict) -> dict:
        if values["type"] in ATOMIC_TYPES or match_array(values["type"]):
            return values

        raise ValueError("unknown column type")


FILE_METADATA_COLUMNS: List[Column] = [
    Column(name="source_file_name", type="STRING"),
    Column(name="source_file_timestamp", type="DATETIME"),
]


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
    object_pattern: List[str]
    compression: Optional[Literal["GZIP"]] = None

    @root_validator
    def non_empty_object_pattern(cls, values: dict) -> dict:
        if not values.get("object_pattern"):
            raise ValueError("At least one object pattern has to be specified")

        return values

    @root_validator
    def non_empty_column_list(cls, values: dict) -> dict:
        if not values.get("columns"):
            raise ValueError("Table should have at least one column")

        return values

    @root_validator
    def primary_index_columns(cls, values: dict) -> dict:
        """
        Ensure the primary index column names exist in the list of columns.
        """
        column_names = {c.name for c in values["columns"]}
        for index_column in values.get("primary_index", []):
            if index_column not in column_names:
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

            if partition.column_name not in column_name_to_type.keys():
                raise ValueError(
                    f"Could not find partition column name {partition.column_name} "
                    f"in the list of table columns"
                )

            if partition.datetime_part is not None:
                partition_column_type = column_name_to_type.get(partition.column_name)
                if partition_column_type not in DATE_TIME_TYPES:
                    raise ValueError(
                        f"Partition column {partition.column_name} must be a "
                        f"compatible datetime type, not a {partition_column_type}"
                    )
        return values

    def generate_internal_columns_string(
        self, add_file_metadata: bool
    ) -> Tuple[str, List]:
        """

        Generate a prepared sql string from list of columns to
        be used in the creation of internal tables.

        Args:
            add_file_metadata: If true, add the source_file_name and
            source_file_timestamp to the list of columns.

        Returns:
            a tuple with partial sql prepared statement and
            list of prepared statement arguments
        """
        additional_partitions = FILE_METADATA_COLUMNS if add_file_metadata else []
        return (
            ", ".join(
                [
                    f"{column.name} {column.type}"
                    for column in self.columns + additional_partitions
                ]
            ),
            [],
        )

    def generate_external_columns_string(self) -> Tuple[str, List]:
        """
        Generate a prepared sql string from list of columns to
        be used in the creation of external table.

        Returns:
            a tuple with partial sql prepared statement and
            list of prepared statement arguments
        """

        column_strings = []
        for column in self.columns:
            column_strings.append(
                f"{column.name} {column.type}"
                f"{' PARTITION(?)' if column.extract_partition else ''}"
            )

        return ", ".join(column_strings), [
            column.extract_partition
            for column in self.columns
            if column.extract_partition
        ]

    def generate_primary_index_string(self) -> str:
        """
        Generate a prepared sql string from list of primary index columns to
        be used in the creation of internal tables with a primary index.
        """
        return ", ".join([index for index in self.primary_index])

    def generate_partitions_string(self, add_file_metadata: bool) -> str:
        """
        Generate a prepared sql string from list of partition columns to
        be used in the creation of internal partitioned tables.

        Args:
            add_file_metadata: If true, add the source_file_name and
            source_file_timestamp columns as partition columns.
        """
        additional_partitions = (
            [Partition(column_name=c.name) for c in FILE_METADATA_COLUMNS]
            if add_file_metadata
            else []
        )
        return ",".join(
            [p.as_sql_string() for p in self.partitions + additional_partitions]
        )
