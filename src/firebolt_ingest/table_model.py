from enum import Enum
from typing import Any, List, Optional, Tuple

import yaml
from pydantic import BaseModel, Field, ValidationError, conlist, root_validator
from pydantic.main import ModelMetaclass
from yaml import Loader


class YamlModelMixin(metaclass=ModelMetaclass):
    """
    Provides a parse_yaml method to a Pydantic model class.
    """

    @classmethod
    def parse_yaml(cls, yaml_obj: Any):
        obj = yaml.load(yaml_obj, Loader=Loader)
        return cls.parse_obj(obj)  # type: ignore


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
    nullable: Optional[bool] = None
    unique: Optional[bool] = None

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
    columns: conlist(Column, min_items=1)  # type: ignore
    primary_index: conlist(str, min_items=1)  # type: ignore
    partitions: List[Partition] = []
    file_type: str
    object_pattern: conlist(str, min_items=1)  # type: ignore
    compression: Optional[str] = None
    csv_skip_header_row: Optional[bool] = None
    json_parse_as_text: Optional[bool] = None

    @root_validator
    def compression_validator(cls, values: dict) -> dict:
        """
        Check whether compression has one of allowed values: {"GZIP"}
        """
        if values.get("compression") is None:
            return values

        values["compression"] = values["compression"].upper()

        if values.get("compression") not in {"GZIP"}:
            raise ValidationError(["unknown compression"], type(cls))

        return values

    @root_validator
    def file_type_validator(cls, values: dict) -> dict:
        """
        Check whether file_type has one of allowed values:
            {"CSV", "JSON", "ORC", "PARQUET", "TCV"}
        """
        values["file_type"] = values["file_type"].upper()

        if values.get("file_type") not in {"CSV", "JSON", "ORC", "PARQUET", "TCV"}:
            raise ValidationError(["unknown file type"], type(cls))

        return values

    @root_validator
    def file_type_additional_fields(cls, values: dict) -> dict:
        """
        validate that if csv_skip_header_row is set, then the file type is CSV
        and if json_parse_as_text is set, the file type is JSON
        """
        if values.get("csv_skip_header_row") and values.get("file_type") != "CSV":
            raise ValidationError(
                ["csv_skip_header_row is only relevant for CSV file_type"], type(cls)
            )

        if values.get("json_parse_as_text") and values.get("file_type") != "JSON":
            raise ValidationError(
                ["json_parse_as_text is only relevant for JSON file_type"], type(cls)
            )

        return values

    @root_validator
    def primary_index_columns(cls, values: dict) -> dict:
        """
        Ensure the primary index column names exist in the list of columns.
        """
        column_names = {c.name for c in values["columns"]}
        for index_column in values.get("primary_index", []):
            if index_column not in column_names:
                raise ValidationError(
                    [
                        f"Could not find primary index {index_column}"
                        f" in the list of table columns."
                    ],
                    type(cls),
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
                raise ValidationError(
                    [
                        f"Could not find partition column name {partition.column_name} "
                        f"in the list of table columns"
                    ],
                    type(cls),
                )

            if partition.datetime_part is not None:
                partition_column_type = column_name_to_type.get(partition.column_name)
                if partition_column_type not in DATE_TIME_TYPES:
                    raise ValidationError(
                        [
                            f"Partition column {partition.column_name} must be a "
                            f"compatible datetime type, not a {partition_column_type}"
                        ],
                        type(cls),
                    )
        return values

    def generate_file_type(self) -> str:
        """
        Returns: a string with file_type and relevant argument
        """
        additional_params = ""
        if self.file_type == "CSV" and self.csv_skip_header_row is not None:
            additional_params = (
                f" SKIP_HEADER_ROWS = {'1' if self.csv_skip_header_row else '0'}"
            )
        elif self.file_type == "JSON" and self.json_parse_as_text is not None:
            additional_params = (
                f" PARSE_AS_TEXT = '{'TRUE' if self.json_parse_as_text else 'FALSE'}'"
            )

        return f"{self.file_type}{additional_params}"

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

        columns_str = []
        for column in self.columns + additional_partitions:
            column_str = f"{column.name} {column.type}"

            column_str += " UNIQUE" if column.unique else ""

            if column.nullable is not None:
                column_str += " NULL" if column.nullable else " NOT NULL"

            columns_str.append(column_str)

        return ", ".join(columns_str), []

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

    def generate_partitions_string(self) -> str:
        """
        Generate a prepared sql string from list of partition columns to
        be used in the creation of internal partitioned tables.

        Args:
            source_file_timestamp columns as partition columns.
        """
        return ",".join([p.as_sql_string() for p in self.partitions])
