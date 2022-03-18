from enum import Enum
from typing import List

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


def match_array(s: str) -> bool:
    """
    Check whether a string is a valid array and return true
    """
    if s.startswith("ARRAY(") and s.endswith(")"):
        return match_array(s[6:-1])
    if s in atomic_type:
        return True
    return False


class ObjectTypes(str, Enum):
    ORC = "ORC"
    PARQUET = "PARQUET"
    TSV = "TSV"


class Column(BaseModel):
    name: str = Field(min_length=1, max_length=255, regex=r"^[0-9a-zA-Z_]+$")
    type: str = Field(min_length=1, max_length=255)

    @root_validator
    def type_validator(cls, values: dict) -> dict:
        if values["type"] in atomic_type or match_array(values["type"]):
            return values

        raise ValueError("unknown column type")


class Table(BaseModel, YamlModelMixin):
    table_name: str = Field(min_length=1, max_length=255, regex=r"^[0-9a-zA-Z_]+$")
    columns: List[Column]
    type: ObjectTypes
    object_pattern: str = Field(min_length=1, max_length=255)

    @root_validator
    def non_empty_column_list(cls, values: dict) -> dict:
        if len(values["columns"]) == 0:
            raise ValueError("Table should have at least one column")

        return values

    def generate_columns_string(self) -> str:
        """
        Function generates a prepared string from list of columns to
        be used in creation of external or internal table

        Args:
            columns: the list of columns

        Returns:
            a prepared string
        """

        return ", ".join([f"{column.name} {column.type}" for column in self.columns])
