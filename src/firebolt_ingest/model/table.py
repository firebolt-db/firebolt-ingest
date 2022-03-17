from enum import Enum
from typing import List

from pydantic import BaseModel, Field, root_validator

from firebolt_ingest.model import YamlModelMixin


class TypesEnum(str, Enum):
    INT = "INT"
    LONG = "LONG"
    TEXT = "TEXT"


class ObjectTypes(str, Enum):
    ORC = "ORC"
    PARQUET = "PARQUET"
    TSV = "TSV"


class Column(BaseModel):
    name: str = Field(min_length=1, max_length=255, regex=r"^[0-9a-zA-Z_]+$")
    type: TypesEnum


class Table(BaseModel, YamlModelMixin):
    database_name: str
    table_name: str
    columns: List[Column]
    type: ObjectTypes
    object_pattern: str = Field(min_length=1, max_length=255)

    @root_validator
    def non_empty_column_list(cls, values: dict) -> dict:
        if len(values["columns"]) == 0:
            raise ValueError("Table should have at least one column")

        return values
