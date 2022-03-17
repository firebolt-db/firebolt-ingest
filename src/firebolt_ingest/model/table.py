from enum import Enum
from typing import List

from pydantic import BaseModel, Field

from firebolt_ingest.model import YamlModelMixin


class TypesEnum(str, Enum):
    INT = "INT"
    TEXT = "TEXT"


class Column(BaseModel):
    name: str = Field(min_length=1, max_length=255, regex=r"^[0-9a-zA-Z_]+$")
    type: TypesEnum


class Table(BaseModel, YamlModelMixin):
    database_name: str
    table_name: str
    columns: List[Column]
