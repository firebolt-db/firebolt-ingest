from typing import List

from pydantic import BaseModel

from firebolt_ingest.model import YamlModelMixin


class Column(BaseModel):
    name: str
    type: str


class Table(BaseModel, YamlModelMixin):
    database_name: str
    table_name: str
    columns: List[Column]
