import pytest

from firebolt_ingest.model.table import Column, Table


def test_table_from_yaml(table_yaml_string, table_dict):
    table = Table.parse_yaml(table_yaml_string)
    assert table.dict() == table_dict


def test_column():
    Column(name="name", type="INT")
    Column(name="name", type="ARRAY(INT)")
    Column(name="name", type="ARRAY(ARRAY(INT))")

    with pytest.raises(ValueError):
        Column(name="name", type="ARRAY()")
        Column(name="name", type="ARRAY(INT")
        Column(name="name", type="ARRAY(INT))")
        Column(name="name", type="NOTLONG")
        Column(name="name", type="INT NULL")
