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


def test_generate_columns_string(mock_table):
    """
    test generate columns string with 0, 1 and multiple columns
    """

    mock_table.columns = []
    assert mock_table.generate_columns_string() == ""

    mock_table.columns = [Column(name="id", type="TEXT")]
    assert mock_table.generate_columns_string() == "id TEXT"

    mock_table.columns = [
        Column(name="id", type="TEXT"),
        Column(name="part", type="INT"),
    ]
    assert mock_table.generate_columns_string() == "id TEXT, part INT"
