from firebolt_ingest.model.table import Table


def test_table_from_yaml(table_yaml_string, table_dict):
    table = Table.parse_yaml(table_yaml_string)
    assert table.dict() == table_dict
