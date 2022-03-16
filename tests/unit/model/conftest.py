import pytest
import yaml


@pytest.fixture
def table_dict() -> dict:
    return {
        "database_name": "test_db",
        "table_name": "test_table",
        "columns": [
            {"name": "test_col_1", "type": "int"},
            {"name": "test_col_2", "type": "string"},
        ],
    }


@pytest.fixture
def table_yaml_string(table_dict) -> str:
    return yaml.dump(table_dict)
