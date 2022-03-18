import pytest
import yaml


@pytest.fixture
def table_dict() -> dict:
    return {
        "table_name": "test_table",
        "columns": [
            {"name": "test_col_1", "type": "INT"},
            {"name": "test_col_2", "type": "TEXT"},
        ],
        "type": "PARQUET",
        "object_pattern": "*.parquet",
    }


@pytest.fixture
def table_yaml_string(table_dict) -> str:
    return yaml.dump(table_dict)
