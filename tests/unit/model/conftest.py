import pytest
import yaml


@pytest.fixture
def table_dict() -> dict:
    return {
        "table_name": "test_table",
        "columns": [
            {
                "name": "test_col_1",
                "type": "INT",
                "extract_partition": "[^\\/]+\\/c_type=([^\\/]+)\\/[^\\/]+\\/[^\\/]+",
            },
            {"name": "test_col_2", "type": "TEXT"},
            {"name": "test_col_3", "type": "DATE"},
        ],
        "partitions": [
            {"column_name": "test_col_2"},
            {"column_name": "test_col_3", "datetime_part": "DAY"},
        ],
        "file_type": "PARQUET",
        "object_pattern": "*.parquet",
    }


@pytest.fixture
def table_yaml_string(table_dict) -> str:
    return yaml.dump(table_dict)
