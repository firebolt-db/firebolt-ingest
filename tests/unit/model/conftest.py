import pytest
import yaml


@pytest.fixture
def table_dict() -> dict:
    return {
        "table_name": "test_table",
        "columns": [
            {"name": "test_col_1", "type": "INT"},
            {"name": "test_col_2", "type": "TEXT"},
            {"name": "test_col_3", "type": "DATE"},
        ],
        "primary_index": ["test_col_1"],
        "partitions": [
            {"column_name": "test_col_2", "datetime_part": None},
            {"column_name": "test_col_3", "datetime_part": "DAY"},
        ],
        "file_type": "PARQUET",
        "object_pattern": ["*0.parquet", "*1.parquet"],
        "compression": "GZIP",
    }


@pytest.fixture
def table_yaml_string(table_dict) -> str:
    return yaml.dump(table_dict)
