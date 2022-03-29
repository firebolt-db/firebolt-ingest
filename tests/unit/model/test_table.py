import pytest

from firebolt_ingest.model.table import (
    Column,
    DatetimePart,
    FileType,
    Partition,
    Table,
)


def test_table_from_yaml(table_yaml_string, table_dict):
    """
    Test that we can parse a yaml file into a Table object
    """
    table = Table.parse_yaml(table_yaml_string)
    assert table.dict() == table_dict


def test_column():
    """
    Test that we can create valid columns
    Test that validations fail if a user provides a bad type for a column
    """
    Column(name="name", type="INT")
    Column(name="name", type="ARRAY(INT)")
    Column(name="name", type="ARRAY(ARRAY(INT))")

    with pytest.raises(ValueError):
        Column(name="name", type="ARRAY()")
        Column(name="name", type="ARRAY(INT")
        Column(name="name", type="ARRAY(INT))")
        Column(name="name", type="NOTLONG")
        Column(name="name", type="INT NULL")


def test_partition_extract_type():
    """
    Ensure we raise an error when a user tries to extract from a non-datetime column.
    """
    with pytest.raises(ValueError):
        Table(
            table_name="test_table_1",
            columns=[Column(name="col_1", type="INT")],
            partitions=[Partition(column_name="col_1", datetime_part=DatetimePart.DAY)],
            file_type=FileType.PARQUET,
            object_pattern="*.parquet",
        )


def test_partition_valid_column():
    """
    Ensure we raise an error when a user specifies a partition not in the column list.
    """
    with pytest.raises(ValueError):
        Table(
            table_name="test_table_1",
            columns=[Column(name="col_1", type="INT")],
            partitions=[Partition(column_name="bad_col")],
            file_type=FileType.PARQUET,
            object_pattern="*.parquet",
        )


def test_generate_columns_string(mock_table):
    """
    Test generate columns string with 0, 1 and multiple columns
    """

    mock_table.columns = []
    assert mock_table.generate_columns_string(add_file_metadata=False) == ""

    mock_table.columns = [Column(name="id", type="TEXT")]
    assert mock_table.generate_columns_string(add_file_metadata=False) == "id TEXT"

    mock_table.columns = [
        Column(name="id", type="TEXT"),
        Column(name="part", type="INT"),
    ]

    assert (
        mock_table.generate_columns_string(add_file_metadata=False)
        == "id TEXT, part INT"
    )

    assert (
        mock_table.generate_columns_string(add_file_metadata=True)
        == "id TEXT, part INT, source_file_name STRING, source_file_timestamp DATETIME"
    )


def test_empty_object_pattern(table_dict):
    """
    Ensure an empty object pattern raises a validation error
    """

    table_dict["object_pattern"] = []
    with pytest.raises(ValueError) as e:
        Table.parse_obj(table_dict)

    assert "object pattern" in str(e)
