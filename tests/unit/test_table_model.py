import pytest
from pydantic import ValidationError

from firebolt_ingest.table_model import Column, DatetimePart, Partition, Table


def prune_nested_dict(d):
    """Prune items from dictionaries where value is None"""
    if isinstance(d, dict):
        return {k: prune_nested_dict(v) for k, v in d.items() if v is not None}
    elif isinstance(d, list):
        return [prune_nested_dict(i) for i in d if i is not None]
    else:
        return d


def test_table_from_yaml(table_yaml_string, table_dict):
    """
    Test that we can parse a yaml file into a Table object
    """
    table = Table.parse_yaml(table_yaml_string)
    assert prune_nested_dict(table.dict()) == table_dict


def test_column():
    """
    Test that we can create valid columns
    Test that validations fail if a user provides a bad type for a column
    """
    with pytest.raises(ValueError):
        Column(name="name.member", type="INT")

    Column(name="name", alias="smth", type="INT")
    Column(name="name.member", alias="name", type="INT")
    Column(name="name", type="ARRAY(INT)")
    Column(name="name", type="ARRAY(ARRAY(INT))")

    with pytest.raises(ValueError):
        Column(name="name", type="ARRAY()")
    with pytest.raises(ValueError):
        Column(name="name", type="ARRAY(INT")
    with pytest.raises(ValueError):
        Column(name="name", type="ARRAY(INT))")
    with pytest.raises(ValueError):
        Column(name="name", type="NOTLONG")
    with pytest.raises(ValueError):
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
            file_type="PARQUET",
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
            file_type="PARQUET",
            object_pattern="*.parquet",
        )


def test_generate_external_columns_string(mock_table):
    """
    Test generate external columns string with 0, 1 and multiple columns
    """

    mock_table.columns = []
    assert mock_table.generate_external_columns_string() == ("", [])

    mock_table.columns = [Column(name="id", alias="last_id", type="TEXT")]
    assert mock_table.generate_external_columns_string() == ('"id" TEXT', [])

    mock_table.columns = [
        Column(name="id", type="TEXT"),
        Column(name="part.name", alias="part_alias", type="INT"),
    ]
    assert mock_table.generate_external_columns_string() == (
        '"id" TEXT, "part.name" INT',
        [],
    )

    mock_table.columns = [
        Column(name="id", type="TEXT", extract_partition=".*"),
        Column(name="part", type="INT"),
    ]
    assert mock_table.generate_external_columns_string() == (
        '"id" TEXT PARTITION(?), "part" INT',
        [".*"],
    )


def test_generate_internal_columns_string(mock_table):
    """
    Test generate internal columns simple, with metadata,
    and with additional nullable, unique options
    """
    mock_table.columns = [
        Column(name="id", type="TEXT", extract_partition=".*"),
        Column(name="part", type="INT", alias="part_alias"),
    ]
    assert mock_table.generate_internal_columns_string(add_file_metadata=False) == (
        "id TEXT, part_alias INT",
        [],
    )

    assert mock_table.generate_internal_columns_string(add_file_metadata=True) == (
        "id TEXT, part_alias INT, source_file_name TEXT, "
        "source_file_timestamp TIMESTAMP",
        [],
    )

    mock_table.columns = [
        Column(name="id", type="TEXT", nullable=True),
        Column(name="part", type="INT", unique=True),
        Column(name="part1", type="INT", unique=False, nullable=False),
    ]

    assert mock_table.generate_internal_columns_string(add_file_metadata=False) == (
        "id TEXT NULL, part INT UNIQUE, part1 INT NOT NULL",
        [],
    )


def test_hyphen_in_column_name(table_dict):
    """
    Ensure a hyphen in a column name without alias raises a validation error
    """

    table_dict["columns"] = [{"name": "test_col-4", "type": "TEXT"}]
    with pytest.raises(ValidationError) as e:
        Table.parse_obj(table_dict)

    assert "grouping alias" in str(e)


def test_dot_in_column_name(table_dict):
    """
    Ensure a dot in a column name without alias raises a validation error
    """

    table_dict["columns"] = [{"name": "test_col.4", "type": "TEXT"}]
    with pytest.raises(ValidationError) as e:
        Table.parse_obj(table_dict)

    assert "grouping alias" in str(e)


def test_empty_object_pattern(table_dict):
    """
    Ensure an empty object pattern raises a validation error
    """

    table_dict["object_pattern"] = []
    with pytest.raises(ValidationError) as e:
        Table.parse_obj(table_dict)

    assert "object_pattern" in str(e)


def test_empty_primary_index(table_dict):
    """
    Ensure an empty primary index raises a validation error
    """
    table_dict["primary_index"] = []
    with pytest.raises(ValidationError) as e:
        Table.parse_obj(table_dict)

    assert "primary_index" in str(e)
