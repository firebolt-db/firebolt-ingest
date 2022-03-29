from typing import List

from firebolt.db import Cursor

from firebolt_ingest.aws_settings import AWSSettings
from firebolt_ingest.model.table import Column, Table
from firebolt_ingest.service.table import TableService


def check_columns(cursor: Cursor, table_name: str, columns: List[Column]):
    cursor.execute(
        "SELECT column_name, data_type "
        "FROM INFORMATION_SCHEMA.columns "
        "WHERE table_name = ?",
        [table_name],
    )
    data = cursor.fetchall()
    columns_plain = [[column.name, column.type] for column in columns]

    assert sorted(data, key=lambda x: x[0]) == sorted(
        columns_plain, key=lambda x: x[0]
    ), "Expected columns and columns from table don't match"


def test_create_external_table(connection):
    """create external table from the getting started example"""

    s3_url = "s3://firebolt-publishing-public/samples/tpc-h/parquet/lineitem/"

    ts = TableService(connection)
    table_name = "ex_lineitem_ingest_integration"
    columns = [
        Column(name="l_orderkey", type="LONG"),
        Column(name="l_partkey", type="LONG"),
    ]

    ts.create_external_table(
        table=Table(
            table_name=table_name,
            columns=columns,
            file_type="PARQUET",
            object_pattern=["*.parquet"],
        ),
        aws_settings=AWSSettings(s3_url=s3_url),
    )

    cursor = connection.cursor()
    check_columns(cursor, table_name, columns)

    cursor.execute(f"DROP TABLE {table_name }")
