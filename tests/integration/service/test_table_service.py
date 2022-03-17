from firebolt_ingest.aws_settings import AWSSettings
from firebolt_ingest.model.table import Column, Table
from firebolt_ingest.service.table import TableService


def test_create_external_table(connection):
    """create external table from the getting started example"""

    s3_url = "s3://firebolt-publishing-public/samples/tpc-h/parquet/lineitem/"

    ts = TableService(connection, AWSSettings(s3_url=s3_url))

    ts.create_external_table(
        table=Table(
            database_name="db_name",
            table_name="ex_lineitem_ingest_integration",
            columns=[
                Column(name="l_orderkey", type="LONG"),
                Column(name="l_partkey", type="LONG"),
            ],
            type="PARQUET",
            object_pattern="*.parquet",
        )
    )
