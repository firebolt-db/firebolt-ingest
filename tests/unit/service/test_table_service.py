from firebolt_ingest.model.table import Column
from firebolt_ingest.service.table import generate_columns_string


def test_generate_columns_string():
    """
    test generate columns string with 0, 1 and multiple columns
    """
    assert generate_columns_string([]) == ""

    assert generate_columns_string([Column(name="id", type="TEXT")]) == "id TEXT"

    assert (
        generate_columns_string(
            [Column(name="id", type="TEXT"), Column(name="part", type="INT")]
        )
        == "id TEXT, part INT"
    )
