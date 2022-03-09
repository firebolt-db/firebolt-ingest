from firebolt_ingest.main import main


def test_main():
    result = main()
    assert result is None
