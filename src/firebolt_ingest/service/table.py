from typing import List

from firebolt_ingest.model.table import Column


def generate_columns_string(columns: List[Column]) -> str:
    """
    Function generates a prepared string from list of columns to
    be used in creation of external or internal table

    Args:
        columns: the list of columns

    Returns:
        a prepared string
    """

    return ", ".join([f"{column.name} {column.type}" for column in columns])
