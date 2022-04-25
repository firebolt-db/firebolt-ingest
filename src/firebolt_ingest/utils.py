from sqlparse import format  # type: ignore


def format_query(query: str) -> str:
    """
    function, that reformats the query using sqlparse.s
    """
    return format(query, reindent=True, indent_width=4)
