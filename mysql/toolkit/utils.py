def get_col_val_str(columns, query_type='insert'):
    cols = ""
    vals = ""
    if query_type == 'insert':
        for index, column in enumerate(columns):
            cols = cols + column + ', '
            vals = vals + '{' + str(index) + '}' + ', '

        # Remove last comma and space
        cols = cols[:-2]
        vals = vals[:-2]
        return cols, vals
    if query_type == 'update':
        for column in columns:
            cols = str(cols + column + '=%s, ')

        # Remove last comma and space
        cols = cols[:-2]
        return cols


def join_cols(cols):
    """Join list of columns into a string for a SQL query"""
    return ", ".join([i for i in cols]) if isinstance(cols, list) else cols


def wrap(item):
    """Wrap a string with `` characters for SQL queries."""
    return '`' + str(item) + '`'
