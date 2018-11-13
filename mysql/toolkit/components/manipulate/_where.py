from mysql.toolkit.utils import wrap
# TODO: Fix assertion to allow for 'is' and 'in'
SELECT_WHERE_OPERATORS = ('=', '<>', '<', '>', '!=', '<=', '>=', ' is ', ' in ')


def null_convert(value):
    """
    Determine if a where clauses's where_val is checking for 'NULL' or 'NOT NULL' values.

    :param value: Value from a where clause
    :return: Transformed where value if null check passes, else same as input
    """
    # not None, True
    if isinstance(value, bool) and value is True:
        return 'NOT NULL'

    # 'not null', 'NOT NULL', 'Not Null'
    elif isinstance(value, str) and value.lower() == 'not null':
        return 'NOT NULL'

    # False
    elif isinstance(value, bool) and value is False:
        return 'NULL'

    # None
    elif value is None:
        return 'NULL'

    # 'null', 'NULL', 'Null'
    elif isinstance(value, str) and value.lower() == 'null':
        return 'NULL'

    # List or tuple of values
    elif isinstance(value, (list, tuple)):
        return value

    # Set or tuple of values
    elif isinstance(value, set):
        return list(value)

    # Add quotation wrapper around value
    else:
        return "'{0}'".format(value)


def _where_clause(where, multi=False):
    """
    Unpack a where clause tuple and concatenate a MySQL WHERE statement.

    Set multi to true if concatenating multiple where clauses.  Returns statement without
    'WHERE' prefix.

    :param where: 2 or 3 part tuple containing a where_column and a where_value (optional operator)
    :param multi: Bool, set to True if concatenating multiple where clauses
    :return: WHERE clause statement
    """
    assert isinstance(where, tuple)
    # Three part tuple (column, operator, value)
    if len(where) == 3:
        where_col, operator, where_val = where

    # Two part tuple (column, value)
    else:
        where_col, where_val = where
        operator = '='

    # Tuple with (table_name, column_name) values
    if isinstance(where_col, tuple):
        tbl, col = where_col
        where_col = "{0}.{1}".format(wrap(tbl), col)

    # Check if where_val is signifying 'NULL' or 'NOT NULL'
    where_val = null_convert(where_val)
    if where_val in ('NULL', 'NOT NULL'):
        operator = ' is '

    # Check if where_val is an iterable, signfying IN clause
    elif isinstance(where_val, (list, tuple, set)):
        # Confirm that where_val contains more than one item
        if len(where_val) > 1:
            operator = ' in '
            where_val = str(tuple(where_val))

        # where_val has one item, use '=' operator
        else:
            operator = '='
            where_val = null_convert(where_val[0])

    # Validate operator
    operator = ' {0} '.format(operator) if operator in ('in', 'is') else operator
    assert operator in SELECT_WHERE_OPERATORS

    # Concatenate WHERE clause (ex: **first_name='John'**)
    if multi:
        return "{0}{1}{2}".format(where_col, operator.upper(), where_val)
    else:
        return "WHERE {0}{1}{2}".format(where_col, operator.upper(), where_val)


def where_clause(where):
    """Wrapper function that handles iterables and calls _where_clause."""
    if isinstance(where, (list, set)):
        # Multiple WHERE clause's (separate with AND)
        clauses = [_where_clause(clause, multi=True) for clause in where]
        return 'WHERE {0}'.format(' AND '.join(clauses))
    else:
        return _where_clause(where)


def where_clause_append(statement, where=None):
    """
    Append an WHERE BY clause(s) to an existing SQL statement.

    If no column is specified in where param, original statement is returned.
    """
    if where:
        statement += ' ' + where_clause(where)
    return statement
