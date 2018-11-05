SELECT_WHERE_OPERATORS = ('=', '<>', '<', '>', '!=', '<=', '>=', ' is ')


def null_convert(where_val):
    """
    Determine if a where clauses's where_val is checking for 'NULL' or 'NOT NULL' values.

    :param where_val: Value from a where clause
    :return: Transformed where value if null check passes, else same as input
    """
    # not None, True
    if isinstance(where_val, bool) and where_val is True:
        return 'NOT NULL'

    # 'not null', 'NOT NULL', 'Not Null'
    elif isinstance(where_val, str) and where_val.lower() == 'not null':
        return 'NOT NULL'

    # False
    elif isinstance(where_val, bool) and where_val is False:
        return 'NULL'

    # None
    elif where_val is None:
        return 'NULL'

    # 'null', 'NULL', 'Null'
    elif isinstance(where_val, str) and where_val.lower() == 'null':
        return 'NULL'

    # Add quotation wrapper around value
    else:
        return "'{0}'".format(where_val)


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
        # Unpack tuple and verify operator
        where_col, operator, where_val = where

    # Two part tuple (column, value)
    else:
        # Unpack tuple
        where_col, where_val = where
        operator = '='

    # Check if where_val is signifying 'NULL' or 'NOT NULL'
    where_val = null_convert(where_val)
    if where_val in ('NULL', 'NOT NULL'):
        operator = ' is '

    # Validate operator
    assert operator in SELECT_WHERE_OPERATORS

    # Concatenate WHERE clause (ex: **first_name='John'**)
    if multi:
        return "{0}{1}{2}".format(where_col, operator, where_val)
    else:
        return "WHERE {0}{1}{2}".format(where_col, operator, where_val)


def where_clause(where):
    """Wrapper function that handles iterables and calls _where_clause."""
    if isinstance(where, (list, set)):
        # Multiple WHERE clause's (separate with AND)
        clauses = [_where_clause(clause, multi=True) for clause in where]
        return 'WHERE {0}'.format(' AND '.join(clauses))
    else:
        return _where_clause(where)
