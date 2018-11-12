ORDER_CLAUSES = ('asc', 'desc')


def _format_order_clause(column_order):
    """Unpack (column, asc_desc) tuple and validate formatting."""
    column, asc_desc = column_order
    assert asc_desc.lower() in ORDER_CLAUSES, "Invalid ORDER BY clause, must be either 'ASC' or 'DESC'"
    return '{0} {1}'.format(column, asc_desc.upper())


def _order_unpack(order_by):
    """
    Unpack ascending/descending clause from order_by clause.

    Try and unpack a (column, asc_desc) tuple.  Then try and find ' asc' or ' desc' in order_by,
    checking for leading whitespace to avoid false matches for column names containing a 'asc'
    or 'desc' string.  If first two checks do not pass, order_by is returned as inputted.
    :param order_by: String or Tuple containing a column name and optional ascending or descending
    :return: Formatted ORDER BY clause
    """
    # Unpack (column, asc_desc) tuple
    if isinstance(order_by, tuple) and len(order_by) == 2:
        return _format_order_clause(order_by)

    # Found asc_desc string with leading whitespace
    elif isinstance(order_by, str) and any(' {0}'.format(clause) in order_by for clause in ORDER_CLAUSES):
        # Split order by string once
        return _format_order_clause(tuple(order_by.split(' ', 1)))

    # Return original input
    else:
        return order_by


def order_clause(order):
    """Generate an ORDER BY clause for a MySQL statement."""
    if isinstance(order, (list, set)):
        cols = ', '.join([_order_unpack(col) for col in order])
    else:
        cols = str(_order_unpack(order))
    return 'ORDER BY {0}'.format(cols)


def order_clause_append(statement, order=None):
    """
    Append an order by clauses to an existing SQL statement.

    If no column is specified in order param, original statement is returned.
    """
    if order:
        statement += ' ' + order_clause(order)
    return statement
