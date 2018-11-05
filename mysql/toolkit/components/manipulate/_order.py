def _order_unpack(order_by):
    """Unpack ascending/descending clause from order_by clause."""
    if isinstance(order_by, tuple):
        column, asc_desc = order_by
        return '{0} {1}'.format(column, asc_desc.upper())
    else:
        return order_by


def order_clause(statement, order):
    """
    Append an order by clauses to an existing SQL statement.

    If no column is specified in order param, original statement is returned.
    """
    if order:
        if isinstance(order, (list, set)):
            cols = ', '.join([_order_unpack(col) for col in order])
        else:
            cols = str(_order_unpack(order))

        statement += ' ORDER BY {0}'.format(cols)
    return statement
