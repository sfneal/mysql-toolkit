def order_clause(statement, order):
    """
    Append an order by clauses to an existing SQL statement.

    If no column is specified in order param, original statement is returned.
    """
    if order:
        if isinstance(order, (list, set, tuple)):
            cols = ', '.join([col for col in order])
        elif isinstance(order, int):
            cols = str(order)
        else:
            cols = order

        statement += ' ORDER BY {0}'.format(cols)
    return statement
