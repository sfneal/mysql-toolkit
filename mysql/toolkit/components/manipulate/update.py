from mysql.toolkit.utils import get_col_val_str, wrap
from mysql.toolkit.components.manipulate._where import where_clause, null_convert


class Update:
    def update(self, table, columns, values, where=None):
        """
        Update the values of a particular row where a value is met.

        :param table: table name
        :param columns: column(s) to update
        :param values: updated values
        :param where: tuple, (where_column, where_value)
        """
        # TODO: Add ability to pass dictionary of (column: value) values
        # Create column string from list of values
        columns = [columns] if not isinstance(values, (list, tuple, set)) else columns
        values = [values] if not isinstance(values, (list, tuple, set)) else values
        cols = get_col_val_str(columns, query_type='update')

        # Concatenate statement
        if where:
            where_statement = where_clause(where)
            statement = "UPDATE {0} SET {1} {2}".format(wrap(table), cols, where_statement)

            # Execute statement
            self._cursor.execute(statement, values)
            for i in range(len(columns)):
                self._printer("\tUpdated table {0}, set column {1} to '{2}' {3}".format(wrap(table), columns[i],
                                                                                        values[i], where_statement))
        else:
            for i in range(len(columns)):
                statement = "UPDATE {0} SET {1} = {2}".format(wrap(table), columns[i], null_convert(values[i]))
                self.execute(statement)
                self._printer("\tUpdated table {0}, set column {1} to '{2}'".format(wrap(table), columns[i], values[i]))

    def update_many(self, table, columns, values, where_col, where_index):
        """
        Update the values of several rows.

        :param table: Name of the MySQL table
        :param columns: List of columns
        :param values: 2D list of rows
        :param where_col: Column name for where clause
        :param where_index: Row index of value to be used for where comparison
        :return:
        """
        # TODO: Fix this garbage parameter pattern
        for row in values:
            wi = row.pop(where_index)
            self.update(table, columns, row, (where_col, wi))
