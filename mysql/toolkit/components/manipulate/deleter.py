# Module named deleter.py instead of delete.py due to PyCharm file recognition issue
from mysql.toolkit.utils import wrap


class Delete:
    def delete_many(self, table, where_tuples=None, column=None, values=None):
        """
        Delete multiple rows from a table.

        :param table: Name of the table
        :param where_tuples: List of where tuples
        :param column: Column used for all where clauses
        :param values: List of values for column
        """
        # List of where_tuples was provided
        if where_tuples:
            for where in where_tuples:
                self.delete(table, where)

        # Single column and list of values was provided
        elif column and values:
            for val in values:
                self.delete(table, (column, val))

    def delete(self, table, where=None):
        """Delete existing rows from a table."""
        if where:
            where_statement = self._where_clause(where)
            query = "DELETE FROM {0} WHERE {1}".format(wrap(table), where_statement)
            self._printer('\tDeleted {0} row where {1}'.format(wrap(table), where_statement))
        else:
            query = 'DELETE FROM {0}'.format(wrap(table))
            self._printer('\tDeleted {0} rows'.format(wrap(table)))
        self.execute(query)
        return True
