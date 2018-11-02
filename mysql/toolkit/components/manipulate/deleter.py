# Module named deleter.py instead of delete.py due to PyCharm file recognition issue
from mysql.toolkit.utils import wrap


class Delete:
    def delete_many(self, table, wheres):
        """
        Delete multiple rows from a table.

        :param table: Name of the table
        :param wheres: List of where tuples
        """
        for where in wheres:
            self.delete(table, where)

    def delete(self, table, where=None):
        """Delete existing rows from a table."""
        if where:
            where_statement = self._where_clause(where)
            query = "DELETE FROM {0} WHERE {1}".format(wrap(table), where_statement)
        else:
            query = 'DELETE FROM {0}'.format(wrap(table))
        self.execute(query)
        return True
