# Module named deleter.py instead of delete.py due to PyCharm file recognition issue
from mysql.toolkit.utils import wrap


class Delete:
    def delete(self, table, where=None):
        """Delete existing rows from a table."""
        if where:
            where_key, where_val = where
            query = "DELETE FROM {0} WHERE {1}='{2}'".format(wrap(table), where_key, where_val)
        else:
            query = 'DELETE FROM {0}'.format(wrap(table))
        self.execute(query)
        return True
