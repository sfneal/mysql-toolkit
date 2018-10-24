from mysql.toolkit.utils import wrap
from mysql.toolkit.components.manipulate.select import Select
from mysql.toolkit.components.manipulate.update import Update
from mysql.toolkit.components.manipulate.insert import Insert


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


class Manipulate(Select, Insert, Update, Delete):
    def __init__(self):
        pass
