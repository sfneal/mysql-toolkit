from mysql.toolkit.utils import wrap


class Results:
    """
    Result retrieval helper methods for the MySQL class.

    Capable of fetching list of available tables/databases, the primary for a table,
    primary key values for a table, number of rows in a table, number of rows of all
    tables in a database.
    """
    def __init__(self):
        pass

    @property
    def tables(self):
        """Retrieve a list of tables in the connected database"""
        statement = 'show tables'
        return self._fetch(statement)

    @property
    def databases(self):
        """Retrieve a list of databases that are accessible under the current connection"""
        return self._fetch('show databases')

    def get_primary_key(self, table):
        """Retrieve the column which is the primary key for a table."""
        for column in self.get_schema(table):
            if 'pri' in column[3].lower():
                return column[0]

    def get_primary_key_vals(self, table):
        """Retrieve a list of primary key values in a table"""
        return self.select(table, self.get_primary_key(table), _print=False)

    def get_schema(self, table, with_headers=False):
        """Retrieve the database schema for a particular table."""
        f = self._fetch('desc ' + wrap(table))

        # If with_headers is True, insert headers to first row before returning
        if with_headers:
            f.insert(0, ['Column', 'Type', 'Null', 'Key', 'Default', 'Extra'])
        return f

    def count_rows(self, table):
        """Get the number of rows in a particular table"""
        return self.select(table, 'COUNT(*)', False)

    def count_rows_all(self):
        """Get the number of rows for every table in the database."""
        return {table: self.count_rows(table) for table in self.tables}
