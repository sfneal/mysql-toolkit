from mysql.toolkit.utils import wrap


class PrimaryKey:
    def get_primary_key_vals(self, table):
        """Retrieve a list of primary key values in a table"""
        return self.select(table, self.get_primary_key(table))

    def get_primary_key(self, table):
        """Retrieve the column which is the primary key for a table."""
        for column in self.get_schema(table):
            if len(column) > 3 and 'pri' in column[3].lower():
                return column[0]

    def set_primary_key(self, table, column):
        """Create a Primary Key constraint on a specific column when the table is already created."""
        self.execute('ALTER TABLE {0} ADD PRIMARY KEY ({1})'.format(table, column))
        self._printer('Added primary key to {0} on column {1}'.format(table, column))

    def drop_primary_key(self, table):
        """Drop a Primary Key constraint for a specific table."""
        self.execute('ALTER TABLE {0} DROP PRIMARY KEY'.format(table))


class ForeignKey:
    def set_foreign_key(self, parent_table, parent_column, child_table, child_column):
        """Create a Foreign Key constraint on a column from a table."""
        self.execute('ALTER TABLE {0} ADD FOREIGN KEY ({1}) REFERENCES {2}({3})'.format(parent_table, parent_column,
                                                                                        child_table, child_column))


class Structure(PrimaryKey, ForeignKey):
    """
    Result retrieval helper methods for the MySQL class.

    Capable of fetching list of available tables/databases, the primary for a table,
    primary key values for a table, number of rows in a table, number of rows of all
    tables in a database.
    """
    @property
    def tables(self):
        """Retrieve a list of tables in the connected database"""
        return self.fetch('show tables')

    @property
    def databases(self):
        """Retrieve a list of databases that are accessible under the current connection"""
        return self.fetch('show databases')

    def get_columns(self, table):
        """Retrieve a list of columns in a table."""
        return [schema[0] for schema in self.get_schema(table)]

    def get_schema(self, table, with_headers=False):
        """Retrieve the database schema for a particular table."""
        f = self.fetch('desc ' + wrap(table))
        if not isinstance(f[0], list):
            f = [f]

        # If with_headers is True, insert headers to first row before returning
        if with_headers:
            f.insert(0, ['Column', 'Type', 'Null', 'Key', 'Default', 'Extra'])
        return f

    def count_rows_all(self):
        """Get the number of rows for every table in the database."""
        return {table: self.count_rows(table) for table in self.tables}

    def count_rows(self, table):
        """Get the number of rows in a particular table"""
        return self.fetch('SELECT COUNT(*) FROM {0}'.format(wrap(table)))
