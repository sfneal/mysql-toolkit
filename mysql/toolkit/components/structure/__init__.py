from mysql.connector.errors import ProgrammingError
from mysql.toolkit.utils import join_cols, wrap
from mysql.toolkit.components.structure.definition import Definition
from mysql.toolkit.components.structure.keys import PrimaryKey, ForeignKey
from mysql.toolkit.components.structure.schema import Schema


class Alter(PrimaryKey, ForeignKey):
    def add_column(self, table, name='ID', data_type='int(11)', after_col=None, null=False, primary_key=False):
        """Add a column to an existing table."""
        location = 'AFTER {0}'.format(after_col) if after_col else 'FIRST'
        null_ = 'NULL' if null else 'NOT NULL'
        comment = "COMMENT 'Column auto created by mysql-toolkit'"
        pk = 'AUTO_INCREMENT PRIMARY KEY {0}'.format(comment) if primary_key else ''
        query = 'ALTER TABLE {0} ADD COLUMN {1} {2} {3} {4} {5}'.format(wrap(table), name, data_type, null_, pk, location)
        self.execute(query)
        self._printer("\tAdded column '{0}' to '{1}' {2}".format(name, table, '(Primary Key)' if primary_key else ''))
        return name

    def drop_column(self, table, name):
        """Remove a column to an existing table."""
        try:
            self.execute('ALTER TABLE {0} DROP COLUMN {1}'.format(wrap(table), name))
            self._printer('\tDropped column {0} from {1}'.format(name, table))
        except ProgrammingError:
            self._printer("\tCan't DROP '{0}'; check that column/key exists in '{1}'".format(name, table))
        return name

    def add_comment(self, table, column, comment):
        """Add a comment to an existing column in a table."""
        col_def = self.get_column_definition(table, column)
        query = "ALTER TABLE {0} MODIFY COLUMN {1} {2} COMMENT '{3}'".format(table, column, col_def, comment)
        self.execute(query)
        return True


class Structure(Alter, Definition, Schema):
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

    def get_unique_column(self, table):
        """Determine if any of the columns in a table contain exclusively unique values."""
        for col in self.get_columns(table):
            if self.count_rows_duplicates(table, col) == 0:
                return col

    def count_rows_duplicates(self, table, cols='*'):
        """Get the number of rows that do not contain distinct values."""
        return self.count_rows(table, '*') - self.count_rows_distinct(table, cols)

    def count_rows_all(self):
        """Get the number of rows for every table in the database."""
        return {table: self.count_rows(table) for table in self.tables}

    def count_rows(self, table, cols='*'):
        """Get the number of rows in a particular table."""
        query = 'SELECT COUNT({0}) FROM {1}'.format(join_cols(cols), wrap(table))
        result = self.fetch(query)
        return result if result is not None else 0

    def count_rows_all_distinct(self):
        """Get the number of distinct rows for every table in the database."""
        return {table: self.count_rows_distinct(table) for table in self.tables}

    def count_rows_distinct(self, table, cols='*'):
        """Get the number distinct of rows in a particular table."""
        return self.fetch('SELECT COUNT(DISTINCT {0}) FROM {1}'.format(join_cols(cols), wrap(table)))

    def get_duplicate_vals(self, table, column):
        """Retrieve duplicate values in a column of a table."""
        query = 'SELECT {0} FROM {1} GROUP BY {0} HAVING COUNT(*) > 1'.format(join_cols(column), wrap(table))
        return self.fetch(query)
