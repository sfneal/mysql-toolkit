from mysql.toolkit.utils import join_cols, wrap
from mysql.connector.errors import ProgrammingError


class PrimaryKey:
    def get_primary_key_vals(self, table):
        """Retrieve a list of primary key values in a table."""
        return self.select(table, self.get_primary_key(table))

    def get_primary_key(self, table):
        """Retrieve the column which is the primary key for a table."""
        for column in self.get_schema(table):
            if len(column) > 3 and 'pri' in column[3].lower():
                return column[0]

    def set_primary_key(self, table, column):
        """Create a Primary Key constraint on a specific column when the table is already created."""
        self.execute('ALTER TABLE {0} ADD PRIMARY KEY ({1})'.format(wrap(table), column))
        self._printer('\tAdded primary key to {0} on column {1}'.format(wrap(table), column))

    def set_primary_keys_all(self, tables=None):
        """
        Create primary keys for every table in the connected database.

        Checks that each table has a primary key.  If a table does not have a key
        then each column is analyzed to determine if it contains only unique values.
        If no columns exist containing only unique values then a new 'ID' column
        is created to serve as a auto_incrementing primary key.
        """
        tables = tables if tables else self.tables
        for t in tables:
            # Confirm no primary key exists
            if not self.get_primary_key(t):
                # Determine if there is a unique column that can become the PK
                unique_col = self.get_unique_column(t)

                # Set primary key
                if unique_col:
                    self.set_primary_key(t, unique_col)

                # Create unique 'ID' column
                else:
                    self.add_column(t, primary_key=True)

    def drop_primary_key(self, table):
        """Drop a Primary Key constraint for a specific table."""
        if self.get_primary_key(table):
            self.execute('ALTER TABLE {0} DROP PRIMARY KEY'.format(wrap(table)))


class ForeignKey:
    def set_foreign_key(self, parent_table, parent_column, child_table, child_column):
        """Create a Foreign Key constraint on a column from a table."""
        self.execute('ALTER TABLE {0} ADD FOREIGN KEY ({1}) REFERENCES {2}({3})'.format(parent_table, parent_column,
                                                                                        child_table, child_column))


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


class Definition:
    def get_table_definition(self, table):
        """Retrieve a CREATE TABLE statement for an existing table."""
        return self.fetch('SHOW CREATE TABLE {0}'.format(table))[1]

    def get_column_definition_all(self, table):
        """Retrieve the column definition statement for a column from a table."""
        # Get complete table definition
        col_defs = self.get_table_definition(table).split('\n')

        # Return only column definitions
        return [i.strip() for i in col_defs if i.strip().startswith('`')]

    def get_column_definition(self, table, column):
        """Retrieve the column definition statement for a column from a table."""
        # Parse column definitions for match
        for col in self.get_column_definition_all(table):
            if col.strip('`').startswith(column):
                return col.strip(',')


class Schema:
    def show_schema(self, tables=None):
        """Print schema information."""
        tables = tables if tables else self.tables
        for t in tables:
            self._printer('\t{0}'.format(t))
            for col in self.get_schema(t, True):
                self._printer('\t\t{0:30} {1:15} {2:10} {3:10} {4:10} {5:10}'.format(*col))

    def get_columns(self, table):
        """Retrieve a list of columns in a table."""
        return [schema[0] for schema in self.get_schema(table)]

    def get_schema_dict(self, table):
        """
        Retrieve the database schema in key, value pairs for easier
        references and comparisons.
        """
        # Retrieve schema in list form
        schema = self.get_schema(table, with_headers=True)

        # Pop headers from first item in list
        headers = schema.pop(0)

        # Create dictionary by zipping headers with each row
        return {values[0]: dict(zip(headers, values[0:])) for values in schema}

    def get_schema(self, table, with_headers=False):
        """Retrieve the database schema for a particular table."""
        f = self.fetch('desc ' + wrap(table))
        if not isinstance(f[0], list):
            f = [f]

        # Replace None with ''
        schema = [['' if col is None else col for col in row] for row in f]

        # If with_headers is True, insert headers to first row before returning
        if with_headers:
            schema.insert(0, ['Column', 'Type', 'Null', 'Key', 'Default', 'Extra'])
        return schema


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
