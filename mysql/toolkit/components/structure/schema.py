from mysql.connector.errors import ProgrammingError
from mysql.toolkit.utils import wrap


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

    def add_column(self, table, name='ID', data_type='int(11)', after_col=None, null=False, primary_key=False):
        """Add a column to an existing table."""
        location = 'AFTER {0}'.format(after_col) if after_col else 'FIRST'
        null_ = 'NULL' if null else 'NOT NULL'
        comment = "COMMENT 'Column auto created by mysql-toolkit'"
        pk = 'AUTO_INCREMENT PRIMARY KEY {0}'.format(comment) if primary_key else ''
        query = 'ALTER TABLE {0} ADD COLUMN {1} {2} {3} {4} {5}'.format(wrap(table), name, data_type, null_, pk,
                                                                        location)
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

    def add_auto_increment(self, table, name):
        """Modify an existing column."""
        # Get current column definition and add auto_incrementing
        definition = self.get_column_definition(table, name) + ' AUTO_INCREMENT'

        # Concatenate and execute modify statement
        self.execute("ALTER TABLE {0} MODIFY {1}".format(table, definition))
        return True

    def modify_column(self, table, name, data_type=None, after_col=None, null=None, primary_key=None):
        """Modify an existing column."""
        existing_def = self.get_schema_dict(table)[name]
        col_names = self.get_columns(table)
        column_index = col_names.index(name)

        # Set data type
        if not data_type:
            data_type = data_type

        # Set after column
        if not after_col:
            after_col = col_names[column_index - 1]
        location = 'AFTER {0}'.format(after_col) if after_col else 'FIRST'

        # Set NULL
        if not null:
            null_ = 'NULL' if existing_def['Null'].lower() == 'yes' else 'NOT NULL'
        else:
            null_ = 'NULL' if null else 'NOT NULL'

        comment = "COMMENT 'Column auto created by mysql-toolkit'"
        if not primary_key:
            primary_key = True if existing_def['Key'] else False
        pk = 'AUTO_INCREMENT PRIMARY KEY {0}'.format(comment) if primary_key else ''
        query = 'ALTER TABLE {0} MODIFY COLUMN {1} {2} {3} {4} {5}'.format(wrap(table), name, data_type, null_, pk,
                                                                           location)
        print(query)
        self.execute(query)
