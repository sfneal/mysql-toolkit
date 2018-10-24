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
