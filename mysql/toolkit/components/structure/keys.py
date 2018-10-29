from mysql.toolkit.utils import wrap


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

    def set_primary_keys_auto(self, tables=None):
        """
        Create primary keys for every table in the connected database.

        Checks that each table has a primary key.  If a table does not have a key
        then each column is analyzed to determine if it contains only unique values.
        If no columns exist containing only unique values then a new 'ID' column
        is created to serve as a auto_incrementing primary key.
        """
        # Retrieve list of tables if not provided
        tables = tables if tables else self.tables

        # Resolve primary keys and return list of table, primary_key tuples
        return [(table, self.set_primary_key_auto(table)) for table in tables]

    def set_primary_key_auto(self, table):
        """
        Analysis a table and set a primary key.

        Determine primary key by identifying a column with unique values
        or creating a new column.

        :param table: Table to alter
        :return: Primary Key column
        """
        # Confirm no primary key exists
        pk = self.get_primary_key(table)
        if not pk:
            # Determine if there is a unique column that can become the PK
            unique_col = self.get_unique_column(table)

            # Set primary key
            if unique_col:
                self.set_primary_key(table, unique_col)

            # Create unique 'ID' column
            else:
                unique_col = self.add_column(table, primary_key=True)
            return unique_col
        else:
            return pk

    def drop_primary_key(self, table):
        """Drop a Primary Key constraint for a specific table."""
        if self.get_primary_key(table):
            self.execute('ALTER TABLE {0} DROP PRIMARY KEY'.format(wrap(table)))


class ForeignKey:
    def set_foreign_key(self, parent_table, parent_column, child_table, child_column):
        """Create a Foreign Key constraint on a column from a table."""
        self.execute('ALTER TABLE {0} ADD FOREIGN KEY ({1}) REFERENCES {2}({3})'.format(parent_table, parent_column,
                                                                                        child_table, child_column))
