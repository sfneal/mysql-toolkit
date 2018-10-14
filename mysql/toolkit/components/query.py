from differentiate import diff
from mysql.toolkit.utils import get_col_val_str, join_cols, wrap


class Query:
    def __init__(self):
        pass

    def select(self, table, cols, _print=True):
        """Query every row and only certain columns from a table."""
        # Concatenate statement
        cols_str = join_cols(cols)
        statement = "SELECT " + cols_str + " FROM " + wrap(table)
        return self._fetch(statement, _print)

    def select_all(self, table):
        """Query all rows and columns from a table."""
        # Concatenate statement
        statement = "SELECT * FROM " + wrap(table)
        return self._fetch(statement)

    def select_all_join(self, table1, table2, key):
        """Left join all rows and columns from two tables where a common value is shared."""
        # TODO: Write function to run a select * left join query
        pass

    def select_where(self, table, cols, where):
        """Query certain columns from a table where a particular value is found."""
        # Either join list of columns into string or set columns to * (all)
        if isinstance(cols, list):
            cols_str = join_cols(cols)
        else:
            cols_str = "*"

        # Unpack WHERE clause dictionary into tuple
        where_col, where_val = where

        statement = ("SELECT " + cols_str + " FROM " + wrap(table) + ' WHERE ' + str(where_col) + '=' + str(where_val))
        self._fetch(statement)

    def insert(self, table, columns, values):
        """Insert a single row into a table."""
        # Concatenate statement
        cols, vals = get_col_val_str(columns)
        statement = "INSERT INTO " + wrap(table) + "(" + cols + ") " + "VALUES (" + vals + ")"

        # Execute statement
        self.execute(statement, values)
        self._printer('\tMySQL row successfully inserted')

    def insert_many(self, table, columns, values):
        """
        Insert multiple rows into a table.

        If only one row is found, self.insert method will be used.
        """
        # Make values a list of lists if it is a flat list
        if not isinstance(values[0], list):
            values = [[v] for v in values]

        # Use self.insert if only one row is being inserted
        if len(values) < 2:
            self.insert(table, columns, values[0])
        else:
            # Concatenate statement
            cols, vals = get_col_val_str(columns)
            statement = "INSERT INTO " + wrap(table) + "(" + cols + ") " + "VALUES (" + vals + ")"

            # Execute statement
            self._cursor.executemany(statement, values)
            self._printer('\tMySQL rows (' + str(len(values)) + ') successfully INSERTED')

    def update(self, table, columns, values, where):
        """
        Update the values of a particular row where a value is met.

        :param table: table name
        :param columns: column(s) to update
        :param values: updated values
        :param where: tuple, (where_column, where_value)
        """
        # Unpack WHERE clause dictionary into tuple
        where_col, where_val = where

        # Create column string from list of values
        cols = get_col_val_str(columns, query_type='update')

        # Concatenate statement
        statement = "UPDATE " + str(table) + " SET " + str(cols) + ' WHERE ' + str(where_col) + '=' + str(where_val)

        # Execute statement
        self._cursor.execute(statement, values)
        self._printer('\tMySQL cols (' + str(len(values)) + ') successfully UPDATED')

    def insert_uniques(self, table, columns, values):
        """
        Insert multiple rows into a table that do not already exist.

        If the rows primary key already exists, the rows values will be updated.
        If the rows primary key does not exists, a new row will be inserted
        """
        # Rows that exist in the table
        existing_rows = self.select(table, columns)

        # Rows that DO NOT exist in the table
        unique = diff(existing_rows, values)  # Get values that are not in existing_rows

        # Keys that exist in the table
        keys = self.get_primary_key_vals(table)

        # Primary key's column index
        pk_col = self.get_primary_key(table)
        pk_index = columns.index(pk_col)

        # Split list of unique rows into list of rows to update and rows to insert
        to_insert, to_update = [], []
        for index, row in enumerate(unique):
            # Primary key is not in list of pk values, insert new row
            if row[pk_index] not in keys:
                to_insert.append(unique[index])

            # Primary key exists, update row rather than insert
            elif row[pk_index] in keys:
                to_update.append(unique[index])

        # Insert new rows
        if len(to_insert) > 0:
            self.insert_many(table, columns, to_insert)

        # Update existing rows
        if len(to_update) > 0:
            self.update_many(table, columns, to_update, pk_col, 0)

        # No inserted or updated rows
        if len(to_insert) < 1 and len(to_update) < 0:
            self._printer('No rows added to', table)

    def update_many(self, table, columns, values, where_col, where_index):
        """Update the values of several rows."""
        for row in values:
            self.update(table, columns, row, (where_col, row[where_index]))
