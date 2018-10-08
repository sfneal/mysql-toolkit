from mysql.toolkit.utils import get_col_val_str, join_cols, wrap


class Core:
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

    def truncate(self, table):
        """Empty a table by deleting all of its rows."""
        statement = "TRUNCATE " + wrap(table)
        self.execute(statement)
        self._printer('\tMySQL table ' + str(table) + ' successfully truncated')

    def drop(self, table):
        """Drop a table from a database."""
        self.execute('DROP TABLE ' + wrap(table))
        return table
