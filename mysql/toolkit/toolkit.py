import mysql.connector
from differentiate import differentiate
from mysql.toolkit.execute import ExecuteScript
from mysql.toolkit.utils import get_col_val_str, join_cols, wrap
from mysql.toolkit.query import Query
from mysql.toolkit.results import Results


class Core:
    def __init__(self):
        pass

    def select(self, table, cols, _print=True):
        """Query only certain columns from a table and every row."""
        # Concatenate statement
        cols_str = join_cols(cols)
        statement = "SELECT " + cols_str + " FROM " + wrap(table)
        return self._fetch(statement, _print)

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

    def select_all(self, table):
        """Query all rows and columns from a table."""
        # Concatenate statement
        statement = "SELECT * FROM " + wrap(table)
        return self._fetch(statement)

    def select_all_join(self, table1, table2, key):
        """Left join all rows and columns from two tables where a common value is shared."""
        # TODO: Write function to run a select * left join query
        pass

    def insert(self, table, columns, values):
        """Insert a singular row into a table"""
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

    def drop_table(self, table):
        """Drop a table from a database."""
        self.execute('DROP TABLE ' + wrap(table))
        return table


class MySQL(Query, Core, Results):
    def __init__(self, config, enable_printing=True):
        """
        Connect to MySQL database and execute queries
        :param config: MySQL server configuration settings
        """
        # Initialize inherited classes
        Query.__init__(self, config, enable_printing)
        Core.__init__(self)
        Results.__init__(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print('\tMySQL disconnecting')
        try:
            self._commit()
            self._close()
        except mysql.connector.errors as e:
            print('\tError: ' + str(e))
            print('\tMySQL disconnected')

    def truncate_database(self):
        """Drop all tables in a database."""
        # Get list of tables
        tables = self.tables if isinstance(self.tables, list) else [self.tables]
        if len(tables) > 0:
            # Join list of tables into comma separated string
            tables_str = ', '.join([wrap(table) for table in tables])
            self.execute('DROP TABLE ' + tables_str)
            self._printer('\t' + str(len(tables)), 'tables truncated')
        return tables

    def get_schema(self, table, with_headers=False):
        """Retrieve the database schema for a particular table."""
        f = self._fetch('desc ' + wrap(table))

        # If with_headers is True, insert headers to first row before returning
        if with_headers:
            f.insert(0, ['Column', 'Type', 'Null', 'Key', 'Default', 'Extra'])
        return f


    def insert_uniques(self, table, columns, values):
        """
        Insert multiple rows into a table that do not already exist.

        If the rows primary key already exists, the rows values will be updated.
        If the rows primary key does not exists, a new row will be inserted
        """
        # Rows that exist in the table
        existing_rows = self.select(table, columns)

        # Rows that DO NOT exist in the table
        unique = differentiate(existing_rows, values)  # Get values that are not in existing_rows

        # Keys that exist in the table
        keys = self.get_primary_key_values(table)

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

    def update_many(self, table, columns, values, where_col, where_index):
        """Update the values of several rows."""
        for row in values:
            self.update(table, columns, row, (where_col, row[where_index]))

    # def create_table(self, table, data, headers=None):
    #     """Generate and execute a create table query by parsing a 2D dataset"""
    #     # TODO: Fix
    #     # Set headers list
    #     if not headers:
    #         headers = data[0]
    #
    #     # Create dictionary columns and data types from headers list
    #     data_types = {header: None for header in headers}
    #
    #     # Confirm that each row of the dataset is the same length
    #     for row in data:
    #         assert len(row) == len(headers)
    #
    #     # Create list of columns
    #     columns = [header + ' ' + data_type for header, data_type in data_types]
    #     self._printer(columns)
    #     statement = "create table " + table + " ("
    #     self._printer(statement)

    def drop_empty_tables(self):
        """Drop all empty tables in a database."""
        # Count number of rows in each table
        counts = self.count_rows_all()
        drops = []

        # Loop through each table key and validate that rows count is not 0
        for table, count in counts.items():
            if count < 1:
                # Drop table if it contains no rows
                self.drop_table(table)
                self._printer('Dropped table', table)
                drops.append(table)
        return drops

    def execute_script(self, sql_script, commands=None):
        """Wrapper method for ExecuteScript class."""
        ExecuteScript(self, sql_script, commands)
