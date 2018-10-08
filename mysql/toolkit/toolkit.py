import os
from time import time
from datetime import datetime
from tqdm import tqdm
from differentiate import differentiate
import mysql.connector
from mysql.connector import errorcode


def get_column_value_strings(columns, query_type='insert'):
    cols = ""
    vals = ""
    if query_type == 'insert':
        for c in columns:
            cols = cols + c + ', '
            vals = vals + '%s' + ', '

        # Remove last comma and space
        cols = cols[:-2]
        vals = vals[:-2]
        return cols, vals
    if query_type == 'update':
        for c in columns:
            cols = str(cols + c + '=%s, ')

        # Remove last comma and space
        cols = cols[:-2]
        return cols


def join_columns(cols):
    """Join list of columns into a string for a SQL query"""
    return ", ".join([i for i in cols]) if isinstance(cols, list) else cols


def wrap(item):
    """Wrap a string with `` characters for SQL queries."""
    return '`' + str(item) + '`'


class MySQL:
    def __init__(self, config, enable_printing=True):
        """
        Connect to MySQL database and execute queries
        :param config: MySQL server configuration settings
        """
        self.enable_printing = enable_printing
        self._cursor = None
        self._cnx = None
        self._connect(config)

    def __enter__(self):
        print('\tMySQL connecting')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print('\tMySQL disconnecting')
        try:
            self._commit()
            self._close()
        except mysql.connector.errors as e:
            print('\tError: ' + str(e))
            print('\tMySQL disconnected')

    # ------------------------------------------------------------------------------
    # |                            HELPER METHODS                                  |
    # ------------------------------------------------------------------------------
    def _connect(self, config):
        """Establish a connection with a MySQL database."""
        try:
            self._cnx = mysql.connector.connect(**config)
            self._cursor = self._cnx.cursor()
            self._printer('\tMySQL DB connection established')
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            raise err

    def _printer(self, *msg):
        """Printing method for internal use."""
        if self.enable_printing:
            print(*msg)

    def _close(self):
        """Close MySQL database connection."""
        self._cursor.close()
        self._cnx.close()

    def _commit(self):
        """Commit the changes made during the current connection."""
        self._cnx.commit()

    def _fetch(self, statement, _print=False):
        """Execute a SQL query and return values."""
        # Execute statement
        self._cursor.execute(statement)
        rows = []
        for row in self._cursor:
            if len(row) == 1:
                rows.append(row[0])
            else:
                rows.append(list(row))
        if _print:
            self._printer('\tMySQL rows successfully queried', len(rows))

        # Return a single item if the list only has one item
        return rows[0] if len(rows) == 1 else rows

    def execute(self, command):
        self._cursor.execute(command)
        self._commit()

    def executemany(self, command):
        self._cursor.executemany(command)
        self._commit()
    # ------------------------------------------------------------------------------
    # |                            END HELPER METHODS                              |
    # ------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------
    #                                GETTER METHODS                                |
    # ------------------------------------------------------------------------------
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

    def get_primary_key_values(self, table):
        """Retrieve a list of primary key values in a table"""
        return self.select(table, self.get_primary_key(table), _print=False)

    def count_rows(self, table):
        """Get the number of rows in a particular table"""
        return self.select(table, 'COUNT(*)', False)

    def count_rows_all(self):
        """Get the number of rows for every table in the database."""
        return {table: self.count_rows(table) for table in self.tables}
    # ------------------------------------------------------------------------------
    #                                END GETTER METHODS                            |
    # ------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------
    # |                 METHODS THAT CONCATENATE SQL QUERIES                       |
    # ------------------------------------------------------------------------------
    def select(self, table, cols, _print=True):
        """Query only certain columns from a table and every row."""
        # Concatenate statement
        cols_str = join_columns(cols)
        statement = "SELECT " + cols_str + " FROM " + wrap(table)
        return self._fetch(statement, _print)

    def select_where(self, table, cols, where):
        """Query certain columns from a table where a particular value is found."""
        # Either join list of columns into string or set columns to * (all)
        if isinstance(cols, list):
            cols_str = join_columns(cols)
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
        cols, vals = get_column_value_strings(columns)
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
            cols, vals = get_column_value_strings(columns)
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
        cols = get_column_value_strings(columns, query_type='update')

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

    def drop_table(self, table):
        """Drop a table from a database."""
        self.execute('DROP TABLE ' + wrap(table))
        return table

    def get_schema(self, table, with_headers=False):
        """Retrieve the database schema for a particular table."""
        f = self._fetch('desc ' + wrap(table))

        # If with_headers is True, insert headers to first row before returning
        if with_headers:
            f.insert(0, ['Column', 'Type', 'Null', 'Key', 'Default', 'Extra'])
        return f
    # ------------------------------------------------------------------------------
    # |                 END METHODS THAT CONCATENATE SQL QUERIES                   |
    # ------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------
    # |                 METHODS THAT UTILIZE CONCAT METHODS                        |
    # ------------------------------------------------------------------------------
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
    # ------------------------------------------------------------------------------
    # |                 END METHODS THAT UTILIZE CONCAT METHODS                    |
    # ------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------
    #                               STANDALONE METHODS                             |
    # ------------------------------------------------------------------------------
    def execute_script(self, sql_script, commands=None):
        """Wrapper method for ExecuteScript class."""
        ExecuteScript(self, sql_script, commands)
    # ------------------------------------------------------------------------------
    #                             END STANDALONE METHODS                           |
    # ------------------------------------------------------------------------------


class ExecuteScript:
    def __init__(self, mysql_instance, sql_script, commands=None):
        """Execute a sql file one command at a time."""
        # Pass MySQL instance from execute_script method to ExecuteScript class
        self.MySQL = mysql_instance

        # SQL script to be executed
        self.sql_script = sql_script

        # Retrieve commands from sql_script if no commands are provided
        self.commands = self._get_commands(sql_script) if not commands else commands

        # Save failed commands to list
        self.fail = []
        self.success = 0

        # Execute commands
        self.execute_commands()

        # Dump failed commands to text file
        if len(self.fail) > 1:
            self.dump_fails()

    @staticmethod
    def _get_commands(sql_script):
        print('\tRetrieving commands')
        # Open and read the file as a single buffer
        with open(sql_script, 'r') as fd:
            sql_file = fd.read()

        # all SQL commands (split on ';')
        # remove dbo. prefixes from table names
        return [com.replace("dbo.", '') for com in split_sql_commands(sql_file)]

    def execute_commands(self):
        # Execute every command from the input file
        print('\t' + str(len(self.commands)), 'commands')
        for command in tqdm(self.commands, total=len(self.commands), desc='Executing SQL Commands'):
            # This will skip and report errors
            # For example, if the tables do not yet exist, this will skip over
            # the DROP TABLE commands
            try:
                self.MySQL.execute(command)
                self.success += 1
            except:
                self.fail.append(command)

        # Write fail commands to a text file
        print('\t' + str(self.success), 'successful commands')

    def dump_fails(self):
        # Re-add semi-colon separator
        fails = [com + ';\n' for com in self.fail]
        print('\t' + str(len(fails)), 'failed commands')

        # Create a directory to save fail SQL scripts
        fails_dir = os.path.join(os.path.dirname(self.sql_script), 'fails')
        if not os.path.exists(fails_dir):
            os.mkdir(fails_dir)
        fails_dir = os.path.join(fails_dir, datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H-%M-%S'))
        if not os.path.exists(fails_dir):
            os.mkdir(fails_dir)
        print('\tDumping failed commands to', fails_dir)

        # Dump failed commands to text file in the same directory as the script
        for count, fail in tqdm(enumerate(fails), total=len(fails), desc='Dumping failed SQL commands to text'):
            fails_fname = str(os.path.basename(self.sql_script).rsplit('.')[0]) + str(count) + '.sql'
            txt_file = os.path.join(fails_dir, fails_fname)

            # Dump to text file
            with open(txt_file, 'w') as txt:
                txt.writelines(fail)


def split_sql_commands(text):
    results = []
    current = ''
    state = None
    for c in tqdm(text, total=len(text), desc='Parsing SQL script file', unit='chars'):
        if state is None:  # default state, outside of special entity
            current += c
            if c in '"\'':
                # quoted string
                state = c
            elif c == '-':
                # probably "--" comment
                state = '-'
            elif c == '/':
                # probably '/*' comment
                state = '/'
            elif c == ';':
                # remove it from the statement
                current = current[:-1].strip()
                # and save current stmt unless empty
                if current:
                    results.append(current)
                current = ''
        elif state == '-':
            if c != '-':
                # not a comment
                state = None
                current += c
                continue
            # remove first minus
            current = current[:-1]
            # comment until end of line
            state = '--'
        elif state == '--':
            if c == '\n':
                # end of comment
                # and we do include this newline
                current += c
                state = None
            # else just ignore
        elif state == '/':
            if c != '*':
                state = None
                current += c
                continue
            # remove starting slash
            current = current[:-1]
            # multiline comment
            state = '/*'
        elif state == '/*':
            if c == '*':
                # probably end of comment
                state = '/**'
        elif state == '/**':
            if c == '/':
                state = None
            else:
                # not an end
                state = '/*'
        elif state[0] in '"\'':
            current += c
            if state.endswith('\\'):
                # prev was backslash, don't check for ender
                # just revert to regular state
                state = state[0]
                continue
            elif c == '\\':
                # don't check next char
                state += '\\'
                continue
            elif c == state[0]:
                # end of quoted string
                state = None
        else:
            raise Exception('Illegal state %s' % state)

    if current:
        current = current.rstrip(';').strip()
        if current:
            results.append(current)

    return results
