from differentiate import diff
from mysql.toolkit.utils import wrap
from mysql.toolkit.script.script import SQLScript


class Advanced:
    def __init__(self):
        pass

    def create_table(self, table, data, headers=None):
        """Generate and execute a create table query by parsing a 2D dataset"""
        # TODO: Finish writing method
        pass
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

    def backup_database(self, structure=True, data=True):
        # TODO: Create method
        pass

    def drop_empty_tables(self):
        """Drop all empty tables in a database."""
        # Count number of rows in each table
        counts = self.count_rows_all()
        drops = []

        # Loop through each table key and validate that rows count is not 0
        for table, count in counts.items():
            if count < 1:
                # Drop table if it contains no rows
                self.drop(table)
                self._printer('Dropped table', table)
                drops.append(table)
        return drops

    def truncate_database(self, database=None):
        """Drop all tables in a database."""
        # Change database if needed
        if database in self.databases and database is not self.database:
            self.change_db(database)

        # Get list of tables
        tables = self.tables if isinstance(self.tables, list) else [self.tables]
        if len(tables) > 0:
            # Join list of tables into comma separated string
            tables_str = ', '.join([wrap(table) for table in tables])
            self.execute('DROP TABLE ' + tables_str)
            self._printer('\t' + str(len(tables)), 'tables truncated')
        return tables

    def execute_script(self, sql_script, commands=None, split_algo='sql_split', prep_statements=True,
                       dump_fails=True, execute_fails=True, ignored_commands=('DROP', 'UNLOCK', 'LOCK')):
        """Wrapper method for SQLScript class"""
        ss = SQLScript(sql_script, split_algo, prep_statements, dump_fails, self)
        ss.execute(commands, ignored_commands=ignored_commands, execute_fails=execute_fails)

    def script(self, sql_script, split_algo='sql_split', prep_statements=True, dump_fails=True):
        """Wrapper method providing access to the SQLScript class's methods and properties"""
        return SQLScript(sql_script, split_algo, prep_statements, dump_fails, self)

    def compare_dbs(self, db_x, db_y, show=True):
        """Compare the tables and row counts of two databases."""
        self._printer("\tComparing database's {0} and {1}".format(db_x, db_y))

        # Run compare_dbs_getter to get row counts
        x = self._compare_dbs_getter(db_x)
        y = self._compare_dbs_getter(db_y)
        x_tbl_count = len(list(x.keys()))
        y_tbl_count = len(list(y.keys()))

        # Check that database does not have zero tables
        if x_tbl_count == 0:
            self._printer('\tThe database {0} has no tables'.format(db_x))
            self._printer('\tDatabase differencing was not run')
            return None
        elif y_tbl_count == 0:
            self._printer('\tThe database {0} has no tables'.format(db_y))
            self._printer('\tDatabase differencing was not run')
            return None

        # Print comparisons
        if show:
            uniques_x = diff(x, y, x_only=True)
            if len(uniques_x) > 0:
                self._printer('\nUnique keys from {0} ({1} of {2}):'.format(db_x, len(uniques_x), x_tbl_count))
                self._printer('------------------------------')
                # print(uniques)
                for k, v in sorted(uniques_x):
                    self._printer('{0:25} {1}'.format(k, v))
                self._printer('\n')

            uniques_y = diff(x, y, y_only=True)
            if len(uniques_y) > 0:
                self._printer('Unique keys from {0} ({1} of {2}):'.format(db_y, len(uniques_y), y_tbl_count))
                self._printer('------------------------------')
                for k, v in sorted(uniques_y):
                    self._printer('{0:25} {1}'.format(k, v))
                self._printer('\n')

            if len(uniques_y) == 0 and len(uniques_y) == 0:
                self._printer("Databases's {0} and {1} are identical:".format(db_x, db_y))
                self._printer('------------------------------')

        return diff(x, y)

    def _compare_dbs_getter(self, db):
        """Retrieve a dictionary of table_name, row count key value pairs for a DB."""
        # Change DB connection if needed
        if self.database != db:
            self.change_db(db)
        return self.count_rows_all()

    def copy_table_structure(self, source_db, destination_db, table):
        """Copy a table from one database to another."""
        command = "CREATE TABLE {0}.{1} LIKE {2}.{3}".format(destination_db, table, source_db, table)
        self.execute(command)

    def copy_tables_structure(self, source_db, destination_db, tables=None):
        """Copy multiple tables from one database to another."""
        if tables is None:
            tables = self.tables
        for t in tables:
            self.copy_table_structure(source_db, destination_db, t)

    def create_database(self, name):
        """Create a new database."""
        statement = "CREATE DATABASE " + wrap(name) + " DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci"
        return self.execute(statement)

    def copy_database(self, source, destination):
        """
        Copy a database's content and structure.

        Inspiration: https://stackoverflow.com/questions/15110769/how-to-clone-mysql-database-under-a-different-name
        -with-the-same-name-and-the-sa
        """
        # Create destination database if it does not exist
        if destination not in self.databases:
            self.create_database(destination)
        # Truncate database if it does exist
        elif destination in self.databases:
            self.truncate_database(destination)

        # Change database to source
        self.change_db(source)

        # CREATE TABLE commands
        self.copy_tables_structure(source, destination)

        # Get table data and columns from source database
        rows = {tbl: self.select_all(tbl) for tbl in self.tables}
        cols = {tbl: self.get_columns(tbl) for tbl in self.tables}

        # Change database to destination
        self.change_db(destination)

        # Insert data into destination database
        for table in self.tables:
            self.insert_many(table, cols[table], rows[table])
