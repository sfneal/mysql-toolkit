from differentiate import diff
from mysql.toolkit.utils import wrap
from mysql.toolkit.script.script import SQLScript
from tqdm import tqdm


class Operations:
    def __init__(self):
        pass

    def create_table(self, name, data, headers=None):
        """Generate and execute a create table query by parsing a 2D dataset"""
        # TODO: Finish writing method
        # Set headers list
        if not headers:
            headers = data[0]

        # Create dictionary columns and data types from headers list
        data_types = {header: {'type': None, 'max': None} for header in headers}

        # Confirm that each row of the dataset is the same length
        for row in data:
            assert len(row) == len(headers)
            row_dict = dict(zip(headers, row))
            for k, v in row_dict.items():
                if data_types[k]['type'] is None:
                    data_types[k]['type'] = type(v)
                    data_types[k]['max'] = len(str(v))
                else:
                    data_types[k]['max'] = max(len(str(v)), data_types[k]['max'])

        for k, v in data_types.items():
            print(k, v)

        # Create list of columns
        columns = []
        for column, v in data_types.items():
            var_type = 'INT' if isinstance(v['type'], int) else 'VARCHAR'
            var_max = str(v['max'])

        self._printer(columns)
        statement = "create table " + name + " ("
        self._printer(statement)

    def backup_database(self, structure=True, data=True):
        # TODO: Create method
        pass

    def drop(self, table):
        """Drop a table from a database."""
        self.execute('DROP TABLE ' + wrap(table))
        return table

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

    def truncate(self, table):
        """Empty a table by deleting all of its rows."""
        statement = "TRUNCATE " + wrap(table)
        self.execute(statement)
        self._printer('\tMySQL table ' + str(table) + ' successfully truncated')

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

    def execute_script(self, sql_script=None, commands=None, split_algo='sql_split', prep_statements=True,
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
        self.execute('CREATE TABLE {0}.{1} LIKE {2}.{1}'.format(destination_db, wrap(table), source_db))

    def copy_tables_structure(self, source_db, destination_db, tables=None):
        """Copy multiple tables from one database to another."""
        # Change database to source
        self.change_db(source_db)

        if tables is None:
            tables = self.tables

        # Change database to destination
        self.change_db(destination_db)
        print('\n')
        for t in tqdm(tables, total=len(tables), desc='Copying {0} table structure'.format(source_db)):
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
        print('\tCopying database {0} strucutre and data to database {1}'.format(source, destination))
        # Create destination database if it does not exist
        if destination in self.databases:
            self.truncate_database(destination)
        # Truncate database if it does exist
        else:
            self.create_database(destination)

        # CREATE TABLE commands
        self.copy_tables_structure(source, destination)

        # Change database to source
        self.enable_printing = False
        self.change_db(source)

        # Get table data and columns from source database
        tables = self.tables
        rows = {tbl: self.select_all(tbl) for tbl in tqdm(tables, total=len(tables),
                                                          desc='Getting {0} rows'.format(source))}
        cols = {tbl: self.get_columns(tbl) for tbl in tqdm(tables, total=len(tables),
                                                           desc='Getting {0} columns'.format(source))}

        # Validate rows and columns
        for r in list(rows.keys()):
            assert r in tables, r
        for c in list(cols.keys()):
            assert c in tables, c

        # Change database to destination
        self.change_db(destination)

        # Insert data into destination database
        for table in tqdm(tables, total=len(tables), desc='Inserting rows into tables'):
            if len(rows[table]) > 1:
                self.insert_many(table, cols[table], rows[table])
            else:
                self.insert(table, cols[table], rows[table])
        self.enable_printing = True
