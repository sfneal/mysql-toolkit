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

        # No inserted or updated rows
        if len(to_insert) < 1 and len(to_update) < 0:
            self._printer('No rows added to', table)

    def update_many(self, table, columns, values, where_col, where_index):
        """Update the values of several rows."""
        for row in values:
            self.update(table, columns, row, (where_col, row[where_index]))

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

    def execute_script(self, sql_script, commands=None, split_algo='sql_split', prep_statements=True,
                       dump_fails=True, execute_fails=True):
        """Wrapper method for SQLScript class"""
        ss = SQLScript(sql_script, split_algo, prep_statements, dump_fails, self)
        ss.execute(commands, execute_fails=execute_fails)

    def script(self, sql_script, split_algo='sql_split', prep_statements=True, dump_fails=True):
        """Wrapper method providing access to the SQLScript class's methods and properties"""
        return SQLScript(sql_script, split_algo, prep_statements, dump_fails, self)

    def compare_dbs(self, db_x, db_y, show=True):
        """Compare the tables and row counts of several databases."""
        self._printer("\tComparing database's {0} and {1}".format(db_x, db_y))
        if self.database != db_x:
            self.change_db(db_x)
        x = self.count_rows_all()
        x_tbls = len(self.tables)
        if x_tbls == 0:
            return

        if self.database != db_y:
            self.change_db(db_y)
        y = self.count_rows_all()
        y_tbls = len(self.tables)
        if y_tbls == 0:
            return

        # Print comparisons
        if show:
            uniques = diff(x, y, x_only=True)
            print('Unique keys from {0} ({1} of {2}):'.format(db_x, len(uniques), x_tbls))
            print('------------------------------')
            for k, v in sorted(uniques):
                print('{0:25} {1}'.format(k, v))
            print('\n')

            uniques = diff(x, y, y_only=True)
            print('Unique keys from {0} ({1} of {2}):'.format(db_y, len(uniques), y_tbls))
            print('------------------------------')
            for k, v in sorted(uniques):
                print('{0:25} {1}'.format(k, v))
            print('\n')
        return diff(x, y)
