from mysql.toolkit.utils import wrap


class Remove:
    def truncate(self, table):
        """Empty a table by deleting all of its rows."""
        if isinstance(table, (list, set, tuple)):
            for t in table:
                self._truncate(t)
        else:
            self._truncate(table)

    def _truncate(self, table):
        """
        Remove all records from a table in MySQL

        It performs the same function as a DELETE statement without a WHERE clause.
        """
        statement = "TRUNCATE TABLE {0}".format(wrap(table))
        self.execute(statement)
        self._printer('\tTruncated table {0}'.format(table))

    def truncate_database(self, database=None):
        """Drop all tables in a database."""
        # Change database if needed
        if database in self.databases and database is not self.database:
            self.change_db(database)

        # Get list of tables
        tables = self.tables if isinstance(self.tables, list) else [self.tables]
        if len(tables) > 0:
            self.drop(tables)
            self._printer('\t' + str(len(tables)), 'tables truncated from', database)
        return tables

    def drop_empty_tables(self):
        """Drop all empty tables in a database."""
        # Count number of rows in each table
        counts = self.count_rows_all()

        # Loop through each table key and validate that rows count is not 0
        to_drop = [table for table, count in counts.items() if count < 1]

        # Drop table if it contains no rows
        self.drop(to_drop)
        return to_drop

    def drop(self, table, if_exists=True, check_foreign_keys=True):
        """
        Drop a table from a database.

        Accepts either a string representing a table name or a list of strings
        representing a table names.

        :param table: Name of the table
        :param if_exists: Bool, drop table conditionally if it exists
        :param check_foreign_keys: Bool, disable foreign key checks before dropping
        :return: Table name
        """
        # Set to avoid foreign key errors
        if check_foreign_keys:
            self.execute('SET FOREIGN_KEY_CHECKS = 0')

        # Multiple tables or single table
        if isinstance(table, (list, set, tuple)):
            for t in table:
                self._drop(t, if_exists)
        else:
            self._drop(table, if_exists)

        # Set again
        if check_foreign_keys:
            self.execute('SET FOREIGN_KEY_CHECKS = 1')
        return table

    def _drop(self, table, if_exists=True):
        """Private method for executing table drop commands."""
        # Only drop table if it exists
        if if_exists:
            query = 'DROP TABLE IF EXISTS {0}'.format(wrap(table))

        # Attempt and drop table without validation, raises error on failure
        else:
            query = 'DROP TABLE {0}'.format(wrap(table))

        # Execute query
        self.execute(query)
        self._printer('\tDropped table {0}'.format(table))
