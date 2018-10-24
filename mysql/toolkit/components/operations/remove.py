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
        self._printer('\tMySQL table ' + str(table) + ' successfully truncated')

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

    def drop(self, table):
        """
        Drop a table from a database.

        Accepts either a string representing a table name or a list of strings
        representing a table names.
        """
        existing_tables = self.tables
        if isinstance(table, (list, set, tuple)):
            for t in table:
                self._drop(t, existing_tables)
        else:
            self._drop(table, existing_tables)
        return table

    def _drop(self, table, existing_tables=None):
        """Private method for executing table drop commands."""
        # Retrieve list of existing tables for comparison
        existing_tables = existing_tables if existing_tables else self.tables

        # Only drop table if it exists
        if table in existing_tables:
            # Set to avoid foreign key errorrs
            self.execute('SET FOREIGN_KEY_CHECKS = 0')

            query = 'DROP TABLE {0}'.format(wrap(table))
            self.execute(query)

            # Set again
            self.execute('SET FOREIGN_KEY_CHECKS = 1')
            self._printer('\tDropped table {0}'.format(table))

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
                drops.append(table)
        return drops