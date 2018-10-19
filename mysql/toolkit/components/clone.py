from looptools import Timer
from mysql.toolkit.utils import wrap
from tqdm import tqdm


class CloneDatabase:
    """Extension of Database class for particular use cases."""
    def copy_database_slow(self, source, destination, optimized=False):
        # Copy table structures
        self.copy_database_structure(source, destination)

        # Copy table data
        self.copy_database_data(source, destination, optimized)

    def copy_database_structure(self, source, destination, tables=None):
        """Copy multiple tables from one database to another."""
        # Change database to source
        self.change_db(source)

        if tables is None:
            tables = self.tables

        # Change database to destination
        self.change_db(destination)
        print('\n')
        for t in tqdm(tables, total=len(tables), desc='Copying {0} table structure'.format(source)):
            self.copy_table_structure(source, destination, t)

    def copy_table_structure(self, source_db, destination_db, table):
        """Copy a table from one database to another."""
        self.execute('CREATE TABLE {0}.{1} LIKE {2}.{1}'.format(destination_db, wrap(table), source_db))

    def copy_database_data(self, source, destination, optimized=False):
        """
        Copy the data from one database to another.

        Retrieve existing data from the source database and insert that data into the destination database.
        """
        # Change database to source
        self.enable_printing = False
        self.change_db(source)
        tables = self.tables

        if not optimized:
            # Retrieve database rows
            rows = self.get_database_rows(tables, source)

            # Retrieve database columns
            cols = self.get_database_columns(tables, source)

            # Validate rows and columns
            for r in list(rows.keys()):
                assert r in tables, r
            for c in list(cols.keys()):
                assert c in tables, c

            # Change database to destination
            self.change_db(destination)

            # Get insert queries
            insert_queries = self._set_database_rows_insert_queries(rows, cols)

            # Execute insert queries
            self._set_database_rows_execute_queries(insert_queries)

        # Copy database data by executing INSERT and SELECT commands in a single query
        else:
            for table in tqdm(tables, total=len(tables), desc='Copying table data (optimized)'):
                self.copy_table_data_optimized(source, destination, table)

        self.enable_printing = True

    def copy_table_data_optimized(self, source, destination, table):
        """Select rows from a source database and insert them into a destination db in one query"""
        self.execute('INSERT INTO {0}.{1} SELECT * FROM {2}.{1}'.format(destination, wrap(table), source))

    def get_database_rows(self, tables=None, database=None):
        """Retrieve a dictionary of table keys and list of rows values for every table."""
        # Get table data and columns from source database
        source = database if database else self.database
        tables = tables if tables else self.tables

        # Get database select queries
        commands = self._get_database_rows_select_queries(source, tables)

        # Execute select commands
        return self._get_database_rows_execute_queries(source, commands)

    def get_database_columns(self, tables=None, database=None):
        """Retrieve a dictionary of columns."""
        # Get table data and columns from source database
        source = database if database else self.database
        tables = tables if tables else self.tables
        return {tbl: self.get_columns(tbl) for tbl in tqdm(tables, total=len(tables),
                                                           desc='Getting {0} columns'.format(source))}

    def _get_database_rows_select_queries(self, source, tables):
        """Create select queries for all of the tables from a source database."""
        # Create dictionary of select queries
        row_queries = {tbl: self.select_all(tbl, execute=False) for tbl in
                       tqdm(tables, total=len(tables), desc='Getting {0} select queries'.format(source))}

        # Convert command strings into lists of commands
        for tbl, command in row_queries.items():
            if isinstance(command, str):
                row_queries[tbl] = [command]

        # Pack commands into list of tuples
        return [(tbl, cmd) for tbl, cmds in row_queries.items() for cmd in cmds]

    def _get_database_rows_execute_queries(self, source, commands):
        """Execute select queries for all of the tables from a source database."""
        rows = {}
        for tbl, command in tqdm(commands, total=len(commands), desc='Executing {0} select queries'.format(source)):
            # Add key to dictionary
            if tbl not in rows:
                rows[tbl] = []
            rows[tbl].extend(self.fetch(command, commit=True))
        self._commit()
        return rows

    def _set_database_rows_insert_queries(self, rows, cols):
        """Retrieve dictionary of insert statements to be executed."""
        # Get insert queries
        insert_queries = {}
        for table in tqdm(list(rows.keys()), total=len(list(rows.keys())), desc='Getting insert rows queries'):
            insert_queries[table] = {}
            _rows = rows.pop(table)
            _cols = cols.pop(table)

            if len(_rows) > 1:
                insert_queries[table]['insert_many'] = self.insert_many(table, _cols, _rows, execute=False)
            elif len(_rows) == 1:
                insert_queries[table]['insert'] = self.insert(table, _cols, _rows, execute=False)
        return insert_queries

    def _set_database_rows_execute_queries(self, insert_queries):
        # Insert data into destination database
        for table in tqdm(list(insert_queries.keys()), total=len(list(insert_queries.keys())),
                          desc='Inserting rows into tables'):
            query = insert_queries.pop(table)
            if 'insert_many' in query:
                stmt, params = query['insert_many']
                self.executemany(stmt, params)
            elif 'insert' in query:
                self.execute(query['insert'])


class Clone(CloneDatabase):
    def copy_database(self, source, destination, optimized=False, one_query=False):
        """
        Copy a database's content and structure.

        SMALL Database speed improvements (DB size < 5mb)
        Using optimized is about 178% faster
        Using one_query is about 200% faster

        LARGE Database speed improvements (DB size > 5mb)
        Using optimized is about 900% faster
        Using one_query is about 2600% faster
        """
        print('\tCopying database {0} structure and data to database {1}'.format(source, destination))
        with Timer('\nSuccess! Copied database {0} to {1} in '.format(source, destination)):
            # Create destination database if it does not exist
            if destination in self.databases:
                self.truncate_database(destination)
            # Truncate database if it does exist
            else:
                self.create_database(destination)

            if not one_query:
                self.copy_database_slow(source, destination, optimized)
            else:
                self._copy_tables_onequery(source, destination)

    def _copy_tables_onequery(self, source, destination, tables=None, primary_keys=True):
        """Copy all tables in a DB by executing CREATE TABLE, SELECT and INSERT INTO statements all in one query."""
        # Change database to source
        self.change_db(source)
        if tables is None:
            tables = self.tables

        # Get dict of primary keys
        pk = {tbl: self.get_primary_key(tbl) for tbl in tables}

        # Change database to destination
        self.change_db(destination)
        print('\n')
        _enable_printing = self.enable_printing
        self.enable_printing = False
        for table in tqdm(tables, total=len(tables), desc='Copying {0} table (onequery)'.format(source)):
            self.execute('CREATE TABLE {0}.{1} SELECT * FROM {2}.{1}'.format(destination, wrap(table), source))
            if primary_keys:
                self.set_primary_key(table, pk[table])
        self.enable_printing = _enable_printing

