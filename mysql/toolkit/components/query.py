from differentiate import diff
from mysql.toolkit.utils import get_col_val_str, join_cols, wrap


MAX_ROWS_PER_QUERY = 50000
SELECT_QUERY_TYPES = ('SELECT', 'SELECT DISTINCT')


class Select:
    def select_all(self, table, limit=MAX_ROWS_PER_QUERY, execute=True):
        """Query all rows and columns from a table."""
        # Determine if a row per query limit should be set
        num_rows = self.count_rows(table)
        if num_rows > limit:
            return self._select_batched(table, '*', num_rows, limit, execute=execute)
        else:
            return self.select(table, '*', execute=execute)

    def select_distinct(self, table, cols='*', execute=True):
        """Query distinct values from a table."""
        return self.select(table, cols, execute, 'SELECT DISTINCT')

    def select(self, table, cols, execute=True, select_type='SELECT'):
        """Query every row and only certain columns from a table."""
        # Validate query type
        select_type = select_type.upper()
        assert select_type in SELECT_QUERY_TYPES

        # Concatenate statement
        statement = '{0} {1} FROM {2}'.format(select_type, join_cols(cols), wrap(table))
        if execute:  # Execute commands
            return self.fetch(statement)
        else:  # Return command
            return statement

    def select_all_join(self, table1, table2, key):
        """Left join all rows and columns from two tables where a common value is shared."""
        # TODO: Write function to run a select * left join query
        pass

    def select_limit(self, table, cols='*', offset=0, limit=MAX_ROWS_PER_QUERY):
        """Run a select query with an offset and limit parameter."""
        return self.fetch(self._select_limit_statement(table, cols, offset, limit))

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
        self.fetch(statement)

    def _select_batched(self, table, cols, num_rows, limit, queries_per_batch=3, execute=True):
        """Run select queries in small batches and return joined resutls."""
        # Execute select queries in small batches to avoid connection timeout
        commands, offset = [], 0
        while num_rows > 0:
            # Use number of rows as limit if num_rows < limit
            _limit = min(limit, num_rows)

            # Execute select_limit query
            commands.append(self._select_limit_statement(table, cols=cols, offset=offset, limit=limit))
            offset += _limit
            num_rows += -_limit

        # Execute commands
        if execute:
            rows = []
            til_reconnect = queries_per_batch
            for c in commands:
                if til_reconnect == 0:
                    self.disconnect()
                    self.reconnect()
                    til_reconnect = queries_per_batch
                rows.extend(self.fetch(c, False))
                til_reconnect += -1
            del commands
            return rows
        # Return commands
        else:
            return commands

    @staticmethod
    def _select_limit_statement(table, cols='*', offset=0, limit=MAX_ROWS_PER_QUERY):
        """Concatenate a select with offset and limit statement."""
        return 'SELECT {0} FROM {1} LIMIT {2}, {3}'.format(cols, wrap(table), offset, limit)


class Insert:
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

    def insert(self, table, columns, values, execute=True):
        """Insert a single row into a table."""
        # Concatenate statement
        cols, vals = get_col_val_str(columns)
        statement = "INSERT INTO " + wrap(table) + " (" + cols + ") " + "VALUES (" + vals + ")"

        # Execute statement
        if execute:
            self._cursor.execute(statement, values)
            self._printer('\tMySQL row successfully inserted')

        # Only return statement
        else:
            return statement

    def insert_many(self, table, columns, values, limit=MAX_ROWS_PER_QUERY, execute=True):
        """
        Insert multiple rows into a table.

        If only one row is found, self.insert method will be used.
        """
        # Valid that at least one row is to be inserted
        if len(values) < 2:
            return False

        # Make values a list of lists if it is a flat list
        if not isinstance(values[0], (list, set, tuple)):
            values = []
            for v in values:
                if v is not None and len(v) > 0:
                    values.append([v])
                else:
                    values.append([None])

        # Concatenate statement
        cols, vals = get_col_val_str(columns)
        statement = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(wrap(table), cols, vals)

        if execute and len(values) > limit:
            while len(values) > 0:
                vals = [values.pop(0) for i in range(0, min(limit, len(values)))]
                self._cursor.executemany(statement, vals)
                self._commit()

        elif execute:
            # Execute statement
            self._cursor.executemany(statement, values)
            self._commit()
            self._printer('\tMySQL rows (' + str(len(values)) + ') successfully INSERTED')

        # Only return statement
        else:
            return statement, values


class Update:
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

    def update_many(self, table, columns, values, where_col, where_index):
        """Update the values of several rows."""
        for row in values:
            self.update(table, columns, row, (where_col, row[where_index]))


class Query(Select, Insert, Update):
    def __init__(self):
        pass
