from mysql.toolkit.utils import join_cols, wrap


MAX_ROWS_PER_QUERY = 50000
SELECT_QUERY_TYPES = ('SELECT', 'SELECT DISTINCT')
SELECT_WHERE_OPERATORS = ('=', '<>', '<', '>', '!=', '<=', '>=')
JOIN_QUERY_TYPES = ('INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN')


class Select:
    # TODO: Add params to allow for return values in dict form
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
        return self.select(table, cols, execute, select_type='SELECT DISTINCT')

    def select(self, table, cols, execute=True, select_type='SELECT', return_type=list):
        """Query every row and only certain columns from a table."""
        # Validate query type
        select_type = select_type.upper()
        assert select_type in SELECT_QUERY_TYPES

        # Concatenate statement
        statement = '{0} {1} FROM {2}'.format(select_type, join_cols(cols), wrap(table))
        if not execute:
            # Return command
            return statement

        # Retrieve values
        values = self.fetch(statement)
        return self._return_rows(table, cols, values, return_type)

    def select_join(self, table1, table2, cols, table1_col, table2_col=None, join_type=None):
        """
        Left join all rows and columns from two tables where a common value is shared.

        :param table1: Name of table #1
        :param table2: Name of table #2
        :param cols: List of columns or column tuples
            String or flat list: Assumes column(s) are from table #1 if not specified
            List of tuples: Each tuple in list of columns represents (table_name, column_name)
        :param table1_col: Column from table #1 to use as key
        :param table2_col: Column from table #2 to use as key
        :param join_type: Type of join query
        :return: Queried rows
        """
        # Check if cols is a list of tuples
        if isinstance(cols[0], tuple):
            cols = join_cols(['{0}.{1}'.format(tbl, col) for tbl, col in cols])
        else:
            cols = join_cols(['{0}.{1}'.format(table1, col) for col in cols])

        # Validate join_type and table2_col
        join_type = join_type.lower().split(' ', 1)[0].upper() + ' JOIN' if join_type else 'LEFT JOIN'
        assert join_type in JOIN_QUERY_TYPES
        table2_col = table2_col if table2_col else table1_col

        # Concatenate and return statement
        statement = '''
        SELECT {columns}
        FROM {table1}
        {join_type} {table2} ON {table1}.{table1_col} = {table2}.{table2_col}
        '''.format(table1=wrap(table1), table2=wrap(table2), columns=cols, table1_col=table1_col, table2_col=table2_col,
                   join_type=join_type)
        return self.fetch(statement)

    def select_limit(self, table, cols='*', offset=0, limit=MAX_ROWS_PER_QUERY):
        """Run a select query with an offset and limit parameter."""
        return self.fetch(self._select_limit_statement(table, cols, offset, limit))

    def select_where(self, table, cols, where, return_type=list):
        """
        Query certain rows from a table where a particular value is found.

        cols parameter can be passed as a iterable (list, set, tuple) or a string if
        only querying a single column.  where parameter can be passed as a two or three
        part tuple.  If only two parts are passed the assumed operator is equals(=).

        :param table: Name of table
        :param cols: List, tuple or set of columns or string with single column name
        :param where: WHERE clause, accepts either a two or three part tuple
            two-part: (where_column, where_value)
            three-part: (where_column, comparison_operator, where_value)
        :param return_type: Type, type to return values in
        :return: Queried rows
        """
        # Unpack WHERE clause dictionary into tuple
        if isinstance(where, (list, set)):
            # Multiple WHERE clause's (separate with AND)
            clauses = [self._where_clause(clause) for clause in where]
            where_statement = ' AND '.join(clauses)
        else:
            where_statement = self._where_clause(where)

        # Concatenate full statement and execute
        statement = "SELECT {0} FROM {1} WHERE {2}".format(join_cols(cols), wrap(table), where_statement)
        values = self.fetch(statement)
        return self._return_rows(table, cols, values, return_type)

    def select_where_between(self, table, cols, where_col, between):
        """
        Query rows from a table where a columns value is found between two values.

        :param table: Name of the table
        :param cols: List, tuple or set of columns or string with single column name
        :param where_col: Column to check values against
        :param between: Tuple with min and max values for comparison
        :return: Queried rows
        """
        # Unpack WHERE clause dictionary into tuple
        min_val, max_val = between

        # Concatenate full statement and execute
        statement = "SELECT {0} FROM {1} WHERE {2} BETWEEN {3} AND {4}".format(join_cols(cols), wrap(table), where_col,
                                                                               min_val, max_val)
        return self.fetch(statement)

    def select_where_like(self, table, cols, where_col, start=None, end=None, anywhere=None,
                          index=(None, None), length=None):
        """
        Query rows from a table where a specific pattern is found in a column.

        MySQL syntax assumptions:
            (%) The percent sign represents zero, one, or multiple characters.
            (_) The underscore represents a single character.

        :param table: Name of the table
        :param cols: List, tuple or set of columns or string with single column name
        :param where_col: Column to check pattern against
        :param start: Value to be found at the start
        :param end: Value to be found at the end
        :param anywhere: Value to be found anywhere
        :param index: Value to be found at a certain index
        :param length: Minimum character length
        :return: Queried rows
        """
        # Retrieve search pattern
        pattern = self._like_pattern(start, end, anywhere, index, length)

        # Concatenate full statement and execute
        statement = "SELECT {0} FROM {1} WHERE {2} LIKE '{3}'".format(join_cols(cols), wrap(table), where_col, pattern)
        return self.fetch(statement)

    @staticmethod
    def _where_clause(where):
        """
        Unpack a where clause tuple and concatenate a MySQL WHERE statement.

        :param where: 2 or 3 part tuple containing a where_column and a where_value (optional operator)
        :return: WHERE clause statement
        """
        assert isinstance(where, tuple)
        if len(where) == 3:
            where_col, operator, where_val = where
        else:
            where_col, where_val = where
            operator = '='
        assert operator in SELECT_WHERE_OPERATORS

        # Concatenate WHERE clause (ex: **first_name='John'**)
        return "{0}{1}'{2}'".format(where_col, operator, where_val)

    def _return_rows(self, table, cols, values, return_type):
        """Return fetched rows in the desired type."""
        if return_type is dict:
            # Pack each row into a dictionary
            cols = self.get_columns(table) if cols is '*' else cols
            if len(values) > 0 and isinstance(values[0], (set, list, tuple)):
                return [dict(zip(cols, row)) for row in values]
            else:
                return dict(zip(cols, values))
        elif return_type is tuple:
            return [tuple(row) for row in values]
        else:
            return values

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
        return 'SELECT {0} FROM {1} LIMIT {2}, {3}'.format(join_cols(cols), wrap(table), offset, limit)

    @staticmethod
    def _like_pattern(start, end, anywhere, index, length):
        """
        Create a LIKE pattern to use as a search parameter for a WHERE clause.

        :param start: Value to be found at the start
        :param end: Value to be found at the end
        :param anywhere: Value to be found anywhere
        :param index: Value to be found at a certain index
        :param length: Minimum character length
        :return: WHERE pattern
        """
        # Unpack index tuple
        index_num, index_char = index
        index = None

        # Start, end, anywhere
        if all(i for i in [start, end, anywhere]) and not any(i for i in [index, length]):
            return '{start}%{anywhere}%{end}'.format(start=start, end=end, anywhere=anywhere)

        # Start, end
        elif all(i for i in [start, end]) and not any(i for i in [anywhere, index, length]):
            return '{start}%{end}'.format(start=start, end=end)

        # Start, anywhere
        elif all(i for i in [start, anywhere]) and not any(i for i in [end, index, length]):
            return '{start}%{anywhere}%'.format(start=start, anywhere=anywhere)

        # End, anywhere
        elif all(i for i in [end, anywhere]) and not any(i for i in [start, index, length]):
            return '%{anywhere}%{end}'.format(end=end, anywhere=anywhere)

        # Start
        elif start and not any(i for i in [end, anywhere, index, length]):
            return '{start}%'.format(start=start)

        # End
        elif end and not any(i for i in [start, anywhere, index, length]):
            return '%{end}'.format(end=end)

        # Anywhere
        elif anywhere and not any(i for i in [start, end, index, length]):
            return '%{anywhere}%'.format(anywhere=anywhere)

        # Index
        elif index_num and index_char and not any(i for i in [start, end, anywhere, length]):
            return '{index_num}{index_char}%'.format(index_num='_' * (index_num + 1), index_char=index_char)

        # Length
        elif length and not any(i for i in [start, end, anywhere, index]):
            return '{length}'.format(length='_%' * length)

        else:
            return None