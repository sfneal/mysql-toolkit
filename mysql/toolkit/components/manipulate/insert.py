from differentiate import diff
from mysql.toolkit.utils import get_col_val_str, wrap
from mysql.toolkit.components.manipulate.select import MAX_ROWS_PER_QUERY


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
        unique = diff(existing_rows, values, y_only=True)  # Get values that are not in existing_rows

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
        # TODO: Cant accept lists?
        # Concatenate statement
        cols, vals = get_col_val_str(columns)
        statement = "INSERT INTO {0} ({1}) VALUES ({2})".format(wrap(table), cols, vals)

        # Execute statement
        if execute:
            self._cursor.execute(statement, values)
            self._commit()
            self._printer('\tMySQL row successfully inserted into {0}'.format(table))

        # Only return statement
        else:
            return statement

    def insert_many(self, table, columns, values, limit=MAX_ROWS_PER_QUERY, execute=True):
        """
        Insert multiple rows into a table.

        If only one row is found, self.insert method will be used.
        """
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
            return statement
