from mysql.toolkit.utils import cols_str, wrap


def insert_statement(table, columns, values):
    vals = string = [str(tuple([col if col is not None else 'NULL' for col in row])) for row in values]
    statement = "INSERT INTO {0} ({1}) VALUES \n{2}".format(wrap(table), cols_str(columns), vals)
    return statement


class Export:
    def export_table(self, table, drop_statement=True, truncate_statement=False):
        """Export a table structure and data to SQL file for backup or later import."""
        create_statement = self.get_table_definition(table)
        data = self.select_all(table)
        print('\n')
        print('\n')
        print(insert_statement(table, self.get_columns(table), data))
        print('\n')

        print('\n')
