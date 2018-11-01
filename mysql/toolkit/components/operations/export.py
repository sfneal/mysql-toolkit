from textwrap import wrap, fill
from tqdm import tqdm
from mysql.toolkit.utils import cols_str, wrap


def insert_statement(table, columns, values):
    """Generate an insert statement string for dumping to text file or MySQL execution."""
    vals = '\n\t'.join([str(tuple([col if col is not None else 'NULL' for col in row])) for row in values])
    statement = "INSERT INTO\n\t{0} ({1}) \nVALUES\n\t{2}".format(wrap(table), cols_str(columns), vals)
    return statement


def sql_file_comment(comment):
    """Automatically wrap strings to create SQL comments."""
    return '--  ' + '\n--  '.join(fill(comment, 77).split('\n'))


class Export:
    def dump_table(self, table, drop_statement=True):
        """Export a table structure and data to SQL file for backup or later import."""
        create_statement = self.get_table_definition(table)
        data = self.select_all(table)
        statements = ['\n', sql_file_comment(''),
                      sql_file_comment('Table structure and data dump for {0}'.format(table)), sql_file_comment('')]
        if drop_statement:
            statements.append('\nDROP TABLE IF EXISTS {0};'.format(wrap(table)))
        statements.append('{0};\n'.format(create_statement))
        statements.append('{0};'.format(insert_statement(table, self.get_columns(table), data)))
        return '\n'.join(statements)

    def dump_database(self, database=None, tables=None):
        """
        Export the table structure and data for tables in a database.

        If not database is specified, it is assumed the currently connected database
        is the source.  If no tables are provided, all tables will be dumped.
        """
        # Change database if needed
        if database and database != self.database:
            self.change_db(database)

        # Set table
        if not tables:
            tables = self.tables

        # Retrieve dump statements
        statements = [self.dump_table(table) for table in tqdm(tables, total=len(tables), desc='Generating dump files')]
        return '\n'.join(statements)

