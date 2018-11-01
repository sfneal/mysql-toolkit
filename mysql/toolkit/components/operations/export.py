from textwrap import wrap, fill
from tqdm import tqdm
from decimal import Decimal
from json import dumps

from mysql.connector.conversion import MySQLConverterBase
from mysql.toolkit.utils import cols_str, wrap
from mysql.toolkit.commands.dump import write_text


def insert_statement(table, columns, values):
    """Generate an insert statement string for dumping to text file or MySQL execution."""
    if not all(isinstance(r, (list, set, tuple)) for r in values):
        values = [[r] for r in values]
    rows = []
    for row in values:
        new_row = []
        for col in row:
            if col is None:
                new_col = 'NULL'
            elif isinstance(col, (int, float, Decimal)):
                new_col = str(MySQLConverterBase().to_mysql(col))
            else:
                string = str(MySQLConverterBase().to_mysql(col))
                if "'" in string:
                    new_col = '"' + string + '"'
                else:
                    new_col = "'" + string + "'"
            new_row.append(new_col)
        rows.append(', '.join(new_row))
    vals = '(' + '),\n\t('.join(rows) + ')'
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
        if len(data) > 0:
            statements.append('{0};'.format(insert_statement(table, self.get_columns(table), data)))
        return '\n'.join(statements)

    def dump_database(self, file_path, database=None, tables=None):
        """
        Export the table structure and data for tables in a database.

        If not database is specified, it is assumed the currently connected database
        is the source.  If no tables are provided, all tables will be dumped.
        """
        # Change database if needed
        if database:
            self.change_db(database)

        # Set table
        if not tables:
            tables = self.tables

        # Retrieve and join dump statements
        statements = [self.dump_table(table) for table in tqdm(tables, total=len(tables), desc='Generating dump files')]
        dump = 'SET FOREIGN_KEY_CHECKS=0;' + '\n'.join(statements) + '\nSET FOREIGN_KEY_CHECKS=1;'

        # Write dump statements to sql file
        file_path = file_path if file_path.endswith('.sql') else file_path + '.sql'
        write_text(dump, file_path)
        return file_path
