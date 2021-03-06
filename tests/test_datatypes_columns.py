import unittest
from looptools import Timer
from mysql.toolkit import MySQL
from mysql.toolkit.datatypes import sql_column_type
from tests import config


class TestDataTypesColumns(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = MySQL(config())

    @classmethod
    def tearDownClass(cls):
        cls.sql.disconnect()

    def tearDown(self):
        if self.sql.database != 'testing_employees':
            self.sql.change_db('testing_employees')

    @Timer.decorator
    def test_column_tinyint(self):
        self.sql.change_db('testing_models')
        column = self.sql.select_where('payments', ['customerNumber'], ('customerNumber', '<', 127))
        dt = sql_column_type(column)
        self.assertEqual('TINYINT (3)', dt)

    @Timer.decorator
    def test_column_smallint(self):
        column = self.sql.select_limit('dept_emp', ['emp_no'], limit=500)
        dt = sql_column_type(column)
        self.assertEqual('SMALLINT (5)', dt)

    @Timer.decorator
    def test_column_int_preferred(self):
        column = self.sql.select_limit('dept_emp', ['emp_no'], limit=500)
        dt = sql_column_type(column, prefer_int=True)
        self.assertEqual('INT (5)', dt)

    @Timer.decorator
    def test_column_decimal(self):
        self.sql.change_db('testing_models')
        column = self.sql.select('payments', 'amount')
        dt = sql_column_type(column)
        self.assertEqual('DECIMAL (6, 2)', dt)

    @Timer.decorator
    def test_column_tinytext(self):
        column = self.sql.select_limit('dept_emp', ['dept_no'], limit=500)

        dt = sql_column_type(column)
        self.assertEqual('TINYTEXT (4)', dt)

    @Timer.decorator
    def test_column_tinytext_varchar_preferred(self):
        column = self.sql.select_limit('dept_emp', ['dept_no'], limit=500)

        dt = sql_column_type(column, prefer_varchar=True)
        self.assertEqual('VARCHAR (4)', dt)

    @Timer.decorator
    def test_column_varchar(self):
        self.sql.change_db('testing_models')
        column = self.sql.select('productlines', 'textDescription')
        dt = sql_column_type(column)
        self.assertEqual('VARCHAR (735)', dt)

    @Timer.decorator
    def test_column_date(self):
        column = self.sql.select_limit('dept_emp', ['from_date'], limit=500)

        dt = sql_column_type(column)
        self.assertEqual('DATE', dt)


if __name__ == '__main__':
    unittest.main()
