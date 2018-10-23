import unittest
from mysql.toolkit import MySQL
from mysql.toolkit.datatypes import column_datatype


class TestDataTypesColumns(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = {
            "database": "testing_employees",
            "host": "stephenneal.net",
            "password": "thisisfortesting",
            "port": 3306,
            "raise_on_warnings": True,
            "user": "stephen_testing"
        }
        cls.sql = MySQL(config)

    @classmethod
    def tearDownClass(cls):
        cls.sql.disconnect()

    def tearDown(self):
        if self.sql.database != 'testing_employees':
            self.sql.change_db('testing_employees')

    def test_column_tinyint(self):
        self.sql.change_db('testing_models')
        column = self.sql.select_where('payments', ['customerNumber'], ('customerNumber', '<', 127))
        dt = column_datatype(column)
        self.assertEqual('TINYINT (3)', dt)

    def test_column_mediumint(self):
        column = self.sql.select_limit('dept_emp', ['emp_no'], limit=500)
        dt = column_datatype(column)
        self.assertEqual('SMALLINT (5)', dt)

    def test_column_decimal(self):
        self.sql.change_db('testing_models')
        column = self.sql.select('payments', 'amount')
        dt = column_datatype(column)
        self.assertEqual('DECIMAL (6, 2)', dt)

    def test_column_int_preferred(self):
        column = self.sql.select_limit('dept_emp', ['emp_no'], limit=500)
        dt = column_datatype(column, prefer_int=True)
        self.assertEqual('INT (5)', dt)

    def test_column_tinytext(self):
        column = self.sql.select_limit('dept_emp', ['dept_no'], limit=500)

        dt = column_datatype(column)
        self.assertEqual('TINYTEXT (4)', dt)

    def test_column_date(self):
        column = self.sql.select_limit('dept_emp', ['from_date'], limit=500)

        dt = column_datatype(column)
        self.assertEqual('DATE', dt)


if __name__ == '__main__':
    unittest.main()
