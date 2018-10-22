import unittest
from mysql.toolkit import MySQL


class TestManipulateSelect(unittest.TestCase):
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

    def test_select_all(self):
        table = 'dept_manager'

        rows = self.sql.select_all(table)
        self.assertEqual(len(rows), self.sql.count_rows(table))
        self.assertEqual(len(rows[0]), len(self.sql.get_columns(table)))

    def test_select(self):
        table = 'employees'
        cols = ['emp_no', 'birth_date', 'first_name', 'last_name']

        rows = self.sql.select(table, cols)
        self.assertEqual(len(rows), self.sql.count_rows(table))
        self.assertEqual(len(rows[0]), len(cols))

    def test_select_distinct(self):
        table = 'departments'
        cols = ['dept_name']

        rows = self.sql.select_distinct(table, cols)
        self.assertEqual(len(rows), self.sql.count_rows(table))

    def test_select_limit(self):
        table = 'salaries'
        limit = 22125

        rows = self.sql.select_limit(table, limit=limit)
        self.assertEqual(len(rows), 22125)

    def test_select_where(self):
        table = 'titles'
        cols = ['title']

        row = self.sql.select_where(table, cols, ('emp_no', 10001))
        self.assertEqual(row, 'Senior Engineer')


if __name__ == '__main__':
    unittest.main()
