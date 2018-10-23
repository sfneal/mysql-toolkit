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

    def tearDown(self):
        self.sql.change_db('testing_employees')

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

    def test_select_where_less(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where('payments', ['customerNumber'], ('customerNumber', '<', 127))
        self.assertEqual(len(rows), 26)

    def test_select_where_more(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where('payments', ['customerNumber'], ('customerNumber', '>', 127))
        self.assertEqual(len(rows), 247)

    def test_select_where_between(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where_between('payments', ['amount'], 'amount', (1500, 10000))
        self.assertEqual(len(rows), 39)

    def test_select_where_like_1(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where_like('customers', ['customerName'], 'customerName', start='a', end='.',
                                          anywhere='&')
        self.assertEqual(len(rows), 2)

    def test_select_where_like_2(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where_like('customers', ['customerName'], 'customerName', start='c', end=None,
                                          anywhere='collect', index=(None, None), length=None)
        self.assertEqual(len(rows), 2)

    def test_select_where_like_3(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where_like('customers', ['customerName'], 'customerName', start=None, end='ltd',
                                          anywhere='o', index=(None, None), length=None)
        self.assertEqual(len(rows), 8)

    def test_select_where_like_4(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where_like('customers', ['customerName'], 'customerName', start='mini', end=None,
                                          anywhere=None, index=(None, None), length=None)
        self.assertEqual(len(rows), 6)

    def test_select_where_like_5(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where_like('customers', ['customerName'], 'customerName', start=None, end='co.',
                                          anywhere=None, index=(None, None), length=None)
        self.assertEqual(len(rows), 26)

    def test_select_where_like_6(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where_like('customers', ['customerName'], 'customerName', start=None, end=None,
                                          anywhere='imports', index=(None, None), length=None)
        self.assertEqual(len(rows), 9)

    def test_select_where_like_7(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where_like('customers', ['customerName'], 'customerName', start=None, end=None,
                                          anywhere=None, index=(1, "i"), length=None)
        self.assertEqual(len(rows), 5)

    def test_select_where_like_8(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where_like('customers', ['customerName'], 'customerName', start=None, end=None,
                                          anywhere=None, index=(None, None), length=20)
        self.assertEqual(len(rows), 68)

    def test_select_join_left(self):
        self.sql.change_db('testing_models')
        cols = [
            ('employees', 'email'),
            ('employees', 'jobTitle'),
            ('customers', 'customerName'),
        ]
        rows = self.sql.select_join('customers', 'employees', cols, table1_col='salesRepEmployeeNumber',
                                    table2_col='employeeNumber')
        self.assertEqual(len(rows), 122)
        for i in rows:
            self.assertEqual(len(i), 3)

    def test_select_join_right(self):
        self.sql.change_db('testing_models')
        cols = [
            ('employees', 'email'),
            ('employees', 'jobTitle'),
            ('customers', 'customerName'),
        ]
        rows = self.sql.select_join('customers', 'employees', cols, table1_col='salesRepEmployeeNumber',
                                    table2_col='employeeNumber', join_type='RIGHT JOIN')
        self.assertEqual(len(rows), 108)
        for i in rows:
            self.assertEqual(len(i), 3)

    def test_select_join_inner(self):
        self.sql.change_db('testing_models')
        cols = [
            ('employees', 'email'),
            ('employees', 'jobTitle'),
            ('customers', 'customerName'),
        ]
        rows = self.sql.select_join('customers', 'employees', cols, table1_col='salesRepEmployeeNumber',
                                    table2_col='employeeNumber', join_type='INNER')
        self.assertEqual(len(rows), 100)
        for i in rows:
            self.assertEqual(len(i), 3)


if __name__ == '__main__':
    unittest.main()
