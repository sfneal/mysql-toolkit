import unittest
from looptools import Timer
from mysql.toolkit import MySQL
from tests import config


class TestManipulateSelect(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = MySQL(config())

    @classmethod
    def tearDownClass(cls):
        cls.sql.disconnect()

    def tearDown(self):
        self.sql.change_db('testing_employees')

    @Timer.decorator
    def test_select_all(self):
        table = 'dept_manager'

        rows = self.sql.select_all(table)
        self.assertEqual(len(rows), self.sql.count_rows(table))
        self.assertEqual(len(rows[0]), len(self.sql.get_columns(table)))

    @Timer.decorator
    def test_select(self):
        table = 'employees'
        cols = ['emp_no', 'birth_date', 'first_name', 'last_name']

        rows = self.sql.select(table, cols)
        self.assertEqual(len(rows), self.sql.count_rows(table))
        self.assertEqual(len(rows[0]), len(cols))

    @Timer.decorator
    def test_select_distinct(self):
        table = 'departments'
        cols = ['dept_name']

        rows = self.sql.select_distinct(table, cols)
        self.assertEqual(len(rows), self.sql.count_rows(table))

    @Timer.decorator
    def test_select_limit(self):
        table = 'salaries'
        limit = 22125

        rows = self.sql.select_limit(table, limit=limit)
        self.assertEqual(len(rows), 22125)

    @Timer.decorator
    def test_select_where(self):
        table = 'titles'
        cols = ['title']

        row = self.sql.select_where(table, cols, ('emp_no', 10001))
        self.assertEqual(row, 'Senior Engineer')

    @Timer.decorator
    def test_select_where_less(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where('payments', ['customerNumber'], ('customerNumber', '<', 127))
        self.assertEqual(len(rows), 26)

    @Timer.decorator
    def test_select_where_more(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where('payments', ['customerNumber'], ('customerNumber', '>', 127))
        self.assertEqual(len(rows), 247)

    @Timer.decorator
    def test_select_where_between(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where_between('payments', ['amount'], 'amount', (1500, 10000))
        self.assertEqual(len(rows), 39)

    @Timer.decorator
    def test_select_where_like_1(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where_like('customers', ['customerName'], 'customerName', start='a', end='.',
                                          anywhere='&')
        self.assertEqual(len(rows), 2)

    @Timer.decorator
    def test_select_where_like_2(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where_like('customers', ['customerName'], 'customerName', start='c', end=None,
                                          anywhere='collect', index=(None, None), length=None)
        self.assertEqual(len(rows), 2)

    @Timer.decorator
    def test_select_where_like_3(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where_like('customers', ['customerName'], 'customerName', start=None, end='ltd',
                                          anywhere='o', index=(None, None), length=None)
        self.assertEqual(len(rows), 8)

    @Timer.decorator
    def test_select_where_like_4(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where_like('customers', ['customerName'], 'customerName', start='mini', end=None,
                                          anywhere=None, index=(None, None), length=None)
        self.assertEqual(len(rows), 6)

    @Timer.decorator
    def test_select_where_like_5(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where_like('customers', ['customerName'], 'customerName', start=None, end='co.',
                                          anywhere=None, index=(None, None), length=None)
        self.assertEqual(len(rows), 26)

    @Timer.decorator
    def test_select_where_like_6(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where_like('customers', ['customerName'], 'customerName', start=None, end=None,
                                          anywhere='imports', index=(None, None), length=None)
        self.assertEqual(len(rows), 9)

    @Timer.decorator
    def test_select_where_like_7(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where_like('customers', ['customerName'], 'customerName', start=None, end=None,
                                          anywhere=None, index=(1, "i"), length=None)
        self.assertEqual(len(rows), 5)

    @Timer.decorator
    def test_select_where_like_8(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where_like('customers', ['customerName'], 'customerName', start=None, end=None,
                                          anywhere=None, index=(None, None), length=20)
        self.assertEqual(len(rows), 68)

    @Timer.decorator
    def test_select_where_multi_clause(self):
        self.sql.change_db('testing_models')
        rows = self.sql.select_where('customers', 'customerName', [('country', 'USA'), ('postalCode', 97562)])
        self.assertEqual(len(rows), 2)

    @Timer.decorator
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

    @Timer.decorator
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

    @Timer.decorator
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

    @Timer.decorator
    def test_select_return_dict(self):
        self.sql.change_db('testing_models')
        cols = ['email', 'jobTitle']
        rows = self.sql.select('employees', cols, return_type=dict)
        self.assertEqual(len(rows), 23)
        for row in rows:
            self.assertEqual(type(row), dict)

    @Timer.decorator
    def test_select_where_not_null(self):
        self.sql.change_db('testing_models')
        tbl = 'customers'
        cols = ['addressLine1', 'city', 'state']
        rows1 = self.sql.select_where(tbl, cols, ('state', not None))
        rows2 = self.sql.select_where(tbl, cols, ('state', True))

        self.assertEqual(49, len(rows1))
        self.assertEqual(49, len(rows2))

    @Timer.decorator
    def test_select_where_null(self):
        self.sql.change_db('testing_models')
        tbl = 'customers'
        cols = ['addressLine1', 'city', 'state']
        rows1 = self.sql.select_where(tbl, cols, ('state', None))
        rows2 = self.sql.select_where(tbl, cols, ('state', False))

        self.assertEqual(73, len(rows1))
        self.assertEqual(73, len(rows2))

    @Timer.decorator
    def test_select_order(self):
        tbl = 'departments'
        cols = '*'
        rows = self.sql.select(tbl, cols, order_by='dept_name')
        for r in rows:
            print(r)


if __name__ == '__main__':
    unittest.main()
