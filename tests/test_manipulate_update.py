import unittest
from differentiate import diff
from mysql.toolkit import MySQL
from tests import config


class TestManipulateUpdate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = MySQL(config())
        if cls.sql.count_rows('departments') == 0:
            cls.sql.execute('''
                INSERT INTO `departments` 
                VALUES 
                    ('d001','Marketing'),
                    ('d002','Finance'),
                    ('d003','Human Resources'),
                    ('d004','Production'),
                    ('d005','Development'),
                    ('d006','Quality Management'),
                    ('d007','Sales'),
                    ('d008','Research'),
                    ('d009','Customer Service');
            ''')
        cls.original_data = cls.sql.select_all('departments')

    @classmethod
    def tearDownClass(cls):
        cls.sql.disconnect()

    def tearDown(self):
        table = 'departments'
        self.sql.truncate(table)
        self.sql.insert(table, self.sql.get_columns(table), self.original_data)

    def test_update(self):
        table = 'departments'
        cols = ['dept_name']
        vals = ['Stock Trading']
        where = ('dept_no', 'd002')

        existing_rows = self.sql.select_all(table)
        self.sql.update(table, cols, vals, where)
        new_rows = self.sql.select_all(table)
        changed_rows = diff(existing_rows, new_rows)

        self.assertEqual(len(changed_rows), 2)
        self.assertEqual(len(self.sql.select_all(table)), 9)

    def test_update_many(self):
        table = 'departments'
        cols = ['dept_name']
        vals = [
            ['d002', 'Stock Trading'],
            ['d003', 'Client Resources'],
        ]
        where_col = 'dept_no'
        where_index = 0

        existing_rows = self.sql.select_all(table)
        self.sql.update_many(table, cols, vals, where_col, where_index)
        new_rows = self.sql.select_all(table)
        changed_rows = diff(existing_rows, new_rows, y_only=True)

        self.assertEqual(len(changed_rows), 2)
        self.assertEqual(len(self.sql.select_all(table)), 9)


if __name__ == '__main__':
    unittest.main()
