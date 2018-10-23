import unittest
from mysql.toolkit import MySQL
from tests import config


class TestManipulateInsert(unittest.TestCase):
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
        table = 'departments'
        cls.sql.truncate(table)
        cls.sql.insert(table, cls.sql.get_columns(table), cls.original_data)
        cls.sql.disconnect()

    def tearDown(self):
        table = 'departments'
        self.sql.truncate(table)
        self.sql.insert(table, self.sql.get_columns(table), self.original_data)

    def test_insert_uniques(self):
        table = 'departments'
        cols = ['dept_no', 'dept_name']
        vals = [
            ['d009', 'Customer Service'],
            ['d002', 'Finance'],
            ['d010', 'Information Technology'],
            ['d011', 'Software Development'],
        ]

        self.sql.insert_uniques(table, cols, vals)
        self.assertEqual(len(self.sql.select_all(table)), 11)

    def test_insert(self):
        table = 'departments'
        cols = ['dept_no', 'dept_name']
        vals = [
            ['d010', 'Information Technology'],
            ['d011', 'Software Development'],
        ]

        self.sql.insert(table, cols, vals)
        self.assertEqual(len(self.sql.select_all(table)), 11)

    def test_insert_many(self):
        table = 'departments'
        cols = ['dept_no', 'dept_name']
        vals = [
            ['d010', 'Information Technology'],
            ['d011', 'Software Development'],
            ['d012', 'Database Admin'],
        ]

        self.sql.insert_many(table, cols, vals)
        self.assertEqual(len(self.sql.select_all(table)), 12)


if __name__ == '__main__':
    unittest.main()
