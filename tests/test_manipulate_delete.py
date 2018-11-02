import unittest
from looptools import Timer
from differentiate import diff
from mysql.toolkit import MySQL
from tests import config


class TestManipulateDelete(unittest.TestCase):
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
        self.sql.insert_many(table, self.sql.get_columns(table), self.original_data)

    @Timer.decorator
    def test_delete(self):
        table = 'departments'
        where = ('dept_no', 'd002')

        existing_rows = self.sql.select_all(table)
        self.sql.delete(table, where)
        new_rows = self.sql.select_all(table)
        changed_rows = diff(existing_rows, new_rows)

        self.assertEqual(len(changed_rows), 1)
        self.assertEqual(len(self.sql.select_all(table)), 8)

    @Timer.decorator
    def test_delete_many(self):
        table = 'departments'
        wheres = [('dept_no', 'd002'), ('dept_no', 'd003')]

        existing_rows = self.sql.select_all(table)
        self.sql.delete_many(table, wheres)
        new_rows = self.sql.select_all(table)
        changed_rows = diff(existing_rows, new_rows)

        self.assertEqual(len(changed_rows), 2)
        self.assertEqual(len(self.sql.select_all(table)), 7)

    @Timer.decorator
    def test_delete_many_singlecol(self):
        table = 'departments'

        existing_rows = self.sql.select_all(table)
        self.sql.delete_many(table, column='dept_no', values=['d002', 'd003'])
        new_rows = self.sql.select_all(table)
        changed_rows = diff(existing_rows, new_rows)

        self.assertEqual(len(changed_rows), 2)
        self.assertEqual(len(self.sql.select_all(table)), 7)

    @Timer.decorator
    def test_delete_except(self):
        table = 'departments'

        existing_rows = self.sql.select_all(table)
        self.sql.delete_except(table, column='dept_no', exceptions=['d002', 'd003'])
        new_rows = self.sql.select_all(table)
        changed_rows = diff(existing_rows, new_rows)

        self.assertEqual(len(changed_rows), 7)
        self.assertEqual(len(self.sql.select_all(table)), 2)


if __name__ == '__main__':
    unittest.main()
