import unittest
from mysql.toolkit import MySQL
from differentiate import diff


class TestManipulateDelete(unittest.TestCase):
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
        where = ('dept_no', 'd002')

        existing_rows = self.sql.select_all(table)
        self.sql.delete(table, where)
        new_rows = self.sql.select_all(table)
        changed_rows = diff(existing_rows, new_rows)

        self.assertEqual(len(changed_rows), 1)
        self.assertEqual(len(self.sql.select_all(table)), 8)


if __name__ == '__main__':
    unittest.main()
