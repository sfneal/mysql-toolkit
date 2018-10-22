import os
import shutil
import unittest
from differentiate import diff
from mysql.toolkit import MySQL


class TestOperationsRemove(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = {
            "database": "toolkit_testing_cm",
            "host": "stephenneal.net",
            "password": "Stealth19!",
            "port": 3306,
            "raise_on_warnings": True,
            "user": "stephen_master"
        }
        cls.sql = MySQL(config)

    @classmethod
    def tearDownClass(cls):
        fails_dir = os.path.join(os.path.dirname(__file__), 'data', 'fails')
        if os.path.exists(fails_dir):
            shutil.rmtree(fails_dir)
        cls.sql.disconnect()

    def tearDown(self):
        self.sql.truncate_database()
        self.sql.execute_script(os.path.join(os.path.dirname(__file__), 'data', 'mysqlsampledatabase.sql'))

    def test_truncate(self):
        table = 'orders'
        self.sql.truncate(table)
        self.assertEqual(self.sql.count_rows(table), 0)

    def test_truncate_database(self):
        table = 'orders'
        self.sql.truncate_database(table)
        self.assertEqual(self.sql.tables, 0)

    def test_drop(self):
        tables = ['orders', 'payments']
        existing = self.sql.tables
        self.sql.drop(tables)
        modified = self.sql.tables
        difference = diff(existing, modified)

        self.assertEqual(len(difference), 2)


if __name__ == '__main__':
    unittest.main()
