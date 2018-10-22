import os
import shutil
import unittest
from differentiate import diff
from mysql.toolkit import MySQL


SQL_SCRIPT = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'samples', 'models.sql')
FAILS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'samples', 'fails')


class TestOperationsRemove(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = {
            "database": "testing_models",
            "host": "stephenneal.net",
            "password": "thisisfortesting",
            "port": 3306,
            "raise_on_warnings": True,
            "user": "stephen_testing"
        }
        cls.sql = MySQL(config)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(FAILS_DIR):
            shutil.rmtree(FAILS_DIR)
        cls.sql.disconnect()

    def tearDown(self):
        self.sql.truncate_database()
        self.sql.execute_script(SQL_SCRIPT)

    def test_truncate(self):
        table = 'payments'
        self.sql.truncate(table)
        self.assertEqual(self.sql.count_rows(table), 0)

    def test_truncate_database(self):
        self.sql.truncate_database()
        self.assertEqual(len(self.sql.tables), 0)

    def test_drop(self):
        tables = ['orders', 'payments']
        existing = self.sql.tables
        self.sql.drop(tables)
        modified = self.sql.tables
        difference = diff(existing, modified)

        self.assertEqual(len(difference), 2)

    def test_drop_empty_tables(self):
        existing = self.sql.tables

        table = 'payments'
        self.sql.truncate(table)
        self.assertEqual(self.sql.count_rows(table), 0)
        self.sql.drop_empty_tables()

        modified = self.sql.tables
        difference = diff(existing, modified)
        self.assertEqual(len(difference), 1)


if __name__ == '__main__':
    unittest.main()
