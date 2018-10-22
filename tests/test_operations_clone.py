import os
import shutil
import unittest
from mysql.toolkit import MySQL
from tests.data.employees import main as employees_db_restore


TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
SQL_SCRIPT = os.path.join(TEST_DATA_DIR, 'models.sql')
FAILS_DIR = os.path.join(os.path.dirname(__file__), 'data', 'fails')


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
        cls.sql.change_db('testing_models')
        cls.sql.truncate_database('testing_models')
        cls.sql.execute_script(SQL_SCRIPT)

        print('Restoring Original testing_employees database')
        sql_scripts = [os.path.join(TEST_DATA_DIR, script) for script in os.listdir(TEST_DATA_DIR)
                       if script.endswith('.sql')]
        for s in sql_scripts:
            cls.sql.execute_script(s)
        cls.sql.disconnect()
        if os.path.exists(FAILS_DIR):
            shutil.rmtree(FAILS_DIR)

    def tearDown(self):
        self.sql.change_db('testing_models')
        self.sql.truncate_database('testing_models')

    def test_clone_standard(self):
        src, dst = 'testing_employees', 'testing_models'
        self.sql.truncate_database('testing_models')
        self.sql.change_db(src)

        self.sql.copy_database(src, dst)

        self.sql.change_db(src)
        src_rows = self.sql.count_rows_all()
        src_pks = [self.sql.get_primary_key_vals(tbl) for tbl in self.sql.tables]

        self.sql.change_db(dst)
        dst_rows = self.sql.count_rows_all()
        dst_pks = [self.sql.get_primary_key_vals(tbl) for tbl in self.sql.tables]

        self.assertEqual(src_rows, dst_rows)
        self.assertEqual(src_pks, dst_pks)

    def test_clone_optimized(self):
        src, dst = 'testing_employees', 'testing_models'
        self.sql.truncate_database('testing_models')
        self.sql.change_db(src)

        self.sql.copy_database(src, dst, optimized=True)

        self.sql.change_db(src)
        src_rows = self.sql.count_rows_all()
        src_pks = [self.sql.get_primary_key_vals(tbl) for tbl in self.sql.tables]

        self.sql.change_db(dst)
        dst_rows = self.sql.count_rows_all()
        dst_pks = [self.sql.get_primary_key_vals(tbl) for tbl in self.sql.tables]

        self.assertEqual(src_rows, dst_rows)
        self.assertEqual(src_pks, dst_pks)

    def test_clone_onequery(self):
        src, dst = 'testing_employees', 'testing_models'
        self.sql.truncate_database('testing_models')
        self.sql.change_db(src)

        self.sql.copy_database(src, dst, one_query=True)

        self.sql.change_db(src)
        src_rows = self.sql.count_rows_all()
        src_pks = [self.sql.get_primary_key_vals(tbl) for tbl in self.sql.tables]

        self.sql.change_db(dst)
        dst_rows = self.sql.count_rows_all()
        dst_pks = [self.sql.get_primary_key_vals(tbl) for tbl in self.sql.tables]

        self.assertEqual(src_rows, dst_rows)
        self.assertEqual(src_pks, dst_pks)


if __name__ == '__main__':
    unittest.main()
