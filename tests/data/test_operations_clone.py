import os
import shutil
import unittest
from looptools import Timer
from mysql.toolkit import MySQL


TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
FAILS_DIR = os.path.join(os.path.dirname(__file__), 'data', 'fails')


class TestOperationsClone(unittest.TestCase):
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
        cls.sql = MySQL(config, auto_reconnect=False)
        cls.src, cls.dst = 'testing_employees', 'testing_models'

    @classmethod
    def tearDownClass(cls):
        cls.sql.change_db('testing_models')
        cls.sql.truncate_database('testing_models')

        print('Restoring Original testing_employees database')
        sql_scripts = [os.path.join(TEST_DATA_DIR, script) for script in os.listdir(TEST_DATA_DIR)
                       if script.endswith('.sql')]
        for s in sql_scripts:
            cls.sql.execute_script(s)
        cls.sql.disconnect()
        if os.path.exists(FAILS_DIR):
            shutil.rmtree(FAILS_DIR)

    @Timer.decorator
    def test_clone_slow_serverside(self):
        self.sql.truncate_database('testing_models')
        self.sql.change_db(self.src)

        self.sql.copy_database_slow(self.src, self.dst, optimized=True)

        self.sql.change_db(self.src)
        self.src_rows = self.sql.count_rows_all()
        self.src_pks = [self.sql.get_primary_key(tbl) for tbl in self.sql.tables]

        self.sql.change_db(self.dst)
        self.dst_rows = self.sql.count_rows_all()
        self.dst_pks = [self.sql.get_primary_key(tbl) for tbl in self.sql.tables]

        self.assertEqual(self.src_rows, self.dst_rows)
        self.assertEqual(self.src_pks, self.dst_pks)

    @Timer.decorator
    def test_clone_slow_clientside(self):
        self.sql.truncate_database('testing_models')
        self.sql.change_db(self.src)

        self.sql.copy_database_slow(self.src, self.dst, optimized=False)

        self.sql.change_db(self.src)
        self.src_rows = self.sql.count_rows_all()
        self.src_pks = [self.sql.get_primary_key(tbl) for tbl in self.sql.tables]

        self.sql.change_db(self.dst)
        self.dst_rows = self.sql.count_rows_all()
        self.dst_pks = [self.sql.get_primary_key(tbl) for tbl in self.sql.tables]

        self.assertEqual(self.src_rows, self.dst_rows)
        self.assertEqual(self.src_pks, self.dst_pks)

    @Timer.decorator
    def test_clone_onequery(self):
        self.sql.truncate_database('testing_models')
        self.sql.change_db(self.src)

        self.sql.copy_database(self.src, self.dst)

        self.sql.change_db(self.src)
        self.src_rows = self.sql.count_rows_all()
        self.src_pks = [self.sql.get_primary_key(tbl) for tbl in self.sql.tables]

        self.sql.change_db(self.dst)
        self.dst_rows = self.sql.count_rows_all()
        self.dst_pks = [self.sql.get_primary_key(tbl) for tbl in self.sql.tables]

        self.assertEqual(self.src_rows, self.dst_rows)
        self.assertEqual(self.src_pks, self.dst_pks)


if __name__ == '__main__':
    unittest.main()
