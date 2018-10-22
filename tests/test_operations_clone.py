import os
import shutil
import unittest
from differentiate import diff
from mysql.toolkit import MySQL


SQL_SCRIPT = os.path.join(os.path.dirname(__file__), 'data', 'mysqlsampledatabase.sql')
FAILS_DIR = os.path.join(os.path.dirname(__file__), 'data', 'fails')


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
        if os.path.exists(FAILS_DIR):
            shutil.rmtree(FAILS_DIR)
        cls.sql.disconnect()

    def tearDown(self):
        self.sql.change_db('toolkit_testing_cm')
        self.sql.truncate_database()
        self.sql.execute_script(SQL_SCRIPT)

    def test_clone_standard(self):
        src, dst = 'toolkit_testing', 'toolkit_testing_cm'
        self.sql.truncate_database('toolkit_testing_cm')
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
        src, dst = 'toolkit_testing', 'toolkit_testing_cm'
        self.sql.truncate_database('toolkit_testing_cm')
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
        src, dst = 'toolkit_testing', 'toolkit_testing_cm'
        self.sql.truncate_database('toolkit_testing_cm')
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
