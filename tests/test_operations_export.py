import os
import shutil
import unittest
from looptools import Timer
from mysql.toolkit import MySQL
from tests import config


class TestOperationsCreate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = MySQL(config('testing_models'))

    @classmethod
    def tearDownClass(cls):
        cls.sql.disconnect()

    @Timer.decorator
    def test_dump_table(self):
        ct = self.sql.dump_table('employees')

    @Timer.decorator
    def test_dump_db(self):
        ct = self.sql.dump_database()


if __name__ == '__main__':
    unittest.main()
