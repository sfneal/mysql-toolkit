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
    def test_export_table(self):
        ct = self.sql.export_table('employees')


if __name__ == '__main__':
    unittest.main()
