import unittest
from mysql.toolkit import MySQL
from tests import config


class TestStructureKeys(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = MySQL(config())

    @classmethod
    def tearDownClass(cls):
        cls.sql.disconnect()

    def test_get_primary_key(self):
        table = 'offices'
        pk = self.sql.get_primary_key(table)
        self.assertEqual(pk, 'officeCode')

    def test_get_primary_key_vals(self):
        table = 'offices'
        pk_vals = self.sql.get_primary_key_vals(table)
        self.assertEqual(pk_vals, list(map(str, list(range(1, 8)))))


if __name__ == '__main__':
    unittest.main()
