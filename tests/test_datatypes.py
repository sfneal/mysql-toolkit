import unittest
from mysql.toolkit.datatypes import Record


class TestDataTypes(unittest.TestCase):
    def test_int_tiny(self):
        value = 14
        dt = Record(value).datatype
        self.assertEqual(dt, 'TINYINT (2)')

    def test_int_medium(self):
        value = 4006
        dt = Record(value).datatype
        self.assertEqual(dt, 'MEDIUMINT (4)')

    def test_int_big(self):
        value = 53147483647
        dt = Record(value).datatype
        self.assertEqual(dt, 'BIGINT (11)')

    def test_int(self):
        value = 21474836
        dt = Record(value).datatype
        self.assertEqual(dt, 'INT (8)')

    def test_float(self):
        value = 362.45
        dt = Record(value).datatype
        self.assertEqual(dt, 'FLOAT (3, 2)')

    def test_varchar(self):
        value = "Hey I'm a varchar string!"
        dt = Record(value).datatype
        self.assertEqual(dt, 'VARCHAR (25)')


if __name__ == '__main__':
    unittest.main()
