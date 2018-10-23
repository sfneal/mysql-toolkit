import unittest
from datetime import datetime
from mysql.toolkit.datatypes import Record


class TestDataTypes(unittest.TestCase):
    def test_num_tinyint(self):
        value = 14
        dt = Record(value).datatype
        self.assertEqual(dt, 'TINYINT (2)')

    def test_num_mediumint(self):
        value = 4006
        dt = Record(value).datatype
        self.assertEqual(dt, 'MEDIUMINT (4)')

    def test_num_bigint(self):
        value = 53147483647
        dt = Record(value).datatype
        self.assertEqual(dt, 'BIGINT (11)')

    def test_num_int(self):
        value = 21474836
        dt = Record(value).datatype
        self.assertEqual(dt, 'INT (8)')

    def test_num_decimal(self):
        value = 362.45
        dt = Record(value).datatype
        self.assertEqual(dt, 'DECIMAL (3, 2)')

    def test_text_varchar(self):
        value = '''Hey I'm a varchar string! Hey I'm a varchar string! Hey I'm a varchar string! Hey I'm a varchar
        string! Hey I'm a varchar string! Hey I'm a varchar string! Hey I'm a varchar string! Hey I'm a varchar
        string! Hey I'm a varchar string! Hey I'm a varchar string!!!!'''
        dt = Record(value).datatype
        self.assertEqual(dt, 'VARCHAR (278)')

    def test_text_tinytext(self):
        value = "Hey I'm a varchar string!"
        dt = Record(value).datatype
        self.assertEqual(dt, 'TINYTEXT (25)')

    def test_date_date(self):
        value = datetime.now().date()
        dt = Record(value).datatype
        self.assertEqual(dt, 'DATE')

    def test_date_datetime(self):
        value = datetime.now()
        dt = Record(value).datatype
        self.assertEqual(dt, 'DATETIME')

    def test_date_time(self):
        value = datetime.now().time()
        dt = Record(value).datatype
        self.assertEqual(dt, 'TIME')

    def test_date_year(self):
        value = datetime.now().year
        dt = Record(value).datatype
        self.assertEqual(dt, 'YEAR')


if __name__ == '__main__':
    unittest.main()
