import unittest
from datetime import datetime
from mysql.toolkit.datatypes import Record


NOW = datetime.now()


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

    def test_num_float(self):
        value = 362.45
        dt = Record(value).datatype
        self.assertEqual(dt, 'FLOAT (3, 2)')

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
        value = NOW.date()
        dt = Record(value).datatype
        self.assertEqual(dt, 'DATE')

    def test_date_datetime(self):
        value = NOW
        dt = Record(value).datatype
        self.assertEqual(dt, 'DATETIME')

    def test_date_time(self):
        value = NOW.time()
        dt = Record(value).datatype
        self.assertEqual(dt, 'TIME')

    def test_date_year(self):
        value = NOW.year
        dt = Record(value).datatype
        self.assertEqual(dt, 'YEAR')


if __name__ == '__main__':
    unittest.main()
