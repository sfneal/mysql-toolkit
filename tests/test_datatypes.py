import unittest
from datetime import datetime
from looptools import Timer
from mysql.toolkit.datatypes import ValueType


class TestDataTypes(unittest.TestCase):
    @Timer.decorator
    def test_num_tinyint(self):
        value = 14
        dt = ValueType(value).sql
        self.assertEqual(dt, 'TINYINT (2)')

    @Timer.decorator
    def test_num_smallint(self):
        value = 4006
        dt = ValueType(value).sql
        self.assertEqual(dt, 'SMALLINT (4)')

    @Timer.decorator
    def test_num_mediumint(self):
        value = 42767
        dt = ValueType(value).sql
        self.assertEqual(dt, 'MEDIUMINT (5)')

    @Timer.decorator
    def test_num_bigint(self):
        value = 53147483647
        dt = ValueType(value).sql
        self.assertEqual(dt, 'BIGINT (11)')

    @Timer.decorator
    def test_num_int(self):
        value = 21474836
        dt = ValueType(value).sql
        self.assertEqual(dt, 'INT (8)')

    @Timer.decorator
    def test_num_decimal(self):
        value = 362.45
        dt = ValueType(value).sql
        self.assertEqual(dt, 'DECIMAL (3, 2)')

    @Timer.decorator
    def test_text_varchar(self):
        value = '''Hey I'm a varchar string! Hey I'm a varchar string! Hey I'm a varchar string! Hey I'm a varchar
        string! Hey I'm a varchar string! Hey I'm a varchar string! Hey I'm a varchar string! Hey I'm a varchar
        string! Hey I'm a varchar string! Hey I'm a varchar string!!!!'''
        dt = ValueType(value).sql
        self.assertEqual(dt, 'VARCHAR (278)')

    @Timer.decorator
    def test_text_tinytext(self):
        value = "Hey I'm a varchar string!"
        dt = ValueType(value).sql
        self.assertEqual(dt, 'TINYTEXT (25)')

    @Timer.decorator
    def test_date_date(self):
        value = datetime.now().date()
        dt = ValueType(value).sql
        self.assertEqual(dt, 'DATE')

    @Timer.decorator
    def test_date_datetime(self):
        value = datetime.now()
        dt = ValueType(value).sql
        self.assertEqual(dt, 'DATETIME')

    @Timer.decorator
    def test_date_time(self):
        value = datetime.now().time()
        dt = ValueType(value).sql
        self.assertEqual(dt, 'TIME')

    @Timer.decorator
    def test_date_year(self):
        value = datetime.now().year
        dt = ValueType(value).sql
        self.assertEqual(dt, 'YEAR')


if __name__ == '__main__':
    unittest.main()
