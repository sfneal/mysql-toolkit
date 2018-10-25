import unittest
from mysql.toolkit import MySQL
from tests import config
from looptools import Timer


class TestStructureKeys(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = MySQL(config('testing_models'))

    @classmethod
    def tearDownClass(cls):
        cls.sql.disconnect()

    @Timer.decorator
    def test_get_primary_key(self):
        table = 'offices'
        pk = self.sql.get_primary_key(table)
        self.assertEqual(pk, 'officeCode')

    @Timer.decorator
    def test_get_primary_key_vals(self):
        table = 'offices'
        pk_vals = self.sql.get_primary_key_vals(table)
        self.assertEqual(pk_vals, list(map(str, list(range(1, 8)))))

    @Timer.decorator
    def test_set_primary_keys_all(self):
        self.sql.change_db('testing_employees')
        table = 'employees'
        create_table = self.sql.get_table_definition(table)
        cols = self.sql.get_columns(table)
        data = self.sql.select_all(table)

        self.sql.drop_primary_key(table)
        actions = self.sql.set_primary_keys_all([table])
        self.assertEqual(create_table, self.sql.get_table_definition(table))
        self.assertEqual(actions['new_pk'][0], 'emp_no')
        self.assertEqual(len(actions['new_pk']), 1)

        self.sql.drop(table)
        self.sql.execute(create_table)
        self.sql.insert_many(table, cols, data)
        self.sql.change_db('testing_models')

    @Timer.decorator
    def test_drop_primary_key(self):
        self.sql.change_db('testing_employees')
        table = 'employees'
        pk = self.sql.get_primary_key(table)
        self.sql.drop_primary_key(table)
        self.assertEqual(None, self.sql.get_primary_key(table))
        self.sql.set_primary_key(table, pk)
        self.assertEqual(pk, self.sql.get_primary_key(table))
        self.sql.change_db('testing_models')


if __name__ == '__main__':
    unittest.main()
