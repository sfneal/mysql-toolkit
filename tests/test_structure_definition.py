import unittest
from mysql.toolkit import MySQL
from tests import config
from looptools import Timer


class TestStructureDefinition(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = MySQL(config('testing_models'))

    @classmethod
    def tearDownClass(cls):
        cls.sql.disconnect()

    @Timer.decorator
    def test_get_table_definition(self):
        table = 'customers'
        td = self.sql.get_table_definition(table)
        self.assertEqual(781, len(td))

    @Timer.decorator
    def test_get_column_definition_all(self):
        definitions = ['`productCode` varchar(15) NOT NULL',
                       '`productName` varchar(70) NOT NULL',
                       '`productLine` varchar(50) NOT NULL',
                       '`productScale` varchar(10) NOT NULL',
                       '`productVendor` varchar(50) NOT NULL',
                       '`productDescription` text NOT NULL',
                       '`quantityInStock` smallint(6) NOT NULL',
                       '`buyPrice` decimal(10, 2) NOT NULL',
                       '`MSRP` decimal(10, 2) NOT NULL']
        table = 'products'
        td = self.sql.get_column_definition_all(table)
        for i in range(0, len(td)):
            self.assertEqual(definitions[i], td[i])
        self.assertEqual(len(td), 9)

    @Timer.decorator
    def test_get_column_definition(self):
        table = 'offices'
        col = 'addressLine1'
        td = self.sql.get_column_definition(table, col)
        self.assertEqual(35, len(td))


if __name__ == '__main__':
    unittest.main()
