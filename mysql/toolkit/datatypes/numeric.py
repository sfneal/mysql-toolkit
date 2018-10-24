from mysql.toolkit.datatypes._constants import DATA_TYPES


class Numeric:
    def __init__(self, data):
        self.data = data
        self.type = None
        self.len = None
        self.len_decimal = None

    def is_tinyint(self):
        """Determine if a data record is of the type TINYINT."""
        return self._is_numeric_data('tinyint')

    def is_smallint(self):
        """Determine if a data record is of the type SMALLINT."""
        return self._is_numeric_data('smallint')

    def is_mediumint(self):
        """Determine if a data record is of the type MEDIUMINT."""
        return self._is_numeric_data('mediumint')

    def is_int(self):
        """Determine if a data record is of the type INT."""
        return self._is_numeric_data('int')

    def is_bigint(self):
        """Determine if a data record is of the type BIGINT."""
        return self._is_numeric_data('bigint')

    def is_decimal(self):
        """Determine if a data record is of the type float."""
        dt = DATA_TYPES['decimal']
        if type(self.data) in dt['type']:
            self.type = 'DECIMAL'
            num_split = str(self.data).split('.', 1)
            self.len = len(num_split[0])
            self.len_decimal = len(num_split[1])
            return True

    def _is_numeric_data(self, data_type):
        """Private method for testing text data types."""
        dt = DATA_TYPES[data_type]
        if dt['min'] and dt['max']:
            if type(self.data) is dt['type'] and dt['min'] < self.data < dt['max']:
                self.type = data_type.upper()
                self.len = len(str(self.data))
                return True
