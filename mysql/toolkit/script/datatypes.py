DATA_TYPES = {
    # Text Data Types
    'varchar': {'type': str, 'max': 255},
    'tinytext': {'type': str, 'max': 255},
    'text': {'type': str, 'max': 65535},
    'mediumtext': {'type': str, 'max': 16777215},
    'longtext': {'type': str, 'max': 4294967295},

    # Numeric Data Types
    'tinyint': {'type': int, 'min': -128, 'max': 127},
    'smallint': {'type': int, 'min': -32768, 'max': 32767},
    'mediumint': {'type': int, 'min': -8388608, 'max': 8388607},
    'int': {'type': int, 'min': -2147483648, 'max': 2147483647},
    'bigint': {'type': int, 'min': -2147483648, 'max': 2147483647},
    'float': {'type': float},
}


class Text:
    def __init__(self, data):
        self.data = data
        self.type = None
        self.len = len(self.data)

    def is_varchar(self):
        """Determine if a data record is of the type VARCHAR."""
        dt = DATA_TYPES['varchar']
        if type(self.data) is dt['type'] and self.len < dt['max']:
            self.type = 'VARCHAR'
            return True

    def is_tinytext(self):
        """Determine if a data record is of the type VARCHAR."""
        return self._is_text_data('tinytext')

    def is_text(self):
        """Determine if a data record is of the type TEXT."""
        return self._is_text_data('text')

    def is_mediumtext(self):
        """Determine if a data record is of the type MEDIUMTEXT."""
        return self._is_text_data('mediumtext')

    def is_longtext(self):
        """Determine if a data record is of the type LONGTEXT."""
        return self._is_text_data('longtext')

    def _is_text_data(self, data_type):
        """Private method for testing text data types."""
        dt = DATA_TYPES[data_type]
        if type(self.data) is dt['type'] and self.len < dt['max'] and all(type(char) == str for char in self.data):
            self.type = data_type.uppercase
            return True


class Numeric:
    def __init__(self, data):
        self.data = data
        self.type = None
        self.len = len(self.data)

    def is_tinyint(self):
        """Determine if a data record is of the type TINYINT."""
        return self._is_numeric_data('tinyint')

    def is_mediumint(self):
        """Determine if a data record is of the type MEDIUMINT."""
        return self._is_numeric_data('mediumint')

    def is_int(self):
        """Determine if a data record is of the type INT."""
        return self._is_numeric_data('mediumint')

    def is_bigint(self):
        """Determine if a data record is of the type BIGINT."""
        return self._is_numeric_data('bigint')

    def _is_numeric_data(self, data_type):
        """Private method for testing text data types."""
        dt = DATA_TYPES[data_type]
        if type(self.data) is dt['type'] and dt['min'] < self.len < dt['max']:
            self.type = data_type.uppercase
            return True

    def is_float(self):
        """Determine if a data record is of the type float."""
        dt = DATA_TYPES['float']
        if type(self.data) is dt['type']:
            self.type = 'FLOAT'
            num_split = str(self.data).split('.', 1)
            self.len = '{0}, {1}'.format(len(num_split[0]), len(num_split[1]))
            return True


class Record(Text, Numeric):
    def __init__(self, data):
        super(Record, self).__init__(data)
        self.data = data
        self.type = None
        self.len = len(self.data)

    @property
    def datatype(self):
        return '{0} ({1})'.format(self.type, self.len)


class DataTypes:
    def __init__(self, data):
        self.record = Record(data)

    def varchar(self):
        """Retrieve the data type of a data record suspected to a VARCHAR."""
        return self.record.datatype if self.record.is_varchar() else False

    def text(self):
        """Retrieve the data type of a data record suspected to a VARCHAR."""
        return self.record.datatype if self.record.is_text() else False
