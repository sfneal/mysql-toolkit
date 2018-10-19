DATA_TYPES = {
    'varchar': {'type': str, 'max': 255},
    'tinytext': {'type': str, 'max': 255},
    'text': {'type': str, 'max': 65535},
    'mediumtext': {'type': str, 'max': 16777215},
    'longtext': {'type': str, 'max': 4294967295},
}


class Record:
    def __init__(self, data):
        self.data = data
        self.type = None
        self.max = len(self.data)

    @property
    def datatype(self):
        return '{0} ({1})'.format(self.type, self.max)

    def is_varchar(self):
        """Determine if a data record is of the type VARCHAR."""
        dt = DATA_TYPES['varchar']
        if type(self.data) is dt['type'] and self.max < dt['max']:
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
        if type(self.data) is dt['type'] and self.max < dt['max'] and all(type(char) == str for char in self.data):
            self.type = data_type.uppercase
            return True


class DataTypes:
    def __init__(self, data):
        self.record = Record(data)

    def varchar(self):
        """Retrieve the data type of a data record suspected to a VARCHAR."""
        return self.record.datatype if self.record.is_varchar() else False

    def text(self):
        """Retrieve the data type of a data record suspected to a VARCHAR."""
        return self.record.datatype if self.record.is_text() else False
