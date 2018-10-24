from mysql.toolkit.datatypes._constants import DATA_TYPES


class Text:
    def __init__(self, data):
        self.data = data
        self.type = None
        self.len = None

    def is_varchar(self):
        """Determine if a data record is of the type VARCHAR."""
        dt = DATA_TYPES['varchar']
        if type(self.data) is dt['type'] and len(self.data) < dt['max']:
            self.type = 'VARCHAR'
            self.len = len(self.data)
            return True

    def is_tinytext(self):
        """Determine if a data record is of the type VARCHAR."""
        return self._is_text_data('tinytext')

    def is_mediumtext(self):
        """Determine if a data record is of the type MEDIUMTEXT."""
        return self._is_text_data('mediumtext')

    def is_longtext(self):
        """Determine if a data record is of the type LONGTEXT."""
        return self._is_text_data('longtext')

    def _is_text_data(self, data_type):
        """Private method for testing text data types."""
        dt = DATA_TYPES[data_type]
        if type(self.data) is dt['type'] and len(self.data) < dt['max'] and all(type(char) == str for char in self.data):
            self.type = data_type.upper()
            self.len = len(self.data)
            return True
