from datetime import datetime
from operator import itemgetter


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

    # Date Data Types
    'date': {'type': str},
    'datetime': {'type': datetime},
    'timestamp': {'type': datetime.timestamp},
    'time': {'type': datetime.time},
    'year': {'type': int, 'min': 1901, 'max': 2155},
}

# MySQL accepted datetime ranges
YEARS = range(1000, 9999)
MONTHS = ['0{0}'.format(i) if len(str(i)) == 1 else i for i in range(1, 13)]
DAYS = ['0{0}'.format(i) if len(str(i)) == 1 else i for i in range(1, 32)]
HOURS = range(-838, 838)
MINUTES = ['0{0}'.format(i) if len(str(i)) == 1 else i for i in range(1, 60)]
SECONDS = ['0{0}'.format(i) if len(str(i)) == 1 else i for i in range(1, 60)]


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
            self.type = data_type.upper()
            self.len = len(self.data)
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

    def is_float(self):
        """Determine if a data record is of the type float."""
        dt = DATA_TYPES['float']
        if type(self.data) is dt['type']:
            self.type = 'FLOAT'
            num_split = str(self.data).split('.', 1)
            self.len = '{0}, {1}'.format(len(num_split[0]), len(num_split[1]))
            return True

    def _is_numeric_data(self, data_type):
        """Private method for testing text data types."""
        dt = DATA_TYPES[data_type]
        if dt['min'] and dt['max']:
            if type(self.data) is dt['type'] and dt['min'] < self.data < dt['max']:
                self.type = data_type.upper()
                self.len = len(str(self.data))
                return True


class Dates:
    def __init__(self, data):
        self.data = data
        self.type = None
        self.len = len(self.data)

    def is_date(self):
        """Determine if a data record is of type DATE."""
        dt = DATA_TYPES['date']
        if type(self.data) is dt['type'] and '-' in self.data and self.data.count('-') == 2:
            # Separate year, month and day
            date_split = self.data.split('-')
            y, m, d = date_split[0], date_split[1], date_split[2]

            # Validate values
            valid_year, valid_months, valid_days = y in YEARS, m in MONTHS, d in DAYS

            # Check that all validations are True
            if all(i is True for i in (valid_year, valid_months, valid_days)):
                self.type = 'date'.upper()
                self.len = len(str(self.data))
                return True

    def is_datetime(self):
        """Determine if a data record is of type DATETIME."""
        return self._is_date_data('datetime')

    def is_time(self):
        """Determine if a data record is of type TIME."""
        dt = DATA_TYPES['date']
        if type(self.data) is dt['type'] and ':' in self.data and self.data.count(':') == 2:
            # Separate hour, month, second
            date_split = self.data.split(':')
            h, m, s = date_split[0], date_split[1], date_split[2]

            # Validate values
            valid_hour, valid_min, valid_sec = h in HOURS, s in SECONDS, m in MINUTES

            if all(i is True for i in (valid_hour, valid_min, valid_sec)):
                self.type = 'time'.upper()
                self.len = len(str(self.data))
                return True

    def is_year(self):
        """Determine if a data record is of type YEAR."""
        dt = DATA_TYPES['year']
        if dt['min'] and dt['max']:
            if type(self.data) is dt['type'] and dt['min'] < self.data < dt['max']:
                self.type = 'year'.upper()
                self.len = len(str(self.data))
                return True

    def _is_date_data(self, data_type):
        """Private method for determining if a data record is of type DATE."""
        dt = DATA_TYPES[data_type]
        if isinstance(self.data, dt['type']):
            self.type = data_type.upper()
            self.len = None
            return True


class Record(Text, Numeric, Dates):
    def __init__(self, data):
        super(Record, self).__init__(data)
        self.data = data
        self.type = None
        self.len = None

    @property
    def datatype(self):
        if not self.type:
            self.get_type()

        if self.len:
            return '{0} ({1})'.format(self.type, self.len)
        else:
            return '{0}'.format(self.type)

    def get_type(self):
        """Retrieve the data type for a data record."""
        test_method = [
            self.is_tinyint,
            self.is_mediumint,
            self.is_int,
            self.is_bigint,
            self.is_float,
            self.is_date,
            self.is_datetime,
            self.is_time,
            self.is_year,
            self.is_varchar,
            self.is_tinytext,
            self.is_text,
            self.is_mediumtext,
            self.is_longtext,
        ]
        # Loop through test methods until a test returns True
        for method in test_method:
            if method():
                return self.datatype


class DataTypes:
    def __init__(self, data):
        self.record = Record(data)

    def varchar(self):
        """Retrieve the data type of a data record suspected to a VARCHAR."""
        return self.record.datatype if self.record.is_varchar() else False

    def text(self):
        """Retrieve the data type of a data record suspected to a VARCHAR."""
        return self.record.datatype if self.record.is_text() else False


def column_datatype(data_set):
    """
    Retrieve the best fit data type for a column of a MySQL table.

    Accepts a iterable of values ONLY for the column whose data type
    is in question.

    :param data_set: Iterable of values
    :return: data type
    """
    types = []
    type_len = []
    for record in data_set:
        r = Record(record)
        r.datatype
        type_len.append((r.type, r.len))
        types.append(r.type)
    types_count = {t: types.count(t) for t in set(types)}
    most_frequent = max(types_count.items(), key=itemgetter(1))[0]

    max_len = 0
    for t, l in type_len:
        if t == most_frequent and l > max_len:
            max_len = l
    return '{0} ({1})'.format(most_frequent, max_len)
