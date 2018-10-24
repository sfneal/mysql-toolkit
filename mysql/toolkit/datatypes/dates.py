from mysql.toolkit.datatypes._constants import DATA_TYPES


# MySQL accepted datetime ranges
YEARS = list(range(1000, 9999))
MONTHS = list(range(1, 13))
DAYS = list(range(1, 32))
HOURS = list(range(-838, 838))
MINUTES = list(range(1, 60))
SECONDS = list(range(0, 60))


class Dates:
    def __init__(self, data):
        self.data = data
        self.type = None
        self.len = len(self.data)

    def is_date(self):
        """Determine if a data record is of type DATE."""
        dt = DATA_TYPES['date']
        if type(self.data) is dt['type'] and '-' in str(self.data) and str(self.data).count('-') == 2:
            # Separate year, month and day
            date_split = str(self.data).split('-')
            y, m, d = date_split[0], date_split[1], date_split[2]

            # Validate values
            valid_year, valid_months, valid_days = int(y) in YEARS, int(m) in MONTHS, int(d) in DAYS

            # Check that all validations are True
            if all(i is True for i in (valid_year, valid_months, valid_days)):
                self.type = 'date'.upper()
                self.len = None
                return True

    def is_datetime(self):
        """Determine if a data record is of type DATETIME."""
        return self._is_date_data('datetime')

    def is_time(self):
        """Determine if a data record is of type TIME."""
        dt = DATA_TYPES['time']
        if type(self.data) is dt['type'] and ':' in str(self.data) and str(self.data).count(':') == 2:
            # Separate hour, month, second
            date_split = str(self.data).split(':')
            h, m, s = date_split[0], date_split[1], date_split[2]

            # Validate values
            valid_hour, valid_min, valid_sec = int(h) in HOURS, int(m) in MINUTES, int(float(s)) in SECONDS

            if all(i is True for i in (valid_hour, valid_min, valid_sec)):
                self.type = 'time'.upper()
                self.len = None
                return True

    def is_year(self):
        """Determine if a data record is of type YEAR."""
        dt = DATA_TYPES['year']
        if dt['min'] and dt['max']:
            if type(self.data) is dt['type'] and dt['min'] < self.data < dt['max']:
                self.type = 'year'.upper()
                self.len = None
                return True

    def _is_date_data(self, data_type):
        """Private method for determining if a data record is of type DATE."""
        dt = DATA_TYPES[data_type]
        if isinstance(self.data, dt['type']):
            self.type = data_type.upper()
            self.len = None
            return True
