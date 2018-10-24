from operator import itemgetter
from mysql.toolkit.datatypes.text import Text
from mysql.toolkit.datatypes.numeric import Numeric
from mysql.toolkit.datatypes.dates import Dates


class ValueType(Text, Numeric, Dates):
    def __init__(self, data):
        super(ValueType, self).__init__(data)
        self.data = data
        self.type = None
        self.len = None
        self.len_decimal = None

    @property
    def sql(self):
        if not self.type:
            self.get_sql()

        if self.len and self.len_decimal:
            return '{0} ({1}, {2})'.format(self.type, self.len, self.len_decimal)
        elif self.len:
            return '{0} ({1})'.format(self.type, self.len)
        else:
            return '{0}'.format(self.type)

    def get_sql(self):
        """Retrieve the data type for a data record."""
        test_method = [
            self.is_time,
            self.is_date,
            self.is_datetime,
            self.is_decimal,
            self.is_year,
            self.is_tinyint,
            self.is_smallint,
            self.is_mediumint,
            self.is_int,
            self.is_bigint,
            self.is_tinytext,
            self.is_varchar,
            self.is_mediumtext,
            self.is_longtext,
        ]
        # Loop through test methods until a test returns True
        for method in test_method:
            if method():
                return self.sql

    @property
    def get_type_len(self):
        """Retrieve the type and length for a data record."""
        # Check types and set type/len
        self.get_sql()
        return self.type, self.len, self.len_decimal


def sql_column_type(column_data, prefer_varchar=False, prefer_int=False):
    """
    Retrieve the best fit data type for a column of a MySQL table.

    Accepts a iterable of values ONLY for the column whose data type
    is in question.

    :param column_data: Iterable of values from a MySQL table column
    :param prefer_varchar: Use type VARCHAR if valid
    :param prefer_int: Use type INT if valid
    :return: data type
    """
    # Collect list of type, length tuples
    type_len_pairs = [ValueType(record).get_type_len for record in column_data]

    # Retrieve frequency counts of each type
    types_count = {t: type_len_pairs.count(t) for t in set([type_ for type_, len_, len_dec in type_len_pairs])}

    # Most frequently occurring datatype
    most_frequent = max(types_count.items(), key=itemgetter(1))[0]

    # Get max length of all rows to determine suitable limit
    len_lst, len_decimals_lst = [], []
    for type_, len_, len_dec in type_len_pairs:
        if type_ == most_frequent:
            if type(len_) is int:
                len_lst.append(len_)
            if type(len_dec) is int:
                len_decimals_lst.append(len_dec)

    # Catch errors if current type has no len
    try:
        max_len = max(len_lst)
    except ValueError:
        max_len = None
    try:
        max_len_decimal = max(len_decimals_lst)
    except ValueError:
        max_len_decimal = None

    # Return VARCHAR or INT type if flag is on
    if prefer_varchar and most_frequent != 'VARCHAR' and 'text' in most_frequent.lower():
        most_frequent = 'VARCHAR'
    elif prefer_int and most_frequent != 'INT' and 'int' in most_frequent.lower():
        most_frequent = 'INT'

    # Return MySQL datatype in proper format, only include length if it is set
    if max_len and max_len_decimal:
        return '{0} ({1}, {2})'.format(most_frequent, max_len, max_len_decimal)
    elif max_len:
        return '{0} ({1})'.format(most_frequent, max_len)
    else:
        return most_frequent
