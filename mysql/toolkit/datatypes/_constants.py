import datetime
from decimal import Decimal


DATA_TYPES = {
    # Text Data Types
    'tinytext': {'type': str, 'max': 255},
    'varchar': {'type': str, 'max': 65535},
    'mediumtext': {'type': str, 'max': 16777215},
    'longtext': {'type': str, 'max': 4294967295},

    # Numeric Data Types
    'tinyint': {'type': int, 'min': -128, 'max': 127},
    'smallint': {'type': int, 'min': -32768, 'max': 32767},
    'mediumint': {'type': int, 'min': -8388608, 'max': 8388607},
    'int': {'type': int, 'min': -2147483648, 'max': 2147483647},
    'bigint': {'type': int, 'min': -9223372036854775808, 'max': 9223372036854775807},
    'decimal': {'type': (Decimal, float)},

    # Date Data Types
    'date': {'type': datetime.date},
    'datetime': {'type': datetime.datetime},
    'time': {'type': datetime.time},
    'year': {'type': int, 'min': 1901, 'max': 2155},
}
