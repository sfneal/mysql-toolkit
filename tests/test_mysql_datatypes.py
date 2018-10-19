# Migrate SQL databases
from mysql.toolkit.script.datatypes import Record, column_datatype
from datetime import datetime


def datatypes():
    d = datetime.now()
    tests = [
        14,
        'this is a test varchar',
        d.year,
        d.timestamp(),
        532.45,
        121231231,
    ]
    for t in tests:
        print('{0:15} {1}'.format(Record(t).datatype, t))


def col_types():
    d = ['sdad', 'asdasdasd', 'dfsdf', 'azxcwe', 'asccs', 'asasc']
    print(column_datatype(d))


def main():
    datatypes()
    col_types()


if __name__ == '__main__':
    main()
