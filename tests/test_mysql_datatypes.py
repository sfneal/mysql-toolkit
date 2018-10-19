# Migrate SQL databases
from mysql.toolkit.script.datatypes import Record
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


def main():
    datatypes()


if __name__ == '__main__':
    main()
