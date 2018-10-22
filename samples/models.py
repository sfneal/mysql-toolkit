import os
from mysql.toolkit import MySQL


BASE = os.path.dirname(__file__)


def setup_sample_data():
    config = {
        "database": "testing_models",
        "host": "stephenneal.net",
        "password": "Stealth19!",
        "port": 3306,
        "raise_on_warnings": True,
        "user": "stephen_master"
    }

    sql_files = [
        'models.sql',
    ]

    with MySQL(config) as sql:
        for dump in sql_files:
            p = os.path.join(BASE, dump)
            sql.execute_script(p)


def main():
    setup_sample_data()


if __name__ == '__main__':
    main()
