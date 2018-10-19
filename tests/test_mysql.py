# Migrate SQL databases
from mysql.toolkit import MySQL


def remote():
    config = {
        "database": "hpa_allin",
        "host": "stephenneal.net",
        "password": "Stealth19!",
        "port": 3306,
        "raise_on_warnings": True,
        "user": "stephen_hpa"
    }

    with MySQL(config) as sql:
        # Show databases
        print(sql.databases)

        # Count number of rows
        c = sql.count_rows_all()
        for table, count in c.items():
            if count > 1:
                print(table, count)


def local():
    config = {
        "database": "midland",
        "host": "localhost",
        "password": "Stealth19!",
        "port": 8889,
        "raise_on_warnings": True,
        "user": "root"
    }

    with MySQL(config) as sql:
        # Show databases
        print(sql.databases)

        # Count number of rows
        c = sql.count_rows_all()
        for table, count in c.items():
            if count > 1:
                print(table, count)


def main():
    # remote()
    local()


if __name__ == '__main__':
    main()
