# Migrate SQL databases
from mysql.toolkit import MySQL


def rename():
    config = {
        "database": "hpa_sandbox",
        "host": "stephenneal.net",
        "password": "Stealth19!",
        "port": 3306,
        "raise_on_warnings": True,
        "user": "stephen_hpa"
    }

    with MySQL(config) as sql:
        # Show databases
        print(sql.tables)

        # Rename tables
        for table in sql.tables:
            new_name = '{0}_new'.format(table)
            sql.rename(table, new_name)

        print(sql.tables)


def main():
    rename()


if __name__ == '__main__':
    main()
