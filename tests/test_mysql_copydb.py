# Migrate SQL databases
from mysql.toolkit import MySQL


config = {
    "database": "hpa_pt",
    "host": "stephenneal.net",
    "password": "Stealth19!",
    "port": 3306,
    "raise_on_warnings": True,
    "user": "stephen_hpa"
}


def standard():
    with MySQL(config, enable_printing=False) as sql:
        src, dst = 'hpa_pt', 'hpa_sandbox'
        sql.copy_database(src, dst)
        sql.compare_dbs(src, dst)


def optimized():
    with MySQL(config, enable_printing=False) as sql:
        src, dst = 'hpa_pt', 'hpa_sandbox'
        sql.copy_database(src, dst, optimized=True)
        sql.compare_dbs(src, dst)


def main():
    standard()
    optimized()


if __name__ == '__main__':
    main()
