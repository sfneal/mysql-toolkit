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


with MySQL(config) as sql:
    src, dst = 'hpa_pt', 'hpa_sandbox'
    sql.copy_database(src, dst)
    sql.compare_dbs(src, dst)

