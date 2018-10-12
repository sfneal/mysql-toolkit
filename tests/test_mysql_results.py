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
    # Show databases
    print(sql.get_schema('project', with_headers=True))
    print(sql.get_columns('project'))

