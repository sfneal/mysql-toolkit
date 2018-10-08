# Migrate SQL databases
from mysql.toolkit import MySQL


config = {
    "database": "hpa_allin",
    "host": "stephenneal.net",
    "password": "Stealth19!",
    "port": 3306,
    "raise_on_warnings": True,
    "user": "stephen_hpa"
}


with MySQL(config) as sql:
    # Retrieve list of tables
    # t = sql.tables
    # for i in t:
    #     print(i)
    # print('\n')
    #
    # # Show table data schema
    # s = sql.get_schema('AddressEmailPhoneType', with_headers=True)
    # for i in s:
    #     print(i)
    # print('\n')

    # Count number of rows
    c = sql.count_rows_all()
    for table, count in c.items():
        if count > 1:
            print(table, count)

