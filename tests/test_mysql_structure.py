# Migrate SQL databases
from mysql.toolkit import MySQL


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
    print(sql.get_schema('project', with_headers=True))
    print(sql.get_columns('project'))
    print('\n')

    print(sql.count_rows_non_distinct('project', 'project'))
    print(sql.count_rows_distinct('project', 'project'))

    print('-' * 200)
    for t in sql.tables:
        print(t)
        for col in sql.get_schema(t, True):
            print('\t{0:30} {1:15} {2:10} {3:10} {4:10} {5:10}'.format(*col))
    print('-' * 200)
