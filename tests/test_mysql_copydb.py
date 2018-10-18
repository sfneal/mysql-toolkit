# Migrate SQL databases
from mysql.toolkit import MySQL
from looptools import Timer


config = {
    "database": "hpa_pt",
    "host": "stephenneal.net",
    "password": "Stealth19!",
    "port": 3306,
    "raise_on_warnings": True,
    "user": "stephen_hpa"
}
printing = True


def standard():
    with MySQL(config, enable_printing=printing) as sql:
        src, dst = 'hpa_pt', 'hpa_sandbox'
        sql.copy_database(src, dst)
        sql.compare_dbs(src, dst)


def optimized():
    with MySQL(config, enable_printing=printing) as sql:
        src, dst = 'hpa_pt', 'hpa_sandbox'
        sql.copy_database(src, dst, optimized=True)
        sql.compare_dbs(src, dst)


def one():
    with MySQL(config, enable_printing=printing) as sql:
        sql.get_column_definition('project', 'year')
        src, dst = 'hpa_pt', 'hpa_sandbox'
        sql.copy_database(src, dst, one_query=True)
        sql.compare_dbs(src, dst)

        print('-' * 200)
        for t in sql.tables:
            print(t)
            for col in sql.get_schema(t, True):
                print('\t{0:30} {1:15} {2:10} {3:10} {4:10} {5:10}'.format(*col))
        print('-' * 200)


def main():
    times = {
        'standard': 0.0,
        'optimized': 0.0,
        'one': 0.0
    }

    # t1 = Timer()
    # standard()
    # t1.end
    # times['standard'] += float(t1)

    # t2 = Timer()
    # optimized()
    # t2.end
    # times['optimized'] += float(t2)

    t3 = Timer()
    one()
    t3.end
    times['one'] += float(t3)

    for k, v in times.items():
        print('{0:12} {1}'.format(k, v))


if __name__ == '__main__':
    # main()
    with MySQL(config, enable_printing=printing) as sql:
        print(sql.get_column_definition('project', 'project'))
