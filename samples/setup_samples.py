import os
from mysql.toolkit import MySQL


BASE = os.path.dirname(__file__)


def setup_sample_data():
    config = {
        "database": "toolkit_testing",
        "host": "stephenneal.net",
        "password": "Stealth19!",
        "port": 3306,
        "raise_on_warnings": True,
        "user": "stephen_master"
    }

    sql_files = [
        # 'employees.sql',
        'load_departments.dump',
        'load_dept_emp.dump',
        'load_dept_manager.dump',
        'load_employees.dump',
        'load_salaries1.dump',
        'load_salaries2.dump',
        'load_salaries3.dump',
        'load_titles.dump',
        'objects.dump',
        'show_elapsed.dump',
    ]

    with MySQL(config) as sql:
        for dump in sql_files:
            p = os.path.join(BASE, dump)
            sql.execute_script(p)


def main():
    setup_sample_data()


if __name__ == '__main__':
    main()
