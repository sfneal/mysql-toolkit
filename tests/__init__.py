def config(db='testing_employees'):
    return {
        "database": db,
        "host": "stephenneal.net",
        "password": "thisisfortesting",
        "port": 3306,
        "raise_on_warnings": True,
        "user": "stephen_testing"
    }


__all__ = ['config']