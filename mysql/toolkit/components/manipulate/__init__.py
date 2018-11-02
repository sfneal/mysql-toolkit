from mysql.toolkit.components.manipulate.select import Select
from mysql.toolkit.components.manipulate.update import Update
from mysql.toolkit.components.manipulate.insert import Insert
from mysql.toolkit.components.manipulate.deleter import Delete


class Manipulate(Select, Insert, Update, Delete):
    def __init__(self):
        pass
