from mysql.connector import errors
from mysql.toolkit.connector import Connector
from mysql.toolkit.components import Core, Results, Advanced


class MySQL(Connector, Core, Results, Advanced):
    def __init__(self, config, enable_printing=True):
        """
        Connect to MySQL database and execute queries
        :param config: MySQL server configuration settings
        """
        # Initialize inherited classes
        Connector.__init__(self, config, enable_printing)
        Core.__init__(self)
        Results.__init__(self)
        Advanced.__init__(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print('\tMySQL disconnecting')
        try:
            self._commit()
            self._close()
        except errors as e:
            print('\tError: ' + str(e))
            print('\tMySQL disconnected')
