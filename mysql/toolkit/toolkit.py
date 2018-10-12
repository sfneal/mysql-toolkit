from mysql.toolkit.components import Connector, Query, SQL, Operations


class MySQL(Connector, Query, SQL, Operations):
    def __init__(self, config, enable_printing=True):
        """
        Connect to MySQL database and execute queries
        :param config: MySQL server configuration settings
        """
        # Initialize inherited classes
        super().__init__(config, enable_printing)
        # Connector.__init__(self, config, enable_printing)
        # Core.__init__(self)
        # Results.__init__(self)
        # Advanced.__init__(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._printer('\tMySQL disconnecting')
        try:
            self._disconnect()
        except:
            self._printer('\tError raised during disconnection')
        self._printer('\tMySQL disconnected')
