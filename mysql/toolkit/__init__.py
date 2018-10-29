from mysql.toolkit.components import Connector, Manipulate, Structure, Operations


class MySQL(Connector, Manipulate, Structure, Operations):
    def __init__(self, config, enable_printing=True, auto_reconnect=True):
        """
        Connect to MySQL database and execute queries
        :param config: MySQL server configuration settings
        """
        # Initialize inherited classes
        Connector.__init__(self, config, enable_printing, auto_reconnect)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._printer('\tMySQL disconnecting')
        try:
            self._disconnect()
        except:
            self._printer('\tError raised during disconnection')
        self._printer('\tMySQL disconnected')


__all__ = ['MySQL']
