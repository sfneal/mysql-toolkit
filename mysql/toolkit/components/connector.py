from mysql.connector import connect, Error
from mysql.connector import errorcode
from mysql.connector.errors import InterfaceError


class Connector:
    """
    Query execution helper methods for the MySQL class.

    Handles opening and closing a connection to a database source, fetching results
    from a query, executing a query and batch executing multiple queries.
    """
    _config = None
    enable_printing = True
    _cursor = None
    _cnx = None
    database = None
    CONFIGURED = False

    def __init__(self, config=None, enable_printing=None):
        if self._config is None and config is not None:
            self.configure(config, enable_printing)

    @classmethod
    def configure(cls, config, enable_printing):
        cls._config = config
        cls.enable_printing = enable_printing
        cls._cursor = None
        cls._cnx = None
        cls._connect(config)
        cls.database = config['database']
        cls.CONFIGURED = True

    def disconnect(self):
        """Disconnect from a MySQL database."""
        self._disconnect()

    def reconnect(self):
        """Reconnect to a MySQL database using the same config."""
        self._connect(self._config)

    def fetch(self, statement, commit=True):
        """Execute a SQL query and attempt to disconnect and reconnect if failure occurs."""
        try:
            return self._fetch(statement, commit)
        except InterfaceError:
            self.reconnect()
            return self._fetch(statement, commit)

    def execute(self, command):
        """Execute a single SQL query without returning a result."""
        self._cursor.execute(command)
        self._commit()
        return True

    def executemore(self, command):
        """Execute a single SQL query without returning a result."""
        self._cursor.execute(command)
        return True

    def executemany(self, command, params=None, max_attempts=5):
        """Execute multiple SQL queries without returning a result."""
        attempts = 0
        while attempts < max_attempts:
            try:
                # Execute statement
                self._cursor.executemany(command, params)
                self._commit()
                return True
            except Exception as e:
                attempts += 1
                self.reconnect()
                continue
        raise e

    def change_db(self, db):
        """Change connect database."""
        # Get original config and change database key
        config = self._config
        config['database'] = db

        # Close current database connection
        self._disconnect()

        # Reconnect to the new database
        self._connect(config)

    @classmethod
    def _connect(cls, config):
        """Establish a connection with a MySQL database."""
        if 'connection_timeout' not in cls._config:
            cls._config['connection_timeout'] = 480
        try:
            cls._cnx = connect(**config)
            cls._cursor = cls._cnx.cursor()
            cls._printer('\tMySQL DB connection established with db', config['database'])
        except Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            raise err

    @classmethod
    def _printer(cls, *msg):
        """Printing method for internal use."""
        if cls.enable_printing:
            print(*msg)

    def _close(self):
        """Close MySQL database connection."""
        self._cursor.close()
        self._cnx.close()

    def _commit(self):
        """Commit the changes made during the current connection."""
        self._cnx.commit()

    def _disconnect(self):
        """Destroy connection with MySQL database."""
        self._commit()
        self._close()

    def _fetch(self, statement, commit, max_attempts=5):
        """
        Execute a SQL query and return a result.

        Recursively disconnect and reconnect to the database
        if an error occurs.
        """
        attempts = 0
        while attempts < max_attempts:
            try:
                # Execute statement
                self._cursor.execute(statement)
                fetch = self._cursor.fetchall()
                rows = self._fetch_rows(fetch)
                if commit:
                    self._commit()

                # Return a single item if the list only has one item
                return rows[0] if len(rows) == 1 else rows
            except Exception as e:
                attempts += 1
                self.reconnect()
                continue
        raise e

    @staticmethod
    def _fetch_rows(fetch):
        """Retrieve fetched rows from a MySQL cursor."""
        rows = []
        for row in fetch:
            if len(row) == 1:
                rows.append(row[0])
            else:
                rows.append(list(row))
        return rows
