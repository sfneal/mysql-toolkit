from tqdm import tqdm
from mysql.toolkit.script.dump import dump_commands, write_read_commands
from mysql.toolkit.script.split import SplitCommands
from mysql.toolkit.script.prepare import prepare_sql, filter_commands

# Conditional import of multiprocessing module
try:
    from multiprocessing import cpu_count
    from multiprocessing.pool import Pool
    MULTIPROCESS = True
except ImportError:
    pass


class SQLScript:
    def __init__(self, sql_script, split_algo='sql_split', prep_statements=True, dump_fails=True, mysql_instance=None):
        """Execute a sql file one command at a time."""
        # Pass MySQL instance from execute_script method to ExecuteScript class
        self._MySQL = mysql_instance

        # SQL script to be executed
        self.sql_script = sql_script

        # Function to use to split SQL commands
        self.split_algo = split_algo

        # Pass SQL statements through PrepareSQL class if True
        self._prep_statements = prep_statements

        # Dump failed SQL commands boolean
        self._dump_fails = dump_fails

    @property
    def commands(self):
        """
        Fetch individual SQL commands from a SQL script containing many commands.

        :return: List of commands
        """
        # Retrieve all commands via split function or splitting on ';'
        print('\tRetrieving commands from', self.sql_script)
        print('\tUsing command splitter algorithm {0}'.format(self.split_algo))

        # Split commands
        # sqlparse packages split function combined with sql_split function
        if self.split_algo is 'sql_parse':
            commands = SplitCommands(self.sql_script).sql_parse

        # Split on every ';' (unreliable)
        elif self.split_algo is 'simple_split':
            commands = SplitCommands(self.sql_script).simple_split()

        # sqlparse package without additional splitting
        elif self.split_algo is 'sql_parse_nosplit':
            commands = SplitCommands(self.sql_script).sql_parse_nosplit

        # Parse every char of the SQL script and determine breakpoints
        elif self.split_algo is 'sql_split':
            commands = SplitCommands(self.sql_script).sql_split(disable_tqdm=False)
        else:
            commands = SplitCommands(self.sql_script).sql_split(disable_tqdm=False)

        # remove dbo. prefixes from table names
        cleaned_commands = [com.replace("dbo.", '') for com in commands]

        # Write and read each command to a text file
        read_commands = write_read_commands(cleaned_commands)

        # Prepare commands for SQL execution
        setattr(self, 'fetched_commands', read_commands)
        return read_commands

    def execute(self, commands=None, ignored_commands=('DROP', 'UNLOCK', 'LOCK'), execute_fails=True):
        """
        Sequentially execute a list of SQL commands.

        Check if commands property has already been fetched, if so use the
        fetched_commands rather than getting them again.

        :param commands: List of SQL commands
        :param ignored_commands: Boolean, skip SQL commands that begin with 'DROP'
        :param execute_fails: Boolean, attempt to execute failed commands again
        :return: Successful and failed commands
        """
        # Retrieve commands from sql_script if no commands are provided
        commands = getattr(self, 'fetched_commands', self.commands) if not commands else commands

        # Remove 'DROP' commands
        if ignored_commands:
            commands = filter_commands(commands, ignored_commands)

        # Execute list of commands
        fail, success = self._execute_commands(commands)

        # Dump failed commands to text files
        print('\t' + str(success), 'successful commands')
        if len(fail) > 1 and self._dump_fails:
            # Dump failed commands
            self.dump_commands(fail)

            # Execute failed commands
            if execute_fails:
                print('executing failed')
                self._execute_failed_commands(fail)
        return fail, success

    def _execute_commands(self, commands, fails=False):
        """Execute commands and get list of failed commands and count of successful commands"""
        # Confirm that prepare_statements flag is on
        if self._prep_statements:
            prepared_commands = [prepare_sql(c) for c in tqdm(commands, total=len(commands),
                                                              desc='Prepping SQL Commands')]
            print('\tCommands prepared', len(prepared_commands))
        else:
            prepared_commands = commands

        desc = 'Executing SQL Commands' if not fails else 'Executing Failed SQL Commands'
        fail, success = [], 0
        for command in tqdm(prepared_commands, total=len(prepared_commands), desc=desc):
            # Attempt to execute command and skip command if error is raised
            try:
                self._MySQL.execute(command)
                success += 1
            except:
                fail.append(command)
        return fail, success

    def _execute_failed_commands(self, fails):
        """Re-attempt to split and execute the failed commands"""
        # Execute failed commands again
        self._execute_commands(fails, fails=True)

    def dump_commands(self, commands):
        """Dump commands wrapper for external access."""
        dump_commands(commands, self.sql_script)
