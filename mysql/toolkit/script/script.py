import os
from tqdm import tqdm
from looptools import Timer
from mysql.toolkit.script.dump import dump_commands, get_commands_from_dir
from mysql.toolkit.script.split import SplitCommands
from mysql.toolkit.script.prepare import prepare_sql, filter_commands

# Conditional import of multiprocessing module
try:
    from multiprocessing import cpu_count
    from multiprocessing.pool import Pool
    MULTIPROCESS = True
except ImportError:
    pass


MAX_EXECUTION_ATTEMPTS = 5


class SQLScript:
    def __init__(self, sql_script=None, split_algo='sql_split', prep_statements=True, dump_fails=True,
                 mysql_instance=None):
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

        # execute method iterations
        self._execute_iters = 0

    @property
    def commands(self):
        """
        Fetch individual SQL commands from a SQL script containing many commands.

        :return: List of commands
        """
        # Retrieve all commands via split function or splitting on ';'
        print('\tRetrieving commands from', self.sql_script)
        print('\tUsing command splitter algorithm {0}'.format(self.split_algo))

        with Timer('\tRetrieved commands in'):
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

            # Prepare commands for SQL execution
            setattr(self, 'fetched_commands', cleaned_commands)
        return cleaned_commands

    def execute(self, commands=None, ignored_commands=('DROP', 'UNLOCK', 'LOCK'), execute_fails=True,
                max_executions=MAX_EXECUTION_ATTEMPTS):
        """
        Sequentially execute a list of SQL commands.

        Check if commands property has already been fetched, if so use the
        fetched_commands rather than getting them again.

        :param commands: List of SQL commands
        :param ignored_commands: Boolean, skip SQL commands that begin with 'DROP'
        :param execute_fails: Boolean, attempt to execute failed commands again
        :param max_executions: Int, max number of attempted executions
        :return: Successful and failed commands
        """
        # Break connection
        self._MySQL.disconnect()
        self._execute_iters += 1
        if self._execute_iters > 0:
            print('\tExecuting commands attempt #{0}'.format(self._execute_iters))

        # Retrieve commands from sql_script if no commands are provided
        commands = self.commands if not commands else commands

        # Remove 'DROP' commands
        if ignored_commands:
            commands = filter_commands(commands, ignored_commands)

        # Reestablish connection
        self._MySQL.reconnect()

        # Execute list of commands
        fail, success = self._execute_commands(commands)

        # Dump failed commands to text files
        print('\t' + str(success), 'successful commands')
        if len(fail) > 1 and self._dump_fails:
            # Dump failed commands
            dump_dir = self.dump_commands(fail)

            # Execute failed commands
            if execute_fails and self._execute_iters < max_executions:
                return self._execute_commands_from_dir(dump_dir)
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
                self._MySQL.executemore(command)
                success += 1
            except:
                fail.append(command)
        self._MySQL._commit()
        return fail, success

    def _execute_commands_from_dir(self, directory):
        """Re-attempt to split and execute the failed commands"""
        # Get file paths and contents
        commands = get_commands_from_dir(directory)

        # Execute failed commands again
        print('\tAttempting to execute {0} failed commands'.format(len(commands)))
        return self.execute(commands, ignored_commands=None, execute_fails=True)

    def dump_commands(self, commands):
        """Dump commands wrapper for external access."""
        # Get base directory
        directory = os.path.join(os.path.dirname(self.sql_script), 'fails')

        # Get file name to be used for folder name
        fname = os.path.basename(self.sql_script.rsplit('.')[0])

        return dump_commands(commands, directory, fname)
