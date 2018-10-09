from mysql.toolkit.script.dump import dump_commands
from mysql.toolkit.script.split import SplitCommands
from tqdm import tqdm

# Conditional import of multiprocessing module
try:
    from multiprocessing import cpu_count
    from multiprocessing.pool import Pool
    MULTIPROCESS = True
except ImportError:
    pass


def filter_commands(commands, query_type):
    """
    Remove particular queries from a list of SQL commands.

    :param commands: List of SQL commands
    :param query_type: Type of SQL command to remove
    :return: Filtered list of SQL commands
    """
    commands_with_drops = len(commands)
    filtered_commands = [c for c in commands if not c.startswith(query_type)]
    if commands_with_drops - len(filtered_commands) > 0:
        print("\t" + query_type + " commands removed", commands_with_drops - len(filtered_commands))
        print("\tFiltered commands", len(filtered_commands))
    return filtered_commands


class SQLScript:
    def __init__(self, sql_script, split_func=True, split_char=';', dump_fails=True, mysql_instance=None):
        """Execute a sql file one command at a time."""
        # Pass MySQL instance from execute_script method to ExecuteScript class
        self._MySQL = mysql_instance

        # SQL script to be executed
        self.sql_script = sql_script

        # split_func boolean and splitter character
        self.split_func, self.split_char = split_func, split_char

        # Dump failed SQL commands boolean
        self._dump_fails = dump_fails

    @property
    def commands(self):
        """
        Fetch individual SQL commands from a SQL script containing many commands.

        :return: List of commands
        """
        # Open and read the file as a single buffer
        print('\tRetrieving commands from', self.sql_script)
        with open(self.sql_script, 'r') as fd:
            sql_file = fd.read()

        # Retrieve all commands via split function or splitting on ';'
        commands = SplitCommands(self.sql_script) if self.split_func else sql_file.split(self.split_char)

        # remove dbo. prefixes from table names
        cleaned_commands = [com.replace("dbo.", '') for com in commands]
        setattr(self, 'fetched_commands', cleaned_commands)
        return cleaned_commands

    def execute(self, commands=None, skip_drops=True, execute_fails=True):
        """
        Sequentially execute a list of SQL commands.

        Check if commands property has already been fetched, if so use the
        fetched_commands rather than getting them again.

        :param commands: List of SQL commands
        :param skip_drops: Boolean, skip SQL commands that begin with 'DROP'
        :return: Successful and failed commands
        """
        # Retrieve commands from sql_script if no commands are provided
        commands = getattr(self, 'fetched_commands', self.commands) if not commands else commands

        # Remove 'DROP' commands
        if skip_drops:
            commands = filter_commands(commands, 'DROP')

        # Execute list of commands
        fail, success = self._execute_commands(commands)

        # Write fail commands to a text file
        print('\t' + str(success), 'successful commands')

        # Dump failed commands to text files
        if len(fail) > 1 and self._dump_fails:
            self.dump_commands(fail)

        # Execute failed commands
        if execute_fails:
            self._execute_failed_commands(fail)
        else:
            return fail, success

    def _execute_commands(self, commands):
        """Execute commands and get list of failed commands and count of successful commands"""
        print('\t' + str(len(commands)), 'commands')
        fail, success = [], 0
        for command in tqdm(commands, total=len(commands), desc='Executing SQL Commands'):
            # Attempt to execute command and skip command if error is raised
            try:
                self._MySQL.execute(command)
                success += 1
            except:
                fail.append(command)
        return fail, success

    def _execute_failed_commands(self, fails):
        """Re-attempt to split and execute the failed commands"""
        commands = []
        for failed in fails:
            f = SplitCommands(failed)
            print(len(f))
            commands.extend(f)
        self.execute(commands, execute_fails=False)

    def dump_commands(self, commands):
        """Dump commands wrapper for external access."""
        dump_commands(commands, self.sql_script)
