import os
from time import time
from datetime import datetime
from tqdm import tqdm
from looptools import Timer

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
    if commands_with_drops - len(commands) > 0:
        print("\tDROP commands removed", commands_with_drops - len(filtered_commands))
        print("\tFiltered commands", len(filtered_commands))
    return filtered_commands


class SQLScript:
    def __init__(self, sql_script, split_func=True, split_char=';', dump_fails=True, mysql_instance=None):
        """Execute a sql file one command at a time."""
        # Pass MySQL instance from execute_script method to ExecuteScript class
        self.MySQL = mysql_instance

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
        print('\tRetrieving commands from', self.sql_script)
        # Open and read the file as a single buffer
        with open(self.sql_script, 'r') as fd:
            sql_file = fd.read()

        # Retrieve all commands via split function or splitting on ';'
        commands = split_sql_commands(sql_file) if self.split_func else sql_file.split(self.split_char)

        # remove dbo. prefixes from table names
        cleaned_commands = [com.replace("dbo.", '') for com in commands]
        setattr(self, 'fetched_commands', cleaned_commands)
        return cleaned_commands

    def execute(self, commands=None, skip_drops=True):
        """
        Sequentially execute a list of SQL commands.

        Check if commands property has already been fetched, if so use the
        fetched_commands rather than getting them again.

        :param commands: List of SQL commands
        :param skip_drops: Boolean, skip SQL commands that beging with 'DROP'
        :return: Successful and failed commands
        """
        # Retrieve commands from sql_script if no commands are provided
        commands = getattr(self, 'fetched_commands', self.commands) if not commands else commands

        # Remove 'DROP' commands
        if skip_drops:
            filter_commands(commands, 'DROP')

        # Execute commands get list of failed commands and count of successful commands
        print('\t' + str(len(commands)), 'commands')
        fail, success = [], 0
        for command in tqdm(commands, total=len(commands), desc='Executing SQL Commands'):
            # Attempt to execute command and skip command if error is raised
            try:
                self.MySQL.execute(command)
                success += 1
            except:
                fail.append(command)

        # Write fail commands to a text file
        print('\t' + str(success), 'successful commands')

        # Dump failed commands to text files
        if len(fail) > 1 and self._dump_fails:
            self._dump_failed(fail)
        return fail, success

    def _dump_failed(self, fails):
        """Dump failed commands to .sql files in the fails directory."""
        dump_commands(fails, self.sql_script)


def dump_commands(commands, sql_script, sub_folder='fails'):
    """
    Dump SQL commands to .sql files.

    :param commands: List of SQL commands
    :param sql_script: Path to SQL script
    :param sub_folder: Sub folder to dump commands to
    :return: Directory failed commands were dumped to
    """
    # Re-add semi-colon separator
    fails = [com + ';\n' for com in commands]
    print('\t' + str(len(fails)), 'failed commands')

    # Create a directory to save fail SQL scripts
    dump_dir = os.path.join(os.path.dirname(sql_script), sub_folder)
    if not os.path.exists(dump_dir):
        os.mkdir(dump_dir)
    dump_dir = os.path.join(dump_dir, datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H-%M-%S'))
    if not os.path.exists(dump_dir):
        os.mkdir(dump_dir)
    print('\tDumping failed commands to', dump_dir)

    # Create list of (path, content) tuples
    command_filepath = []
    for count, fail in tqdm(enumerate(fails), total=len(fails), desc='Dumping failed SQL commands to text'):
        txt_file = os.path.join(dump_dir, str(os.path.basename(sql_script).rsplit('.')[0]) + str(count) + '.sql')
        command_filepath.append((fail, txt_file))

    # Dump failed commands to text file in the same directory as the script
    # Utilize's multiprocessing module if it is available
    timer = Timer()
    if MULTIPROCESS:
        pool = Pool(cpu_count())
        pool.map(dump, command_filepath)
        pool.close()
        print('\tDumped ', len(command_filepath), 'commands in', timer.end, '(multiprocessing)')
    else:
        for tup in command_filepath:
            dump(tup)
        print('\tDumped ', len(command_filepath), 'commands in', timer.end, '(sequential processing)')

    # Return base directory of dumped commands
    return dump_dir


def dump(tup):
    """
    Dump SQL command to a text file.

    :param tup: SQL command, text file path tuple
    """
    # Unpack tuple
    command, txt_file = tup
    # Dump to text file
    with open(txt_file, 'w') as txt:
        txt.writelines(command)


def split_sql_commands(text):
    results = []
    current = ''
    state = None
    for c in tqdm(text, total=len(text), desc='Parsing SQL script file', unit='chars'):
        if state is None:  # default state, outside of special entity
            current += c
            if c in '"\'':
                # quoted string
                state = c
            elif c == '-':
                # probably "--" comment
                state = '-'
            elif c == '/':
                # probably '/*' comment
                state = '/'
            elif c == ';':
                # remove it from the statement
                current = current[:-1].strip()
                # and save current stmt unless empty
                if current:
                    results.append(current)
                current = ''
        elif state == '-':
            if c != '-':
                # not a comment
                state = None
                current += c
                continue
            # remove first minus
            current = current[:-1]
            # comment until end of line
            state = '--'
        elif state == '--':
            if c == '\n':
                # end of comment
                # and we do include this newline
                current += c
                state = None
            # else just ignore
        elif state == '/':
            if c != '*':
                state = None
                current += c
                continue
            # remove starting slash
            current = current[:-1]
            # multiline comment
            state = '/*'
        elif state == '/*':
            if c == '*':
                # probably end of comment
                state = '/**'
        elif state == '/**':
            if c == '/':
                state = None
            else:
                # not an end
                state = '/*'
        elif state[0] in '"\'':
            current += c
            if state.endswith('\\'):
                # prev was backslash, don't check for ender
                # just revert to regular state
                state = state[0]
                continue
            elif c == '\\':
                # don't check next char
                state += '\\'
                continue
            elif c == state[0]:
                # end of quoted string
                state = None
        else:
            raise Exception('Illegal state %s' % state)

    if current:
        current = current.rstrip(';').strip()
        if current:
            results.append(current)

    return results
