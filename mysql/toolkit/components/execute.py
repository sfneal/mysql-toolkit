import os
from time import time
from datetime import datetime
from tqdm import tqdm


class SQLScript:
    def __init__(self, mysql_instance, sql_script, commands=None, split_func=True, split_char=';', dump_fails=True):
        """Execute a sql file one command at a time."""
        # Pass MySQL instance from execute_script method to ExecuteScript class
        self.MySQL = mysql_instance

        # split_func boolean and splitter character
        self.split_func, self.split_char = split_func, split_char

        # SQL script to be executed
        self.sql_script = sql_script

        # Retrieve commands from sql_script if no commands are provided
        self.commands = self.get_commands(sql_script) if not commands else commands

        # Execute commands get list of failed commands and count of successful commands
        self.fail, self.success = self.execute_commands(self.commands)

        # Dump failed commands to text file
        if len(self.fail) > 1 and dump_fails:
            self.dump_failed_commands()

    def get_commands(self, sql_script):
        """
        Fetch individual SQL commands from a SQL script containing many commands.

        :param sql_script: Path to SQL script
        :return: List of commands
        """
        print('\tRetrieving commands from', sql_script)
        # Open and read the file as a single buffer
        with open(sql_script, 'r') as fd:
            sql_file = fd.read()

        # Retrieve all commands via split function or splitting on ';'
        commands = split_sql_commands(sql_file) if self.split_func else sql_file.split(self.split_char)

        # remove dbo. prefixes from table names
        return [com.replace("dbo.", '') for com in commands]

    def execute_commands(self, commands, skip_drops=True):
        """
        Sequentially execute a list of SQL commands.

        :param commands: List of SQL commands
        :param skip_drops: Boolean, skip SQL commands that beging with 'DROP'
        :return: Successful and failed commands
        """
        # Remove 'DROP' commands
        if skip_drops:
            commands_with_drops = len(commands)
            commands = [c for c in commands if not c.startswith('DROP')]
            if commands_with_drops - len(commands) > 0:
                print("\tDROP commands removed", commands_with_drops - len(commands))

        # Execute every command in list of commands
        print('\t' + str(len(commands)), 'commands')

        fail, success = [], 0
        for command in tqdm(commands, total=len(commands), desc='Executing SQL Commands'):
            # This will skip and report errors
            # For example, if the tables do not yet exist, this will skip over
            # the DROP TABLE commands
            try:
                self.MySQL.execute(command)
                success += 1
            except:
                fail.append(command)

        # Write fail commands to a text file
        print('\t' + str(success), 'successful commands')
        return fail, success

    def dump_failed_commands(self):
        """Dump failed commands to .sql files in the fails directory."""
        dump_commands(self.fail, self.sql_script)


def dump_commands(commands, sql_script):
    """Dump SQL commands to .sql files."""
    # Re-add semi-colon separator
    fails = [com + ';\n' for com in commands]
    print('\t' + str(len(fails)), 'failed commands')

    # Create a directory to save fail SQL scripts
    fails_dir = os.path.join(os.path.dirname(sql_script), 'fails')
    if not os.path.exists(fails_dir):
        os.mkdir(fails_dir)
    fails_dir = os.path.join(fails_dir, datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H-%M-%S'))
    if not os.path.exists(fails_dir):
        os.mkdir(fails_dir)
    print('\tDumping failed commands to', fails_dir)

    # Dump failed commands to text file in the same directory as the script
    for count, fail in tqdm(enumerate(fails), total=len(fails), desc='Dumping failed SQL commands to text'):
        fails_fname = str(os.path.basename(sql_script).rsplit('.')[0]) + str(count) + '.sql'
        txt_file = os.path.join(fails_dir, fails_fname)

        # Dump to text file
        with open(txt_file, 'w') as txt:
            txt.writelines(fail)


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
