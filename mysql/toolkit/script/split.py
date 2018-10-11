# Split SQL script file into list of commands
import os
import sqlparse
from tqdm import tqdm

# Conditional import of multiprocessing module
try:
    from multiprocessing import cpu_count
    from multiprocessing.pool import Pool
    MULTIPROCESS = True
except ImportError:
    pass


class SplitCommands:
    """Split a text blob or text file full of SQL commands into a list of single commands"""
    def __init__(self, text):
        # Check if text is a file_path or a string
        if os.path.isfile(text):
            with open(text, 'r') as fd:
                self.sql_data = fd.read()
        else:
            self.sql_data = text

    def __call__(self):
        return self.__iter__()

    def __iter__(self):
        return iter(self.sql_parse)

    def __len__(self):
        return len(self.sql_parse)

    @property
    def sql_parse(self):
        commands = []
        for command in sqlparse.split(self.sql_data):
            commands.extend(self.sql_split(command))
        try:
            return set(commands)
        except:
            print('\tSet error in script.split.SplitCommands.parse')
            return {c for c in commands}

    @property
    def sql_parse_nosplit(self):
        commands = []
        for command in sqlparse.split(self.sql_data):
            commands.extend(command)
        try:
            return set(commands)
        except:
            print('\tSet error in script.split.SplitCommands.parse')
            return {c for c in commands}

    def sql_split(self, text=None, disable_tqdm=True, iteration=None):
        data = self.sql_data if not text else text

        # Set progress bar description
        _desc = 'Parsing SQL script file'
        desc = _desc + ' ' + str(iteration) if iteration else _desc

        results = []
        current = ''
        state = None
        for c in tqdm(data, total=len(data), desc=desc, unit='chars', disable=disable_tqdm):
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

    def simple_split(self, split_char=';'):
        """Read a SQL script file and split on a particular char"""
        # Open and read the file as a single buffer
        return self.sql_data.split(split_char)
