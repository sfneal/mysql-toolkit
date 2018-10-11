# Dump SQL commands to files
import os
from time import time
from datetime import datetime
from looptools import Timer
from tqdm import tqdm
from tempfile import NamedTemporaryFile

# Conditional import of multiprocessing module
try:
    from multiprocessing import cpu_count
    from multiprocessing.pool import Pool
    MULTIPROCESS = True
except ImportError:
    pass


def dump_commands(commands, sql_script, db=None, sub_folder='fails'):
    """
    Dump SQL commands to .sql files.

    :param commands: List of SQL commands
    :param sql_script: Path to SQL script
    :param db: Name of a database
    :param sub_folder: Sub folder to dump commands to
    :return: Directory failed commands were dumped to
    """
    # Re-add semi-colon separator
    fails = [com + ';\n' for com in commands]
    print('\t' + str(len(fails)), 'failed commands')

    # Create a directory to save fail SQL scripts
    directory = os.path.dirname(sql_script) if os.path.isfile(sql_script) else sql_script
    dump_dir = os.path.join(directory, sub_folder)
    if not os.path.exists(dump_dir):
        os.mkdir(dump_dir)

    # Set current timestamp
    timestamp = datetime.fromtimestamp(time()).strftime('%H-%M-%S')

    # Get file name to be used for folder name
    src_fname = os.path.basename(sql_script.rsplit('.')[0]) if db is None else db

    dump_dir = os.path.join(dump_dir, '{0}_{1}'.format(src_fname, timestamp))
    if not os.path.exists(dump_dir):
        os.mkdir(dump_dir)

    # Create list of (path, content) tuples
    command_filepath = []
    for count, fail in enumerate(fails):
        txt_file = os.path.join(dump_dir, str(count) + '.sql')
        command_filepath.append((fail, txt_file))

    # Dump failed commands to text file in the same directory as the script
    # Utilize's multiprocessing module if it is available
    timer = Timer()
    if MULTIPROCESS:
        pool = Pool(cpu_count())
        pool.map(dump, command_filepath)
        pool.close()
        print('\tDumped ', len(command_filepath), 'commands in', timer.end, '(multiprocessing) to', dump_dir)
    else:
        for tup in command_filepath:
            dump(tup)
        print('\tDumped ', len(command_filepath), 'commands in', timer.end, '(sequential processing) to', dump_dir)

    # Return base directory of dumped commands
    return dump_dir


def dump(tup):
    """
    Dump SQL command to a text file.

    :param tup: SQL command, text file path tuple
    """
    # Unpack tuple
    _command, txt_file = tup

    # Clean up command
    command = _command.strip()

    # Dump to text file
    with open(txt_file, 'w') as txt:
        txt.writelines(command)


def _write_read(command):
    """Write and read SQL commands to and from text files."""
    # Create temporary file context
    with NamedTemporaryFile(suffix='.sql') as temp:
        # Write to sql file
        with open(temp.name, 'w') as write:
            write.writelines(command)

        # Read the sql file
        with open(temp.name, 'r') as read:
            _command = read.read()
    return _command


def _write_read_packed(pack):
    """Multiprocessing intermediary wrapper"""
    index, command = pack
    return [index, _write_read(command)]


def write_read_commands(commands):
    """Multiprocessing wrapper for _write_read function."""
    if MULTIPROCESS:
        commands_packed = [(index, command) for index, command in enumerate(commands)]
        timer = Timer()
        pool = Pool(cpu_count())
        _commands = pool.map(_write_read_packed, commands_packed)
        pool.close()
        print('\tRead and Wrote ', len(_commands), 'commands in', timer.end, '(multiprocessing)')

        # Sort list by index and then return flat list of commands
        return [cmd_lst[1] for cmd_lst in sorted(_commands, key=lambda i: i[0])]
    else:
        return [_write_read(command) for command in tqdm(commands, total=len(commands),
                                                         desc='Writing and Reading SQL commands')]
