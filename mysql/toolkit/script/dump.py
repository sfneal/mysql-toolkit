# Dump SQL commands to files
import os
import shutil
from dirutility import ZipBackup
from looptools import Timer
from time import time
from datetime import datetime
from tempfile import TemporaryDirectory

# Conditional import of multiprocessing module
# Replace with global import
try:
    from multiprocessing import cpu_count
    from multiprocessing.pool import Pool
    MULTIPROCESS = True
except ImportError:
    pass


def set_dump_directory(base=None, sub_dir=None):
    """Create directory for dumping SQL commands."""
    # Set current timestamp
    timestamp = datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H-%M-%S')

    # Clean sub_dir
    if sub_dir and '.' in sub_dir:
        sub_dir = sub_dir.rsplit('.', 1)[0]

    # Create a directory to save fail SQL scripts
    # TODO: Replace with function that recursively creates directories until path exists
    if not os.path.exists(base):
        os.mkdir(base)
    dump_dir = os.path.join(base, sub_dir) if sub_dir else base
    if not os.path.exists(dump_dir):
        os.mkdir(dump_dir)
    dump_dir = os.path.join(dump_dir, timestamp)
    if not os.path.exists(dump_dir):
        os.mkdir(dump_dir)
        return dump_dir


def dump_commands(commands, directory=None, sub_dir=None):
    """
    Dump SQL commands to .sql files.

    :param commands: List of SQL commands
    :param directory: Directory to dump commands to
    :param sub_dir: Sub directory
    :return: Directory failed commands were dumped to
    """
    print('\t' + str(len(commands)), 'failed commands')

    # Create dump_dir directory
    if directory and os.path.isfile(directory):
        dump_dir = set_dump_directory(os.path.dirname(directory), sub_dir)
        return_dir = dump_dir
    elif directory:
        dump_dir = set_dump_directory(directory, sub_dir)
        return_dir = dump_dir
    else:
        dump_dir = TemporaryDirectory().name
        return_dir = TemporaryDirectory()

    # Create list of (path, content) tuples
    command_filepath = [(fail, os.path.join(dump_dir, str(count) + '.sql')) for count, fail in enumerate(commands)]

    # Dump failed commands to text file in the same directory as the script
    # Utilize's multiprocessing module if it is available
    timer = Timer()
    if MULTIPROCESS:
        pool = Pool(cpu_count())
        pool.map(write_text, command_filepath)
        pool.close()
        print('\tDumped ', len(command_filepath), 'commands\n\t\tTime      : {0}'.format(timer.end),
              '\n\t\tMethod    : (multiprocessing)\n\t\tDirectory : {0}'.format(dump_dir))
    else:
        for tup in command_filepath:
            write_text(tup)
        print('\tDumped ', len(command_filepath), 'commands\n\t\tTime      : {0}'.format(timer.end),
              '\n\t\tMethod    : (sequential)\n\t\tDirectory : {0}'.format(dump_dir))

    # Return base directory of dumped commands
    return return_dir


def write_text(tup):
    """
    Dump SQL command to a text file.

    :param tup: SQL command, text file path tuple
    """
    # Unpack tuple, clean command and dump to text file
    _command, txt_file = tup
    command = _command.strip()
    with open(txt_file, 'w') as txt:
        txt.writelines(command)


def get_commands_from_dir(directory, zip_backup=True, remove_dir=True):
    """Traverse a directory and read contained SQL files."""
    # Get SQL script file paths
    failed_scripts = sorted([os.path.join(directory, fn) for fn in os.listdir(directory) if fn.endswith('.sql')])

    # Read each failed SQL file and append contents to a list
    print('\tReading SQL scripts from files')
    commands = []
    for sql_file in failed_scripts:
        with open(sql_file, 'r') as txt:
            sql_command = txt.read()
        commands.append(sql_command)

    # Remove most recent failures folder after reading
    if zip_backup:
        ZipBackup(directory).backup()
    if remove_dir:
        shutil.rmtree(directory)
    return commands
