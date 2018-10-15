# Dump SQL commands to files
import os
from time import time
from datetime import datetime
from looptools import Timer

# Conditional import of multiprocessing module
# Replace with global import
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
    print('\t' + str(len(commands)), 'failed commands')

    # Get base directory
    directory = os.path.dirname(sql_script) if os.path.isfile(sql_script) else sql_script

    # Get file name to be used for folder name
    src_fname = os.path.basename(sql_script.rsplit('.')[0]) if db is None else db

    # Set current timestamp
    timestamp = datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H-%M-%S')

    # Create a directory to save fail SQL scripts
    # TODO: Replace with function that recursively creates directories until path exists
    dump_dir = os.path.join(directory, sub_folder)
    if not os.path.exists(dump_dir):
        os.mkdir(dump_dir)
    dump_dir = os.path.join(dump_dir, src_fname)
    if not os.path.exists(dump_dir):
        os.mkdir(dump_dir)
    dump_dir = os.path.join(dump_dir, timestamp)
    if not os.path.exists(dump_dir):
        os.mkdir(dump_dir)

    # Create list of (path, content) tuples
    command_filepath = [(fail, os.path.join(dump_dir, str(count) + '.sql')) for count, fail in enumerate(commands)]

    # Dump failed commands to text file in the same directory as the script
    # Utilize's multiprocessing module if it is available
    timer = Timer()
    if MULTIPROCESS:
        pool = Pool(cpu_count())
        pool.map(dump, command_filepath)
        pool.close()
        print('\tDumped ', len(command_filepath), 'commands\n\t\tTime      : {0}'.format(timer.end),
              '\n\t\tMethod    : (multiprocessing)\n\t\tDirectory : {0}'.format(dump_dir))
    else:
        for tup in command_filepath:
            dump(tup)
        print('\tDumped ', len(command_filepath), 'commands\n\t\tTime      : {0}'.format(timer.end),
              '\n\t\tMethod    : (sequential)\n\t\tDirectory : {0}'.format(dump_dir))

    # Return base directory of dumped commands
    return dump_dir


def dump(tup):
    """
    Dump SQL command to a text file.

    :param tup: SQL command, text file path tuple
    """
    # Unpack tuple, clean command and dump to text file
    _command, txt_file = tup
    command = _command.strip()
    with open(txt_file, 'w') as txt:
        txt.writelines(command)
