# Dump SQL commands to files
import os
from time import time
from datetime import datetime
from looptools import Timer

# Conditional import of multiprocessing module
try:
    from multiprocessing import cpu_count
    from multiprocessing.pool import Pool
    MULTIPROCESS = True
except ImportError:
    pass


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
    for count, fail in enumerate(fails):
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