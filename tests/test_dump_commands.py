import os
from mysql.toolkit.script.split import SplitCommands
from mysql.toolkit.script.dump import dump_commands


sql_script = os.path.join(os.path.dirname(__file__), 'data', 'oneline.sql')

with open(sql_script, 'r') as sf:
    stmnt = sf.read()

split = SplitCommands(stmnt).sql_parse

dump_commands(split, sql_script)
