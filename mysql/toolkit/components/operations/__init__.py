from mysql.toolkit.commands.execute import Execute
from mysql.toolkit.components.operations.alter import Alter
from mysql.toolkit.components.operations.clone import Clone
from mysql.toolkit.components.operations.compare import Compare
from mysql.toolkit.components.operations.remove import Remove


class Operations(Alter, Compare, Clone, Remove):
    def execute_script(self, sql_script=None, commands=None, split_algo='sql_split', prep_statements=False,
                       dump_fails=True, execute_fails=True, ignored_commands=('DROP', 'UNLOCK', 'LOCK')):
        """Wrapper method for SQLScript class."""
        ss = Execute(sql_script, split_algo, prep_statements, dump_fails, self)
        ss.execute(commands, ignored_commands=ignored_commands, execute_fails=execute_fails)

    def script(self, sql_script, split_algo='sql_split', prep_statements=True, dump_fails=True):
        """Wrapper method providing access to the SQLScript class's methods and properties."""
        return Execute(sql_script, split_algo, prep_statements, dump_fails, self)
