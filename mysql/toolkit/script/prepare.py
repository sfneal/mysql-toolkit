try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO


def filter_commands(commands, invalid_query_starts=('DROP', 'UNLOCK', 'LOCK')):
    """
    Remove particular queries from a list of SQL commands.

    :param commands: List of SQL commands
    :param invalid_query_starts: Type of SQL command to remove
    :return: Filtered list of SQL commands
    """
    commands_with_drops = len(commands)
    filtered_commands = [c for c in commands if not c.startswith(invalid_query_starts)]
    if commands_with_drops - len(filtered_commands) > 0:
        print("\t" + str(invalid_query_starts) + " commands removed", commands_with_drops - len(filtered_commands))
    return filtered_commands


class PrepareSQL:
    def __init__(self, sql, add_semicolon=True, invalid_starts=('--', '/*', '*/', ';')):
        """
        Prepare a SQL statement for execution by removing comments and validating syntax.

        :param sql: SQL statement(s)
        :param add_semicolon: Add semicolon to end of statements
        :param invalid_starts: Invalid line starts
        """
        # TODO: Create method to clean up VALUES portion of an insert statement
        # Add whitespace to comma separated lists
        self._sql = sql.replace(', ', ',').replace(',', ', ')
        self._add_semicolon = add_semicolon
        self._invalid_starts = invalid_starts

    def __str__(self):
        return self.prepared

    @property
    def prepared(self):
        results = StringIO()

        in_statement = False
        in_line_comment = False
        in_block_comment = False
        for (start, end, contents) in self._split_sql(self._sql):
            precontents = None
            start_str = None

            # decide where we are
            if not in_statement and not in_line_comment and not in_block_comment:
                # not currently in any block
                if start != "--" and start != "/*" and len(contents.strip()) > 0:
                    # not starting a comment and there is contents
                    in_statement = True
                    precontents = ""

            if start == "/*":
                in_block_comment = True
            elif start == "--" and not in_block_comment:
                in_line_comment = True
                if not in_statement:
                    start_str = "//"

            start_str = start_str or start or ""
            precontents = precontents or ""

            # Only write line if line start is valid
            if start not in self._invalid_starts:
                results.write(start_str + precontents + contents)

            if not in_line_comment and not in_block_comment and in_statement and end == ";":
                in_statement = False

            if in_block_comment and end == "*/":
                in_block_comment = False

            if in_line_comment and end == "\n":
                in_line_comment = False

        response = results.getvalue()
        results.close()
        if self._add_semicolon and in_statement and not in_block_comment:
            if in_line_comment:
                response = response + "\n"
            response = response + ';'
        return response

    def _split_sql(self, sql):
        """
        Generate hunks of SQL that are between the bookends.
        note: beginning & end of string are returned as None

        :return: tuple of beginning bookend, closing bookend, and contents
        """
        bookends = ("\n", ";", "--", "/*", "*/")
        last_bookend_found = None
        start = 0

        while start <= len(sql):
            results = self._get_next_occurrence(sql, start, bookends)
            if results is None:
                yield (last_bookend_found, None, sql[start:])
                start = len(sql) + 1
            else:
                (end, bookend) = results
                yield (last_bookend_found, bookend, sql[start:end])
                start = end + len(bookend)
                last_bookend_found = bookend

    @staticmethod
    def _get_next_occurrence(haystack, offset, needles):
        """
        Find next occurence of one of the needles in the haystack

        :return: tuple of (index, needle found)
             or: None if no needle was found"""
        # make map of first char to full needle (only works if all needles
        # have different first characters)
        firstcharmap = dict([(n[0], n) for n in needles])
        firstchars = firstcharmap.keys()
        while offset < len(haystack):
            if haystack[offset] in firstchars:
                possible_needle = firstcharmap[haystack[offset]]
                if haystack[offset:offset + len(possible_needle)] == possible_needle:
                    return offset, possible_needle
            offset += 1
        return None


def prepare_sql(sql, add_semicolon=True, invalid_starts=('--', '/*', '*/', ';')):
    """Wrapper method for PrepareSQL class."""
    return PrepareSQL(sql, add_semicolon, invalid_starts).prepared
