import os
from looptools import Timer
from mysql.toolkit.script.prepare import prepare_sql, filter_commands
from mysql.toolkit.script.split import SplitCommands


with open(os.path.join(os.path.dirname(__file__), 'data', 'All_In_HPA1.sql'), 'r') as sf:
    stmnt = sf.read()


def test1():
    prepared = prepare_sql(stmnt)
    split = SplitCommands(prepared).sql_split()
    filtered = filter_commands(split)
    return split, filtered


def test2():
    split = SplitCommands(stmnt).sql_split()
    filtered = filter_commands(split)
    return split, filtered


def test3():
    prepared = prepare_sql(stmnt)
    split = SplitCommands(prepared).sql_parse
    filtered = filter_commands(split)
    return split, filtered


def test4():
    split = SplitCommands(stmnt).sql_parse
    filtered = filter_commands(split)
    return split, filtered


def test5():
    prepared = prepare_sql(stmnt)
    split = SplitCommands(prepared).sql_parse_nosplit
    filtered = filter_commands(split)
    return split, filtered


def test6():
    split = SplitCommands(stmnt).sql_parse_nosplit
    filtered = filter_commands(split)
    return split, filtered


def test7():
    prepared = prepare_sql(stmnt)
    split = SplitCommands(prepared).simple_split()
    filtered = filter_commands(split)
    return split, filtered


def test8():
    split = SplitCommands(stmnt).simple_split()
    filtered = filter_commands(split)
    return split, filtered


def main():
    funcs = [
        (test1, 'Test 1: Prepare statement and sql_split'),
        (test2, 'Test 2: sql_split'),
        (test3, 'Test 3: Prepare statement and sql_parse'),
        (test4, 'Test 4: sql_parse'),
        (test5, 'Test 5: Prepare statement and sql_parse_nosplit'),
        (test6, 'Test 6: sql_parse_nosplit'),
        (test7, 'Test 7: Prepare statement and simple_split'),
        (test8, 'Test 8: simple_split'),
    ]
    for test, name in funcs:
        print('-------------------------------------------------------')
        print(name)
        print('-------------------------------------------------------')
        with Timer('\n'):
            split, filtered = test()
            print('Split commands', len(split), '\nFiltered commands', len(filtered))
        print('\n\n')


if __name__ == '__main__':
    main()
