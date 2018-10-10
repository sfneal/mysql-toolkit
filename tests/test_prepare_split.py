import os
from mysql.toolkit.script.prepare import prepare_sql, filter_commands
from mysql.toolkit.script.split import SplitCommands


with open(os.path.join(os.path.dirname(__file__), 'data', 'All_In_HPA1.sql'), 'r') as sf:
    stmnt = sf.read()


def test1():
    name = 'Test 1: Prepare statement and sql_split'
    prepared = prepare_sql(stmnt)
    split = SplitCommands(prepared).sql_split()
    filtered = filter_commands(split)
    return name, split, filtered


def test2():
    name = 'Test 2: sql_split'
    split = SplitCommands(stmnt).sql_split()
    filtered = filter_commands(split)
    return name, split, filtered


def test3():
    name = 'Test 3: Prepare statement and sql_parse'
    prepared = prepare_sql(stmnt)
    split = SplitCommands(prepared).sql_parse
    filtered = filter_commands(split)
    return name, split, filtered


def test4():
    name = 'Test 4: sql_parse'
    split = SplitCommands(stmnt).sql_parse
    filtered = filter_commands(split)
    return name, split, filtered


def test5():
    name = 'Test 5: Prepare statement and sql_parse_nosplit'
    prepared = prepare_sql(stmnt)
    split = SplitCommands(prepared).sql_parse_nosplit
    filtered = filter_commands(split)
    return name, split, filtered


def test6():
    name = 'Test 6: sql_parse_nosplit'
    split = SplitCommands(stmnt).sql_parse_nosplit
    filtered = filter_commands(split)
    return name, split, filtered


def test7():
    name = 'Test 7: Prepare statement and simple_split'
    prepared = prepare_sql(stmnt)
    split = SplitCommands(prepared).simple_split()
    filtered = filter_commands(split)
    return name, split, filtered


def test8():
    name = 'Test 8: simple_split'
    split = SplitCommands(stmnt).simple_split()
    filtered = filter_commands(split)
    return name, split, filtered


def main():
    funcs = [test1, test2, test3, test4, test5, test6, test7, test8]
    for test in funcs:
        name, split, filtered = test()
        print('-------------------------------------------------------')
        print(name)
        print('-------------------------------------------------------')
        print('Split commands', len(split), '\nFiltered commands', len(filtered))
