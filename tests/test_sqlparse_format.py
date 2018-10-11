import os
import sqlparse
from mysql.toolkit.script.prepare import prepare_sql
from looptools import Timer


with open(os.path.join(os.path.dirname(__file__), 'data', 'oneline.sql'), 'r') as sf:
    stmnt = sf.read()


print(stmnt)
print('------------------------------------------------------------------------')
s = sqlparse.format(stmnt, reindent=True)
print('------------------------------------------------------------------------')
print(s)
