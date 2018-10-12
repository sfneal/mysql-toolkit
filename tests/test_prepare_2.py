import os
from mysql.toolkit.script.prepare import prepare_sql
from differentiate import diff


with open(os.path.join(os.path.dirname(__file__), 'data', 'oneline.sql'), 'r') as sf:
    stmnt = sf.read()

print(stmnt)
s = prepare_sql(stmnt)
print('------------------------------------------------------------------------')
print(s)

print(diff(stmnt, s))