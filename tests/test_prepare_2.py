import os
from mysql.toolkit.script.prepare import prepare_sql


with open(os.path.join(os.path.dirname(__file__), 'data', 'oneline.sql'), 'r') as sf:
    stmnt = sf.read()

print(stmnt)
print('------------------------------------------------------------------------')
s = prepare_sql(stmnt)
print('------------------------------------------------------------------------')
print(s)
