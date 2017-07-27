# Clear and re-populate the (course) attributes table.

import sqlite3
import csv

db = sqlite3.connect('cuny_catalog.db')
cur = db.cursor()
cur.execute('delete from attributes')
with open('SR742A___CRSE_ATTRIBUTE_VALUE.csv') as csvfile:
  csv_reader = csv.reader(csvfile)
  cols = None
  for row in csv_reader:
    if cols == None:
      row[0] = row[0].replace('\ufeff', '')
      cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
    else:
      q = """insert into attributes values('{}', '{}', '{}')""".format(
          row[cols.index('crse_attr')],
          row[cols.index('crsatr_val')],
          row[cols.index('formal_description')].replace('\'', '’'))
      print(q)
      cur.execute(q)
  db.commit()
  db.close()
