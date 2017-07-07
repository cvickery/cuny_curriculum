# Clear and re-populate the (requirement) designations table.

import sqlite3
import csv

db = sqlite3.connect('courses.db')
cur = db.cursor()
cur.execute('delete from designations')
with open('QCCV_RQMNT_DESIG_TBL.csv') as csvfile:
  csv_reader = csv.reader(csvfile)
  cols = None
  for row in csv_reader:
    if cols == None:
      row[0] = row[0].replace('\ufeff', '')
      cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
    else:
      q = """insert into designations values('{}', '{}')""".format(
          row[cols.index('designation')],
          row[cols.index('formal_description')].replace('l&Q', 'l & Q').replace('eR', 'e R'))
      cur.execute(q)
  db.commit()
  db.close()
