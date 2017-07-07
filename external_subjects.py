# Clear and re-populate the external_subjects table.

import sqlite3
import csv

db = sqlite3.connect('courses.db')
cur = db.cursor()
cur.execute('delete from external_subjects')
with open('QNS_QCCV_EXTERNAL_SUBJECT_TBL.csv') as csvfile:
  csv_reader = csv.reader(csvfile)
  cols = None
  for row in csv_reader:
    if cols == None:
      row[0] = row[0].replace('\ufeff', '')
      cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
    else:
      q = """insert into external_subjects values('{}', '{}')""".format(
          row[cols.index('external_subject_area')],
          row[cols.index('description')].replace("'", "’P"))
      cur.execute(q)
  db.commit()
  db.close()
