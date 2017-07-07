# Clear and re-populate the careers table.

import sqlite3
import csv

db = sqlite3.connect('courses.db')
cur = db.cursor()
cur.execute('delete from careers')
with open('ACAD_CAREER_TBL.csv') as csvfile:
  csv_reader = csv.reader(csvfile)
  cols = None
  for row in csv_reader:
    if cols == None:
      row[0] = row[0].replace('\ufeff', '')
      cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
    else:
      is_graduate = 0
      if row[cols.index('graduate')] == 'Y': is_graduate = 1
      q = """insert into careers values('{}', '{}', '{}', {})""".format(
          row[cols.index('institution')],
          row[cols.index('career')],
          row[cols.index('descr')],
          is_graduate)
      cur.execute(q)
  db.commit()
  db.close()
