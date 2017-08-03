# Clear and re-populate the cuny_departments table.

from datetime import date
import re
import os

import sqlite3
import csv

# Find the most recent list of CUNY Academic Organizations
all_files = [x for x in os.listdir('.') if x.endswith('.csv')]
# Find most recent catalog, requisite, and attribute files; be sure they all
# have the same date.
latest_org = '0000-00-00'
org_file = None
for file in all_files:
  mdate = date.fromtimestamp(os.lstat(file).st_mtime).strftime('%Y-%m-%d')
  if re.search('academic_organizations_np', file, re.I) and mdate > latest_org:
    latest_org = mdate
    org_file = file

db = sqlite3.connect('cuny_catalog.db')
cur = db.cursor()
cur.execute('delete from cuny_departments')
with open(org_file) as csvfile:
  csv_reader = csv.reader(csvfile)
  cols = None
  for row in csv_reader:
    if cols == None:
      row[0] = row[0].replace('\ufeff', '')
      if row[0].lower() == 'acad org':
        cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
    else:
      q = """insert into cuny_departments values('{}', '{}')""".format(
          row[cols.index('acad_org')],
          row[cols.index('formaldesc')].replace('\'', '’'))
      cur.execute(q)
  db.commit()
  db.close()
