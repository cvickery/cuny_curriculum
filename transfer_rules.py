# Clear and re-populate the transfer_rules table.

import sqlite3
import csv
import os

# Get most recent transfer_rules file
all_files = [x for x in os.listdir('.') if x.startswith('QNS_CV_TRNS_INTERNAL_RULS_SHRT')]
the_file = sorted(all_files, reverse=True)[0]
print(the_file)
db = sqlite3.connect('cuny_catalog.db')
cur = db.cursor()
cur.execute('delete from transfer_rules')
with open(the_file) as csvfile:
  csv_reader = csv.reader(csvfile)
  cols = None
  for row in csv_reader:
    if cols == None:
      row[0] = row[0].replace('\ufeff', '')
      cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
    else:
      q = """insert or ignore into transfer_rules values({}, {})""".format(
          row[cols.index('source_course_id')],
          row[cols.index('destination_course_id')])
      cur.execute(q)
  db.commit()
  db.close()
