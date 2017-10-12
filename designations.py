# Clear and re-populate the (requirement) designations table.

import psycopg2
import csv

db = psycopg2.connect('dbname=cuny_courses')
cur = db.cursor()
cur.execute('drop table if exists designations cascade')
cur.execute("""
    create table designations (
    designation text primary key,
    description text)
    """)
with open('./queries/QCCV_RQMNT_DESIG_TBL.csv') as csvfile:
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
  cur.execute("insert into designations values ('', 'No Designation')")
  db.commit()
  db.close()
