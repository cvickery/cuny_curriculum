# Clear and re-populate the cuny_subjects table.

import psycopg2
import csv

db = psycopg2.connect('dbname=cuny_courses')
cur = db.cursor()
cur.execute('drop table if exists cuny_subjects cascade')
cur.execute("""
  create table cuny_subjects (
  area text primary key,
  description text
  )
  """)
with open('./queries/QNS_QCCV_EXTERNAL_SUBJECT_TBL.csv') as csvfile:
  csv_reader = csv.reader(csvfile)
  cols = None
  for row in csv_reader:
    if cols == None:
      row[0] = row[0].replace('\ufeff', '')
      cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
    else:
      q = """insert into cuny_subjects values('{}', '{}')""".format(
          row[cols.index('external_subject_area')],
          row[cols.index('description')].replace("'", "’P"))
      cur.execute(q)
  db.commit()
  db.close()
