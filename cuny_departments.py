# Clear and re-populate the cuny_departments table.

from datetime import date
import re
import os

import psycopg2
import csv

# Get the most recent list of CUNY Academic Organizations
org_file = './latest_queries/QNS_CV_ACADEMIC_ORGANIZATIONS.csv'

db = psycopg2.connect('dbname=cuny_courses')
cur = db.cursor()
cur.execute('drop table if exists cuny_departments cascade')
cur.execute("""
  create table cuny_departments (
  department text primary key,
  institution text references institutions,
  description text)
  """)
with open(org_file) as csvfile:
  csv_reader = csv.reader(csvfile)
  cols = None
  for row in csv_reader:
    if cols == None:
      row[0] = row[0].replace('\ufeff', '')
      if row[0].lower() == 'acad org':
        cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
    else:
      institution = row[cols.index('institution')]
      if institution == 'CUNY' or institution == 'UAPC1': continue
      q = """insert into cuny_departments values('{}', '{}')""".format(
          row[cols.index('acad_org')],
          row[cols.index('institution')],
          row[cols.index('formaldesc')].replace('\'', '’'))
      cur.execute(q)
  db.commit()
  db.close()
