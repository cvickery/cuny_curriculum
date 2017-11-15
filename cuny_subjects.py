# Clear and re-populate the (course) attributes table.

import os
import re
import psycopg2
import csv
import argparse
from datetime import date
from collections import namedtuple

parser = argparse.ArgumentParser('Create subjects table from CUNY’s')
parser.add_argument('--debug', '-d', action='store_true')
args = parser.parse_args()

# Be sure there is a subject query file, and get the latest one.
all_files = [x for x in os.listdir('./queries/') if x.endswith('.csv')]
latest_subj = '0000-00-00'
subj_file = None
for file in all_files:
  mdate = date.fromtimestamp(os.lstat('./queries/' + file).st_mtime).strftime('%Y-%m-%d')
  if re.search('_subject_', file, re.I) and mdate > latest_subj:
    latest_subj = mdate
    subj_file = file

if subj_file == None:
  print('No subject table query found')
if args.debug: print('cuny_subjects.py using', subj_file)
db = psycopg2.connect('dbname=cuny_courses')
cur = db.cursor()
cur.execute('drop table if exists subjects')
cur.execute(
    """
    create table subjects (
      institution text references institutions,
      subject text,
      description text,
      primary key (institution, subject))
    """)

with open('./queries/{}'.format(subj_file)) as csvfile:
  csv_reader = csv.reader(csvfile)
  cols = None
  for row in csv_reader:
    if cols == None:
      row[0] = row[0].replace('\ufeff', '')
      if row[0] == 'Institution':
        cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
        Record = namedtuple('Record', cols)
    else:
      record = Record._make(row)
      if record.institution != 'UAPC1':
        q = """insert into subjects values ('{}', '{}', '{}') on conflict do nothing""".format(
            record.institution,
            record.subject,
            record.formal_description.replace('\'', '’'))
        if args.debug: print(q)
        cur.execute(q)
  db.commit()
  db.close()
