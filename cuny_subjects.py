# Clear and re-populate the table of internal subjects at all cuny colleges (disciplines).
# Clear and re-populate the table of external subject areas (cuny_subjects).

import os
import sys
import re
import psycopg2
import csv
import argparse
from datetime import date
from collections import namedtuple

parser = argparse.ArgumentParser('Create internal and external subject tables')
parser.add_argument('--debug', '-d', action='store_true')
args = parser.parse_args()

db = psycopg2.connect('dbname=cuny_courses')
cursor = db.cursor()

# Internal Subjects (disciplines)
# Be sure there are subject_tbl (disciplines) and external_subject (cuny_subjects) query files,
# and get the latest ones.
all_files = [x for x in os.listdir('./queries/') if x.endswith('.csv')]
latest_discp = '0000-00-00'
discp_file = None
latest_extern = '0000-00-00'
extern_file = None
for file in all_files:
  mdate = date.fromtimestamp(os.lstat('./queries/' + file).st_mtime).strftime('%Y-%m-%d')
  if re.search('cuny_disciplines', file, re.I) and mdate > latest_discp:
    latest_discp = mdate
    discp_file = file
  if re.search('cuny_subjects', file, re.I) and mdate > latest_extern:
    latest_extern = mdate
    extern_file = file

if discp_file == None:
  print('No CUNY disciplines query found', file = sys.stderr)
  exit(1)
if extern_file == None:
  print('No CUNY subjects query found', file = sys.stderr)
  exit(1)
cursor.execute("""
               update updates
               set update_date = '{}', file_name = '{}'
               where table_name = 'disciplines'""".format(latest_discp, discp_file))
cursor.execute("""
               update updates
               set update_date = '{}', file_name = '{}'
               where table_name = 'subjects'""".format(latest_extern, extern_file))

if args.debug: print('cuny_subjects.py:\n  disciplines: {}\n  cuny_subjects: {}'.format(discp_file,
                                                                                      extern_file))

# CUNY Subjects table
cursor.execute('drop table if exists cuny_subjects cascade')
cursor.execute("""
  create table cuny_subjects (
  subject text primary key,
  description text
  )
  """)

# Disciplines table
cursor.execute('drop table if exists disciplines cascade')
cursor.execute(
    """
    create table disciplines (
      institution text references institutions,
      discipline text,
      description text,
      cuny_subject text default 'missing' references cuny_subjects,
      primary key (institution, discipline))
    """)

# Populate cuny_subjects
# cursor.execute("insert into cuny_subjects values('missing', 'MISSING')")
with open('./queries/' + extern_file) as csvfile:
  csv_reader = csv.reader(csvfile)
  cols = None
  for row in csv_reader:
    if cols == None:
      row[0] = row[0].replace('\ufeff', '')
      cols = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
      Record = namedtuple('Record', cols)
    else:
      record = Record._make(row)
      q = """insert into cuny_subjects values('{}', '{}')""".format(
          record.external_subject_area,
          record.description.replace("'", "’"))
      cursor.execute(q)
  db.commit()

# Populate disciplines
with open('./queries/' + discp_file) as csvfile:
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
        external_subject_area = record.external_subject_area
        if external_subject_area == '':
          external_subject_area = 'missing'
        q = """insert into disciplines values ('{}', '{}', '{}', '{}') on conflict do nothing
            """.format(
            record.institution,
            record.subject,
            record.formal_description.replace('\'', '’'),
            external_subject_area)
        if args.debug: print(q)
        cursor.execute(q)
  db.commit()

db.close()
