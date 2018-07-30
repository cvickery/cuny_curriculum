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
discp_file = './latest_queries/QNS_CV_CUNY_DISCIPLINES.csv'
extern_file = './latest_queries/QNS_CV_CUNY_SUBJECTS.csv'
discp_date = date.fromtimestamp(os.lstat(discp_file).st_birthtime).strftime('%Y-%m-%d')
extern_date = date.fromtimestamp(os.lstat(extern_file).st_birthtime).strftime('%Y-%m-%d')

cursor.execute("""
               update updates
               set update_date = '{}', file_name = '{}'
               where table_name = 'disciplines'""".format(discp_date, discp_file))
cursor.execute("""
               update updates
               set update_date = '{}', file_name = '{}'
               where table_name = 'subjects'""".format(extern_date, extern_file))

if args.debug: print('cuny_subjects.py:\n  disciplines: {}\n  cuny_subjects: {}'.format(discp_file,
                                                                                      extern_file))
# Get list of known departments
cursor.execute("""
                select department
                from cuny_departments
                group by department
               """)
departments = [department[0] for department in cursor.fetchall()]

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
      department text references cuny_departments,
      discipline text,
      description text,
      status text,
      cuny_subject text default 'missing' references cuny_subjects,
      primary key (institution, discipline))
    """)

# Populate cuny_subjects
cursor.execute("insert into cuny_subjects values('missing', 'MISSING')")
with open(extern_file) as csvfile:
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
with open(discp_file) as csvfile:
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
      if record.acad_org in departments:
        if record.institution != 'UAPC1':
          external_subject_area = record.external_subject_area
          if external_subject_area == '':
            external_subject_area = 'missing'
          cursor.execute("""insert into disciplines values (%s, %s, %s, %s, %s, %s)
                    on conflict do nothing
              """, (record.institution,
                    record.acad_org,
                    record.subject,
                    record.formal_description.replace('\'', '’'),
                    record.status,
                    external_subject_area))
          if args.debug: print(cursor.query)
  db.commit()

db.close()
