#! /usr/local/bin/python3
# Clear and re-populate the table of internal subjects at all cuny colleges (cuny_disciplines).
# Clear and re-populate the table of external subject areas (cuny_subjects).

import os
import re
import sys
import csv

from datetime import date
from collections import namedtuple

import psycopg2
from psycopg2.extras import NamedTupleCursor

from cuny_divisions import ignore_institutions

import argparse

parser = argparse.ArgumentParser('Create internal and external subject tables')
parser.add_argument('--debug', '-d', action='store_true')
args = parser.parse_args()

db = psycopg2.connect('dbname=cuny_courses')
cursor = db.cursor(cursor_factory=NamedTupleCursor)

# Internal subject (disciplines) and external subject area (cuny_subjects) queries
discp_file = './latest_queries/QNS_CV_CUNY_SUBJECT_TABLE.csv'
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

if args.debug:
  print(f'cuny_subjects.py:\n  cuny_disciplines: {discp_file}\n  cuny_subjects: {extern_file}')

# Get list of known departments
cursor.execute("""
                select department
                from cuny_departments
               """)
departments = [d.department for d in cursor.fetchall()]

# CUNY Subjects table
cursor.execute('drop table if exists cuny_subjects cascade')
cursor.execute("""
  create table cuny_subjects (
  subject text primary key,
  subject_name text
  )
  """)

# Populate cuny_subjects
cursor.execute("insert into cuny_subjects values('missing', 'MISSING')")
with open(extern_file) as csvfile:
  csv_reader = csv.reader(csvfile)
  cols = None
  for line in csv_reader:
    if cols is None:
      line[0] = line[0].replace('\ufeff', '')
      cols = [val.lower().replace(' ', '_').replace('/', '_') for val in line]
      Row = namedtuple('Row', cols)
    else:
      row = Row._make(line)
      q = """insert into cuny_subjects values('{}', '{}')""".format(
          row.external_subject_area,
          row.description.replace("'", "’"))
      cursor.execute(q)
  db.commit()

# Disciplines table
cursor.execute('drop table if exists cuny_disciplines cascade')
cursor.execute(
    """
    create table cuny_disciplines (
      institution text references institutions,
      department text references cuny_departments,
      discipline text,
      discipline_name text,
      status text,
      cuny_subject text default 'missing' references cuny_subjects,
      primary key (institution, discipline))
    """)

# Populate disciplines

#
# TEMPORARY: Add missing disciplines for courses currently don't have one
#
Discipline = namedtuple('Discipline',
                        'institution department discipline discipline_name status cuny_subject')
missing_disciplines = [Discipline._make(x) for x in [
    ('BAR01', 'BAR01', 'LTS', 'Temporary Discipline', 'A', 'ELEC'),
    ('SPS01', 'SPS01', 'HESA', 'Temporary Discipline', 'A', 'ELEC'),
    ('QCC01', 'QCC01', 'ELEC', 'Temporary Discipline', 'A', 'ELEC')]]
for discp in missing_disciplines:
  cursor.execute(f"""
                  insert into cuny_disciplines values (
                  '{discp.institution}',
                  '{discp.department}',
                  '{discp.discipline}',
                  '{discp.discipline_name}',
                  '{discp.status}',
                  '{discp.cuny_subject}'
                  )
                 """)
Discipline_Key = namedtuple('Discipline_Key', 'institution discipline')
discipline_keys = set()
with open(discp_file) as csvfile:
  csv_reader = csv.reader(csvfile)
  cols = None
  for line in csv_reader:
    if cols is None:
      line[0] = line[0].replace('\ufeff', '')
      if line[0] == 'Institution':
        cols = [val.lower().replace(' ', '_').replace('/', '_') for val in line]
        Row = namedtuple('Row', cols)
    else:
      row = Row._make(line)
      if row.acad_org in departments:
        if row.institution not in ignore_institutions:
          external_subject_area = row.external_subject_area
          if external_subject_area == '':
            external_subject_area = 'missing'
          discipline_key = Discipline_Key._make([row.institution, row.subject])
          if discipline_key in discipline_keys:
            continue
          discipline_keys.add(discipline_key)
          cursor.execute("""insert into cuny_disciplines values (%s, %s, %s, %s, %s, %s)
                         """, (row.institution,
                               row.acad_org,
                               row.subject,
                               row.formal_description.replace('\'', '’'),
                               row.status,
                               external_subject_area))
db.commit()
db.close()
