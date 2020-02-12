#! /usr/local/bin/python3

import csv
import psycopg2
from psycopg2.extras import NamedTupleCursor

from collections import namedtuple

db = psycopg2.connect('dbname=cuny_curriculum')
cursor = db.cursor(cursor_factory=NamedTupleCursor)

cursor.execute("""
               drop table if exists cuny_programs;
               create table cuny_programs (
               id serial primary key,
               nys_program_code integer,
               institution text references cuny_institutions,
               department text,
               percent_owned float,
               academic_plan text,
               description text,
               cip_code text,
               hegis_code text,
               program_status text)
               """)

with open('latest_queries/QCCV_PROG_PLAN_ORG.csv') as csvfile:
  reader = csv.reader(csvfile)
  cols = None
  for line in reader:
    if cols is None:
      if 'Institution' == line[0]:
        cols = [val.lower().replace(' ', '_')
                           .replace('/', '_')
                           .replace('-', '_')
                           .replace('?', '') for val in line]
        Row = namedtuple('Row', cols)
    else:
      row = Row._make(line)
      if row.nys_program_code != '' and row.nys_program_code != '0':
        cursor.execute("""
                       insert into cuny_programs values (
                                                 default, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       """, (row.nys_program_code,
                             row.institution,
                             row.academic_organization,
                             row.percent_owned,
                             row.academic_plan,
                             row.transcript_description,
                             row.cip_code,
                             row.hegis_code,
                             row.status))

with open('latest_queries/ACAD_SUBPLN_TBL.csv') as csvfile:
  reader = csv.reader(csvfile)
  cols = None
  for line in reader:
    if cols is None:
      if 'Institution' == line[0]:
        cols = [val.lower().replace(' ', '_')
                           .replace('/', '_')
                           .replace('-', '') for val in line]
        schema = ', '.join([f'{col} text' for col in cols])
        schema = schema.replace('institution text', 'institution text references cuny_institutions')
        cursor.execute(f"""
                        drop table if exists cuny_subplans;
                        create table cuny_subplans (
                        {schema},
                        primary key (institution, plan, subplan))
                        """)
        Row = namedtuple('Row', cols)
    else:
      row = Row._make(line)
      values = ', '.join([f"""'{val.replace("'", '’')}'""" for val in row])
      cursor.execute(f"""
                      insert into cuny_subplans values ({values})
                     """)

db.commit()
db.close()
