#! /usr/local/bin/python3

import csv
from pgconnection import PgConnection

from collections import namedtuple

conn = PgConnection()
cursor = conn.cursor()

cursor.execute("""
               drop table if exists cuny_programs;
               create table cuny_programs (
               id serial primary key,
               nys_program_code integer,
               institution text references cuny_institutions,
               department text,
               percent_owned float,
               academic_plan text,
               plan_type text,
               description text,
               cip_code text,
               hegis_code text,
               program_status text,
               career text,
               effective_date date,
               first_term_valid text,
               last_admit text)
               """)

with open('./latest_queries/QCCV_PROG_PLAN_ORG.csv') as csvfile:
  reader = csv.reader(csvfile)
  cols = None
  for line in reader:
    if cols is None:
      line[0] = line[0].replace('\ufeff', '')
      if 'Institution' == line[0]:
        cols = [val.lower().replace(' ', '_')
                           .replace('/', '_')
                           .replace('-', '_')
                           .replace('?', '') for val in line]
        Row = namedtuple('Row', cols)
    else:
      row = Row._make(line)
      if row.institution in ['MHC01', 'UAPC1']:
        continue
      nys_program_code = '0' if row.nys_program_code == '' else row.nys_program_code
      cursor.execute("""
                     insert into cuny_programs values (default, %s, %s, %s, %s, %s, %s, %s,
                                                                %s, %s, %s, %s, %s, %s, %s)
                     """, (nys_program_code,
                           row.institution,
                           row.academic_organization,
                           row.percent_owned,
                           row.academic_plan,
                           row.plan_type,
                           row.transcript_description,
                           row.cip_code,
                           row.hegis_code,
                           row.status,
                           row.career,
                           row.effective_date,
                           row.first_term_valid,
                           row.last_admit))

with open('./latest_queries/ACAD_SUBPLAN_TBL.csv') as csvfile:
  reader = csv.reader(csvfile)
  cols = None
  for line in reader:
    line[0] = line[0].replace('\ufeff', '')
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

conn.commit()
conn.close()
