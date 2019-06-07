import csv
import psycopg2
from psycopg2.extras import NamedTupleCursor

from collections import namedtuple

db = psycopg2.connect('dbname=cuny_courses')
cursor = db.cursor(cursor_factory=NamedTupleCursor)

cursor.execute("""
               drop table if exists academic_plans;
               create table academic_plans (
               program_id integer,
               institution text references institutions,
               academic_plan text,
               description text,
               primary key (program_id, institution, academic_plan))
               """)
with open('latest_queries/QCCV_ACADEMIC_PLAN_TBL.csv') as csvfile:
  reader = csv.reader(csvfile)
  cols = None
  for line in reader:
    if cols is None:
      if 'Institution' == line[0]:
        cols = [val.lower().replace(' ', '_').replace('/', '_') for val in line]
        Row = namedtuple('Row', cols)
    else:
      row = Row._make(line)
      if row.nys_program_code != '' and row.nys_program_code != '0':
        cursor.execute("""
                       insert into academic_plans values (%s, %s, %s, %s)
                       """, (row.nys_program_code,
                             row.institution,
                             row.academic_plan,
                             row.description))

db.commit()
db.close()
